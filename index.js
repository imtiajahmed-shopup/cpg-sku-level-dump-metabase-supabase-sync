const fs = require("fs");
const os = require("os");
const path = require("path");
const { pipeline } = require("stream/promises");
const { Readable } = require("stream");
const { Client } = require("pg");
const { from: copyFrom } = require("pg-copy-streams");


// ============================================================
// CONFIG
// ============================================================

const METABASE_URL = process.env.METABASE_URL?.replace(/\/+$/, "");
const METABASE_API_KEY = process.env.METABASE_API_KEY;
const METABASE_CARD_ID = process.env.METABASE_CARD_ID;
const SUPABASE_DB_URL = process.env.SUPABASE_DB_URL;

// First version = full refresh.
// Later we will change this to incremental refresh.
const FULL_REFRESH =
  String(process.env.FULL_REFRESH || "true").toLowerCase() === "true";


function validateEnvironment() {
  const required = {
    METABASE_URL,
    METABASE_API_KEY,
    METABASE_CARD_ID,
    SUPABASE_DB_URL
  };

  const missing = Object.entries(required)
    .filter(([, value]) => !value)
    .map(([key]) => key);

  if (missing.length > 0) {
    throw new Error(
      `Missing environment variables: ${missing.join(", ")}`
    );
  }
}


// ============================================================
// STEP 1
// DOWNLOAD CSV FROM METABASE
// ============================================================

async function downloadMetabaseCSV(outputFile) {
  const url =
    `${METABASE_URL}/api/card/${METABASE_CARD_ID}/query/csv`;

  console.log(`Fetching Metabase card: ${METABASE_CARD_ID}`);
  console.log(`URL: ${url}`);

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "X-API-Key": METABASE_API_KEY,
      "Content-Type": "application/json"
    },

    // No dashboard/card filters are being supplied yet.
    // format_rows=false helps keep raw values unformatted.
    body: JSON.stringify({
      format_rows: false
    })
  });

  if (!response.ok) {
    const errorText = await response.text();

    throw new Error(
      `Metabase request failed: ${response.status} ${response.statusText}\n${errorText}`
    );
  }

  if (!response.body) {
    throw new Error("Metabase returned an empty response body.");
  }

  const output = fs.createWriteStream(outputFile);

  await pipeline(
    Readable.fromWeb(response.body),
    output
  );

  const stats = fs.statSync(outputFile);

  console.log(
    `Metabase CSV downloaded successfully: ${stats.size.toLocaleString()} bytes`
  );
}


// ============================================================
// STEP 2
// CONNECT TO SUPABASE POSTGRES
// ============================================================

function createSupabaseClient() {
  return new Client({
    connectionString: SUPABASE_DB_URL,

    // Supabase requires encrypted database connections.
    ssl: {
      rejectUnauthorized: false
    }
  });
}


// ============================================================
// STEP 3
// LOAD RAW CSV INTO STAGING TABLE
// ============================================================

async function loadCSVToStaging(client, csvFile) {
  console.log("Clearing staging table...");

  await client.query(`
    TRUNCATE TABLE public.stg_cpg_sku_order_line;
  `);

  console.log("Loading CSV into staging table...");

  const copySQL = `
    COPY public.stg_cpg_sku_order_line (
      dms_order_id,
      oms_order_id,
      created_at,
      updated_at,
      retailer_id,
      sku,
      sku_id,
      product_name,
      product_id,
      category_id,
      category,
      order_type,
      status,
      order_qty,
      lp,
      sp,
      order_value,
      delivered_qty,
      delivered_value,
      return_qty,
      return_value,
      exchange_qty,
      exchange_value,
      damage_qty,
      damage_value,
      free_claimable_qty,
      free_claimable_value,
      free_non_claimable_qty,
      free_non_claimable_value,
      claimable_discount,
      non_claimable_discount,
      sku_weight_in_gm,
      anchor_receivable,
      group_order_id,
      dsr_id,
      dsr_name,
      dsr_phone,
      employee_id,
      db_id,
      delivered_date,
      pick_list_ids,
      sub_anchor_type,
      sub_bu,
      non_claimable_discount_total,
      trade_discount,
      claimable_discount_total,
      nmv,
      week_number
    )
    FROM STDIN
    WITH (
      FORMAT CSV,
      HEADER TRUE,
      QUOTE '"',
      ESCAPE '"'
    )
  `;

  const copyStream = client.query(copyFrom(copySQL));

  await pipeline(
    fs.createReadStream(csvFile),
    copyStream
  );

  console.log("CSV successfully loaded into staging table.");
}


// ============================================================
// STEP 4
// TRANSFORM + LOAD FINAL TABLES
// ============================================================

async function transformAndLoad(client) {

  // Make PostgreSQL interpret Metabase timestamps in Bangladesh time.
  await client.query(`
    SET TIME ZONE 'Asia/Dhaka';
  `);


  // ----------------------------------------------------------
  // START TRANSACTION
  // ----------------------------------------------------------

  await client.query("BEGIN");

  try {

    // --------------------------------------------------------
    // FIRST VERSION:
    // FULL REFRESH OF FACT TABLE
    //
    // Later we will replace this with incremental loading.
    // --------------------------------------------------------

    if (FULL_REFRESH) {

      console.log("FULL_REFRESH=true");
      console.log("Clearing fact table...");

      await client.query(`
        TRUNCATE TABLE public.fact_cpg_sku_order_line;
      `);
    }


    // --------------------------------------------------------
    // DB DIMENSION
    //
    // The SKU query does not contain DB name.
    // Therefore we only create/update the DB IDs here.
    // DB name can later be populated from your master file.
    // --------------------------------------------------------

    console.log("Updating dim_db...");

    await client.query(`
      INSERT INTO public.dim_db (db_id)
      SELECT DISTINCT
        NULLIF(TRIM(db_id), '')::bigint
      FROM public.stg_cpg_sku_order_line
      WHERE
        NULLIF(TRIM(db_id), '') IS NOT NULL
        AND TRIM(db_id) ~ '^[0-9]+$'
      ON CONFLICT (db_id) DO NOTHING;
    `);


    // --------------------------------------------------------
    // PRODUCT DIMENSION
    // --------------------------------------------------------

    console.log("Updating dim_product...");

    await client.query(`
      INSERT INTO public.dim_product (
        product_id,
        sku_id,
        sku,
        product_name,
        category_id,
        category,
        sku_weight_in_gm,
        updated_at
      )
      SELECT DISTINCT ON (
        NULLIF(TRIM(product_id), '')::bigint
      )

        NULLIF(TRIM(product_id), '')::bigint,

        CASE
          WHEN NULLIF(TRIM(sku_id), '') ~ '^[0-9]+$'
          THEN TRIM(sku_id)::bigint
          ELSE NULL
        END,

        NULLIF(TRIM(sku), ''),

        NULLIF(TRIM(product_name), ''),

        CASE
          WHEN NULLIF(TRIM(category_id), '') ~ '^[0-9]+$'
          THEN TRIM(category_id)::bigint
          ELSE NULL
        END,

        NULLIF(TRIM(category), ''),

        CASE
          WHEN NULLIF(TRIM(sku_weight_in_gm), '') <> ''
          THEN REPLACE(TRIM(sku_weight_in_gm), ',', '')::numeric
          ELSE NULL
        END,

        now()

      FROM public.stg_cpg_sku_order_line

      WHERE
        NULLIF(TRIM(product_id), '') IS NOT NULL
        AND TRIM(product_id) ~ '^[0-9]+$'

      ORDER BY
        NULLIF(TRIM(product_id), '')::bigint,
        loaded_at DESC

      ON CONFLICT (product_id)
      DO UPDATE SET

        sku_id = EXCLUDED.sku_id,
        sku = EXCLUDED.sku,
        product_name = EXCLUDED.product_name,
        category_id = EXCLUDED.category_id,
        category = EXCLUDED.category,
        sku_weight_in_gm = EXCLUDED.sku_weight_in_gm,
        updated_at = now();
    `);


    // --------------------------------------------------------
    // DSR DIMENSION
    // --------------------------------------------------------

    console.log("Updating dim_dsr...");

    await client.query(`
      INSERT INTO public.dim_dsr (
        dsr_id,
        dsr_name,
        dsr_phone,
        employee_id,
        updated_at
      )
      SELECT DISTINCT ON (
        NULLIF(TRIM(dsr_id), '')::bigint
      )

        NULLIF(TRIM(dsr_id), '')::bigint,

        NULLIF(TRIM(dsr_name), ''),

        NULLIF(TRIM(dsr_phone), ''),

        NULLIF(TRIM(employee_id), ''),

        now()

      FROM public.stg_cpg_sku_order_line

      WHERE
        NULLIF(TRIM(dsr_id), '') IS NOT NULL
        AND TRIM(dsr_id) ~ '^[0-9]+$'

      ORDER BY
        NULLIF(TRIM(dsr_id), '')::bigint,
        loaded_at DESC

      ON CONFLICT (dsr_id)
      DO UPDATE SET

        dsr_name = EXCLUDED.dsr_name,
        dsr_phone = EXCLUDED.dsr_phone,
        employee_id = EXCLUDED.employee_id,
        updated_at = now();
    `);


    // --------------------------------------------------------
    // FACT TABLE
    // --------------------------------------------------------

    console.log("Loading fact_cpg_sku_order_line...");

    await client.query(`
      INSERT INTO public.fact_cpg_sku_order_line (

        dms_order_id,
        oms_order_id,
        group_order_id,
        retailer_id,

        db_id,
        product_id,
        dsr_id,
        employee_id,

        created_at,
        updated_at,
        delivered_at,

        delivered_date,

        order_type,
        status,

        sub_anchor_type,
        sub_bu,

        order_qty,
        delivered_qty,
        return_qty,
        exchange_qty,
        damage_qty,

        free_claimable_qty,
        free_non_claimable_qty,

        lp,
        sp,

        order_value,
        delivered_value,
        return_value,
        exchange_value,
        damage_value,

        free_claimable_value,
        free_non_claimable_value,

        claimable_discount,
        non_claimable_discount,
        non_claimable_discount_total,
        trade_discount,
        claimable_discount_total,

        anchor_receivable,

        nmv,

        pick_list_ids,
        sku_weight_in_gm,
        week_number,

        loaded_at
      )

      SELECT

        -- ----------------------------------------------------
        -- SOURCE IDENTIFIERS
        -- ----------------------------------------------------

        CASE
          WHEN NULLIF(TRIM(s.dms_order_id), '') ~ '^[0-9]+$'
          THEN REPLACE(TRIM(s.dms_order_id), ',', '')::bigint
          ELSE NULL
        END,

        NULLIF(TRIM(s.oms_order_id), ''),

        CASE
          WHEN NULLIF(TRIM(s.group_order_id), '') ~ '^[0-9]+$'
          THEN REPLACE(TRIM(s.group_order_id), ',', '')::bigint
          ELSE NULL
        END,

        CASE
          WHEN NULLIF(TRIM(s.retailer_id), '') ~ '^[0-9]+$'
          THEN REPLACE(TRIM(s.retailer_id), ',', '')::bigint
          ELSE NULL
        END,


        -- ----------------------------------------------------
        -- DIMENSION KEYS
        -- ----------------------------------------------------

        TRIM(s.db_id)::bigint,

        CASE
          WHEN NULLIF(TRIM(s.product_id), '') ~ '^[0-9]+$'
          THEN TRIM(s.product_id)::bigint
          ELSE NULL
        END,

        CASE
          WHEN NULLIF(TRIM(s.dsr_id), '') ~ '^[0-9]+$'
          THEN TRIM(s.dsr_id)::bigint
          ELSE NULL
        END,

        NULLIF(TRIM(s.employee_id), ''),


        -- ----------------------------------------------------
        -- TIMESTAMPS
        -- ----------------------------------------------------

        CASE
          WHEN NULLIF(TRIM(s.created_at), '') IS NOT NULL
          THEN to_timestamp(
            TRIM(s.created_at),
            'FMMonth DD, YYYY, HH12:MI AM'
          )
          ELSE NULL
        END,

        CASE
          WHEN NULLIF(TRIM(s.updated_at), '') IS NOT NULL
          THEN to_timestamp(
            TRIM(s.updated_at),
            'FMMonth DD, YYYY, HH12:MI AM'
          )
          ELSE NULL
        END,

        CASE
          WHEN NULLIF(TRIM(s.delivered_date), '') IS NOT NULL
          THEN to_timestamp(
            TRIM(s.delivered_date),
            'FMMonth DD, YYYY, HH12:MI AM'
          )
          ELSE NULL
        END,


        -- ----------------------------------------------------
        -- BUSINESS DATE
        --
        -- MAIN DATE FILTER
        -- ----------------------------------------------------

        (
          to_timestamp(
            TRIM(s.delivered_date),
            'FMMonth DD, YYYY, HH12:MI AM'
          )
        )::date,


        -- ----------------------------------------------------
        -- ORDER ATTRIBUTES
        -- ----------------------------------------------------

        NULLIF(TRIM(s.order_type), ''),

        CASE
          WHEN NULLIF(TRIM(s.status), '') ~ '^-?[0-9]+$'
          THEN TRIM(s.status)::smallint
          ELSE NULL
        END,


        -- ----------------------------------------------------
        -- FILTERS
        -- ----------------------------------------------------

        NULLIF(TRIM(s.sub_anchor_type), ''),

        NULLIF(TRIM(s.sub_bu), ''),


        -- ----------------------------------------------------
        -- QUANTITIES
        -- ----------------------------------------------------

        CASE WHEN NULLIF(TRIM(s.order_qty), '') <> ''
          THEN REPLACE(TRIM(s.order_qty), ',', '')::numeric
          ELSE NULL END,

        CASE WHEN NULLIF(TRIM(s.delivered_qty), '') <> ''
          THEN REPLACE(TRIM(s.delivered_qty), ',', '')::numeric
          ELSE NULL END,

        CASE WHEN NULLIF(TRIM(s.return_qty), '') <> ''
          THEN REPLACE(TRIM(s.return_qty), ',', '')::numeric
          ELSE NULL END,

        CASE WHEN NULLIF(TRIM(s.exchange_qty), '') <> ''
          THEN REPLACE(TRIM(s.exchange_qty), ',', '')::numeric
          ELSE NULL END,

        CASE WHEN NULLIF(TRIM(s.damage_qty), '') <> ''
          THEN REPLACE(TRIM(s.damage_qty), ',', '')::numeric
          ELSE NULL END,


        CASE WHEN NULLIF(TRIM(s.free_claimable_qty), '') <> ''
          THEN REPLACE(TRIM(s.free_claimable_qty), ',', '')::numeric
          ELSE NULL END,

        CASE WHEN NULLIF(TRIM(s.free_non_claimable_qty), '') <> ''
          THEN REPLACE(TRIM(s.free_non_claimable_qty), ',', '')::numeric
          ELSE NULL END,


        -- ----------------------------------------------------
        -- PRICING
        -- ----------------------------------------------------

        CASE WHEN NULLIF(TRIM(s.lp), '') <> ''
          THEN REPLACE(TRIM(s.lp), ',', '')::numeric
          ELSE NULL END,

        CASE WHEN NULLIF(TRIM(s.sp), '') <> ''
          THEN REPLACE(TRIM(s.sp), ',', '')::numeric
          ELSE NULL END,


        -- ----------------------------------------------------
        -- VALUES
        -- ----------------------------------------------------

        CASE WHEN NULLIF(TRIM(s.order_value), '') <> ''
          THEN REPLACE(TRIM(s.order_value), ',', '')::numeric
          ELSE NULL END,

        CASE WHEN NULLIF(TRIM(s.delivered_value), '') <> ''
          THEN REPLACE(TRIM(s.delivered_value), ',', '')::numeric
          ELSE NULL END,

        CASE WHEN NULLIF(TRIM(s.return_value), '') <> ''
          THEN REPLACE(TRIM(s.return_value), ',', '')::numeric
          ELSE NULL END,

        CASE WHEN NULLIF(TRIM(s.exchange_value), '') <> ''
          THEN REPLACE(TRIM(s.exchange_value), ',', '')::numeric
          ELSE NULL END,

        CASE WHEN NULLIF(TRIM(s.damage_value), '') <> ''
          THEN REPLACE(TRIM(s.damage_value), ',', '')::numeric
          ELSE NULL END,


        -- ----------------------------------------------------
        -- FREE ITEM VALUES
        -- ----------------------------------------------------

        CASE WHEN NULLIF(TRIM(s.free_claimable_value), '') <> ''
          THEN REPLACE(TRIM(s.free_claimable_value), ',', '')::numeric
          ELSE NULL END,

        CASE WHEN NULLIF(TRIM(s.free_non_claimable_value), '') <> ''
          THEN REPLACE(TRIM(s.free_non_claimable_value), ',', '')::numeric
          ELSE NULL END,


        -- ----------------------------------------------------
        -- DISCOUNTS
        -- ----------------------------------------------------

        CASE WHEN NULLIF(TRIM(s.claimable_discount), '') <> ''
          THEN REPLACE(TRIM(s.claimable_discount), ',', '')::numeric
          ELSE NULL END,

        CASE WHEN NULLIF(TRIM(s.non_claimable_discount), '') <> ''
          THEN REPLACE(TRIM(s.non_claimable_discount), ',', '')::numeric
          ELSE NULL END,

        CASE WHEN NULLIF(TRIM(s.non_claimable_discount_total), '') <> ''
          THEN REPLACE(TRIM(s.non_claimable_discount_total), ',', '')::numeric
          ELSE NULL END,

        CASE WHEN NULLIF(TRIM(s.trade_discount), '') <> ''
          THEN REPLACE(TRIM(s.trade_discount), ',', '')::numeric
          ELSE NULL END,

        CASE WHEN NULLIF(TRIM(s.claimable_discount_total), '') <> ''
          THEN REPLACE(TRIM(s.claimable_discount_total), ',', '')::numeric
          ELSE NULL END,


        -- ----------------------------------------------------
        -- ANCHOR RECEIVABLE
        -- ----------------------------------------------------

        CASE WHEN NULLIF(TRIM(s.anchor_receivable), '') <> ''
          THEN REPLACE(TRIM(s.anchor_receivable), ',', '')::numeric
          ELSE NULL END,


        -- ----------------------------------------------------
        -- MAIN METRIC
        -- ----------------------------------------------------

        CASE WHEN NULLIF(TRIM(s.nmv), '') <> ''
          THEN REPLACE(TRIM(s.nmv), ',', '')::numeric
          ELSE NULL END,


        -- ----------------------------------------------------
        -- PICK LIST IDS
        -- Example source:
        -- [3066700]
        -- ----------------------------------------------------

        CASE
          WHEN TRIM(COALESCE(s.pick_list_ids, '')) = ''
            THEN NULL

          WHEN TRIM(s.pick_list_ids) = '[]'
            THEN NULL

          ELSE
            string_to_array(
              regexp_replace(
                TRIM(s.pick_list_ids),
                '[\\[\\]\\s]',
                '',
                'g'
              ),
              ','
            )::bigint[]
        END,


        -- ----------------------------------------------------
        -- OTHER
        -- ----------------------------------------------------

        CASE WHEN NULLIF(TRIM(s.sku_weight_in_gm), '') <> ''
          THEN REPLACE(TRIM(s.sku_weight_in_gm), ',', '')::numeric
          ELSE NULL END,

        CASE
          WHEN NULLIF(TRIM(s.week_number), '') ~ '^-?[0-9]+$'
          THEN TRIM(s.week_number)::smallint
          ELSE NULL
        END,

        now()

      FROM public.stg_cpg_sku_order_line s

      WHERE
        NULLIF(TRIM(s.db_id), '') IS NOT NULL
        AND TRIM(s.db_id) ~ '^[0-9]+$'

        AND NULLIF(TRIM(s.delivered_date), '') IS NOT NULL
    `);


    // --------------------------------------------------------
    // COMMIT
    // --------------------------------------------------------

    await client.query("COMMIT");

    console.log("Transformation and final load completed.");

  } catch (error) {

    await client.query("ROLLBACK");

    throw error;
  }
}


// ============================================================
// STEP 5
// VALIDATE DATA
// ============================================================

async function validateData(client) {

  console.log("");
  console.log("========== VALIDATION ==========");

  const result = await client.query(`
    SELECT
      COUNT(*) AS row_count,
      COUNT(DISTINCT db_id) AS db_count,
      COUNT(DISTINCT product_id) AS product_count,
      MIN(delivered_date) AS min_delivered_date,
      MAX(delivered_date) AS max_delivered_date,
      COALESCE(SUM(nmv), 0) AS total_nmv
    FROM public.fact_cpg_sku_order_line;
  `);

  console.table(result.rows);


  const stagingResult = await client.query(`
    SELECT COUNT(*) AS staging_row_count
    FROM public.stg_cpg_sku_order_line;
  `);

  console.table(stagingResult.rows);

  console.log("================================");
}


// ============================================================
// MAIN
// ============================================================

async function main() {

  validateEnvironment();

  const tempFile = path.join(
    os.tmpdir(),
    `cpg_sku_order_dump_${Date.now()}.csv`
  );

  const client = createSupabaseClient();

  try {

    console.log("========================================");
    console.log("CPG METABASE → SUPABASE SYNC");
    console.log("========================================");

    // Connect
    console.log("Connecting to Supabase...");
    await client.connect();

    console.log("Connected to Supabase.");

    // Download from Metabase
    await downloadMetabaseCSV(tempFile);

    // Load raw data
    await loadCSVToStaging(client, tempFile);

    // Transform + final load
    await transformAndLoad(client);

    // Validation
    await validateData(client);

    console.log("");
    console.log("SYNC COMPLETED SUCCESSFULLY.");

  } catch (error) {

    console.error("");
    console.error("SYNC FAILED");
    console.error(error);

    process.exitCode = 1;

  } finally {

    try {
      await client.end();
    } catch (error) {
      console.error("Error closing database connection:", error.message);
    }

    // Delete temporary CSV
    try {
      if (fs.existsSync(tempFile)) {
        fs.unlinkSync(tempFile);
      }
    } catch (error) {
      console.error(
        "Could not delete temporary file:",
        error.message
      );
    }
  }
}


main();
