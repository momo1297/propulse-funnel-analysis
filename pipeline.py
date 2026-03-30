"""
pipeline.py — Medallion pipeline: Bronze → Silver → Gold
DuckDB in-process engine, no external dependencies beyond duckdb + pandas.

Business context: subscription platform with 3 partners (HealthMedia, WellnessPress,
NutriDigital) acquiring users through 5 channels. We want to understand where the
funnel leaks and which acquisition channels / experiments drive the best outcomes.
"""

import duckdb
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR   = Path(__file__).parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR   = BASE_DIR / "data" / "gold"

for d in (SILVER_DIR, GOLD_DIR):
    d.mkdir(parents=True, exist_ok=True)

DB_PATH = BASE_DIR / "data" / "funnel.duckdb"

# ---------------------------------------------------------------------------
# Connection — persistent file-based DB so layers accumulate across runs
# ---------------------------------------------------------------------------
con = duckdb.connect(str(DB_PATH))


# ============================================================================
# LAYER 1 — BRONZE
# Raw ingestion: read CSVs as-is, add _ingested_at watermark.
# No transformations here; bronze is the immutable record of what arrived.
# ============================================================================

def ingest_bronze() -> None:
    print("\n[BRONZE] Ingesting raw CSV files …")

    ingested_at = datetime.utcnow().isoformat(timespec="seconds")

    # DuckDB can query CSVs directly; we materialise them so downstream layers
    # always read from DB tables (consistent schema, faster joins).
    con.execute("DROP TABLE IF EXISTS bronze_funnel_events")
    con.execute(f"""
        CREATE TABLE bronze_funnel_events AS
        SELECT
            *,
            '{ingested_at}' AS _ingested_at
        FROM read_csv_auto('{BRONZE_DIR}/funnel_events.csv', header=true)
    """)

    con.execute("DROP TABLE IF EXISTS bronze_ab_tests")
    con.execute(f"""
        CREATE TABLE bronze_ab_tests AS
        SELECT
            *,
            '{ingested_at}' AS _ingested_at
        FROM read_csv_auto('{BRONZE_DIR}/ab_tests.csv', header=true)
    """)

    n_events = con.execute("SELECT COUNT(*) FROM bronze_funnel_events").fetchone()[0]
    n_ab     = con.execute("SELECT COUNT(*) FROM bronze_ab_tests").fetchone()[0]
    print(f"  bronze_funnel_events : {n_events:,} rows")
    print(f"  bronze_ab_tests      : {n_ab:,} rows")


# ============================================================================
# LAYER 2 — SILVER
# Clean, type-cast, and semantically enrich the raw data.
# Goal: one row per visitor, every field trustworthy, business concepts surfaced.
# ============================================================================

def build_silver() -> None:
    print("\n[SILVER] Building silver_funnel …")

    con.execute("DROP TABLE IF EXISTS silver_funnel")
    con.execute("""
        CREATE TABLE silver_funnel AS
        SELECT
            -- ── Identity ──────────────────────────────────────────────────
            event_id,
            visitor_id,
            partner_id,
            channel,
            country,
            device,

            -- ── Dates: cast strings to proper DATE type ───────────────────
            -- DuckDB auto-detected them as VARCHAR; explicit cast avoids
            -- silent failures in date arithmetic downstream.
            TRY_CAST(landing_date  AS DATE) AS landing_date,
            TRY_CAST(signup_date   AS DATE) AS signup_date,
            TRY_CAST(trial_date    AS DATE) AS trial_date,
            TRY_CAST(paid_date     AS DATE) AS paid_date,

            -- ── Funnel boolean flags ──────────────────────────────────────
            -- visited is always TRUE by construction; kept for completeness.
            CAST(visited        AS BOOLEAN) AS visited,
            CAST(signed_up      AS BOOLEAN) AS signed_up,
            CAST(started_trial  AS BOOLEAN) AS started_trial,
            CAST(converted_paid AS BOOLEAN) AS converted_paid,

            -- ── Time-to-convert metrics ───────────────────────────────────
            -- Days between each step; NULL when the step was never reached.
            -- Business use: a long time_to_signup_days may indicate friction
            -- in the onboarding form; long time_to_paid_days during trial
            -- may indicate price anchoring issues.
            DATEDIFF('day', TRY_CAST(landing_date AS DATE), TRY_CAST(signup_date  AS DATE)) AS time_to_signup_days,
            DATEDIFF('day', TRY_CAST(signup_date  AS DATE), TRY_CAST(trial_date   AS DATE)) AS time_to_trial_days,
            DATEDIFF('day', TRY_CAST(trial_date   AS DATE), TRY_CAST(paid_date    AS DATE)) AS time_to_paid_days,

            -- ── Funnel stage ──────────────────────────────────────────────
            -- The deepest step reached; useful for cohort segmentation and
            -- for computing per-stage counts without multiple GROUP BYs.
            CASE
                WHEN CAST(converted_paid   AS BOOLEAN) THEN 'converted'
                WHEN CAST(started_trial    AS BOOLEAN) THEN 'trialing'
                WHEN CAST(signed_up        AS BOOLEAN) THEN 'signed_up'
                ELSE                                        'visited'
            END AS funnel_stage,

            -- ── Dropped-at stage ─────────────────────────────────────────
            -- Where did the visitor stop progressing? NULL for converted
            -- visitors (they didn't drop). This lets us directly count
            -- "exits per step" without self-joins.
            CASE
                WHEN CAST(converted_paid   AS BOOLEAN) THEN NULL
                WHEN CAST(started_trial    AS BOOLEAN) THEN 'trial_to_paid'
                WHEN CAST(signed_up        AS BOOLEAN) THEN 'signup_to_trial'
                ELSE                                        'visit_to_signup'
            END AS dropped_at,

            _ingested_at

        FROM bronze_funnel_events
    """)

    n = con.execute("SELECT COUNT(*) FROM silver_funnel").fetchone()[0]
    print(f"  silver_funnel : {n:,} rows")

    # Quick sanity: no row should have signed_up=FALSE but started_trial=TRUE
    violations = con.execute("""
        SELECT COUNT(*) FROM silver_funnel
        WHERE signed_up = FALSE AND started_trial = TRUE
    """).fetchone()[0]
    print(f"  Funnel integrity check (should be 0): {violations}")


# ============================================================================
# LAYER 3 — GOLD
# Purpose-built analytical tables. Each is pre-aggregated for BI / ad-hoc
# queries.  No row-level data; only metrics the business actually queries.
# ============================================================================

def build_gold() -> None:

    # ── 3a. Funnel overview ────────────────────────────────────────────────
    # Shows the classic "funnel waterfall" broken down by channel × partner.
    # Conversion rates are computed at each step (not just overall) to reveal
    # where a specific partner × channel combination over- or under-performs.
    print("\n[GOLD] Building gold_funnel_overview …")
    con.execute("DROP TABLE IF EXISTS gold_funnel_overview")
    con.execute("""
        CREATE TABLE gold_funnel_overview AS
        SELECT
            channel,
            partner_id,

            -- ── Volume at each stage ──────────────────────────────────────
            COUNT(*)                                       AS total_visitors,
            SUM(CAST(signed_up      AS INT))               AS total_signups,
            SUM(CAST(started_trial  AS INT))               AS total_trials,
            SUM(CAST(converted_paid AS INT))               AS total_paid,

            -- ── Step-level conversion rates ───────────────────────────────
            -- NULLIF guards against division-by-zero when a slice has 0 signups/trials.
            ROUND(100.0 * SUM(CAST(signed_up      AS INT)) / NULLIF(COUNT(*), 0),                               2) AS pct_visit_to_signup,
            ROUND(100.0 * SUM(CAST(started_trial  AS INT)) / NULLIF(SUM(CAST(signed_up      AS INT)), 0),       2) AS pct_signup_to_trial,
            ROUND(100.0 * SUM(CAST(converted_paid AS INT)) / NULLIF(SUM(CAST(started_trial  AS INT)), 0),       2) AS pct_trial_to_paid,

            -- ── End-to-end funnel rate ────────────────────────────────────
            -- The "north star" metric: of everyone who landed, how many paid?
            ROUND(100.0 * SUM(CAST(converted_paid AS INT)) / NULLIF(COUNT(*), 0),                               2) AS pct_overall_conversion

        FROM silver_funnel
        GROUP BY channel, partner_id
        ORDER BY pct_overall_conversion DESC
    """)
    n = con.execute("SELECT COUNT(*) FROM gold_funnel_overview").fetchone()[0]
    print(f"  gold_funnel_overview : {n} rows  (channel × partner combinations)")


    # ── 3b. Drop-off analysis ─────────────────────────────────────────────
    # For each combination of stage × channel × device we count how many
    # visitors dropped there. This directly answers "where is the biggest
    # leak?" and "does the leak look different on mobile vs desktop?",
    # which drives prioritisation of engineering and UX work.
    print("[GOLD] Building gold_dropoff_analysis …")
    con.execute("DROP TABLE IF EXISTS gold_dropoff_analysis")
    con.execute("""
        CREATE TABLE gold_dropoff_analysis AS
        SELECT
            dropped_at,
            channel,
            device,
            COUNT(*)                                                              AS visitors_dropped,

            -- Share of all non-converted visitors that dropped at this node.
            -- Helps normalise across channels with different traffic volumes.
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY dropped_at), 2) AS pct_of_stage_dropoffs,

            -- Overall conversion rate for this channel+device slice.
            -- Joining back to the silver total lets us rank slices by efficiency.
            ROUND(100.0 * SUM(CAST(converted_paid AS INT)) / NULLIF(COUNT(*), 0), 2) AS slice_conversion_rate

        FROM silver_funnel
        WHERE dropped_at IS NOT NULL   -- exclude converted visitors from drop-off counting
        GROUP BY dropped_at, channel, device
        ORDER BY visitors_dropped DESC
    """)
    n = con.execute("SELECT COUNT(*) FROM gold_dropoff_analysis").fetchone()[0]
    print(f"  gold_dropoff_analysis : {n} rows  (stage × channel × device)")


    # ── 3c. Time-to-convert ───────────────────────────────────────────────
    # Average and median days between each funnel step, per channel.
    # Medians are more robust than means here because a small number of
    # very-slow converters (e.g., someone who bookmarked the page and came
    # back months later) can heavily skew the mean.
    print("[GOLD] Building gold_time_to_convert …")
    con.execute("DROP TABLE IF EXISTS gold_time_to_convert")
    con.execute("""
        CREATE TABLE gold_time_to_convert AS
        SELECT
            channel,
            COUNT(*)                                           AS total_visitors,

            -- ── visit → signup ────────────────────────────────────────────
            ROUND(AVG(time_to_signup_days),    1)             AS avg_days_visit_to_signup,
            ROUND(MEDIAN(time_to_signup_days), 1)             AS med_days_visit_to_signup,

            -- ── signup → trial ────────────────────────────────────────────
            ROUND(AVG(time_to_trial_days),    1)              AS avg_days_signup_to_trial,
            ROUND(MEDIAN(time_to_trial_days), 1)              AS med_days_signup_to_trial,

            -- ── trial → paid ─────────────────────────────────────────────
            ROUND(AVG(time_to_paid_days),    1)               AS avg_days_trial_to_paid,
            ROUND(MEDIAN(time_to_paid_days), 1)               AS med_days_trial_to_paid,

            -- ── end-to-end (only for fully converted visitors) ───────────
            -- Total days from first visit to first payment. Business uses this
            -- to estimate "payback period" relative to CAC.
            ROUND(AVG(
                CASE WHEN converted_paid
                THEN time_to_signup_days + time_to_trial_days + time_to_paid_days
                END
            ), 1) AS avg_days_full_journey

        FROM silver_funnel
        GROUP BY channel
        ORDER BY avg_days_full_journey ASC NULLS LAST
    """)
    n = con.execute("SELECT COUNT(*) FROM gold_time_to_convert").fetchone()[0]
    print(f"  gold_time_to_convert : {n} rows  (one per channel)")


    # ── 3d. A/B test results ──────────────────────────────────────────────
    # Lift in percentage points (not relative %) is the business-standard
    # metric for A/B tests: it tells stakeholders "treatment converts 3pp
    # more than control" in absolute terms, which is more conservative and
    # less gameable than relative lift.
    # We also report sample sizes so that analysts can immediately apply a
    # χ² or z-test for proportions to validate statistical significance.
    print("[GOLD] Building gold_ab_test_results …")
    con.execute("DROP TABLE IF EXISTS gold_ab_test_results")
    con.execute("""
        CREATE TABLE gold_ab_test_results AS
        WITH per_variant AS (
            SELECT
                test_name,
                variant,
                COUNT(*)                                                   AS n_visitors,
                SUM(CAST(converted AS INT))                                AS n_converted,
                ROUND(100.0 * SUM(CAST(converted AS INT)) / COUNT(*), 2)  AS conversion_rate
            FROM bronze_ab_tests
            GROUP BY test_name, variant
        ),
        pivoted AS (
            -- Pivot control and treatment side by side for easy comparison.
            SELECT
                test_name,
                MAX(CASE WHEN variant = 'control'   THEN n_visitors      END) AS control_n,
                MAX(CASE WHEN variant = 'treatment' THEN n_visitors      END) AS treatment_n,
                MAX(CASE WHEN variant = 'control'   THEN conversion_rate END) AS control_conv_rate,
                MAX(CASE WHEN variant = 'treatment' THEN conversion_rate END) AS treatment_conv_rate
            FROM per_variant
            GROUP BY test_name
        )
        SELECT
            test_name,
            control_n,
            treatment_n,
            control_conv_rate,
            treatment_conv_rate,

            -- Absolute lift (pp) — treatment minus control.
            -- Positive = treatment wins. This is what we report to stakeholders.
            ROUND(treatment_conv_rate - control_conv_rate, 2) AS lift_pp,

            -- Relative lift (%) — how much better is treatment vs control?
            -- Useful for comparing tests of different base rates.
            ROUND(100.0 * (treatment_conv_rate - control_conv_rate)
                        / NULLIF(control_conv_rate, 0), 1)    AS lift_relative_pct,

            -- Minimum detectable effect check: if both arms have < 100 visitors
            -- flag the result as potentially underpowered (rough heuristic).
            CASE
                WHEN control_n < 100 OR treatment_n < 100 THEN 'underpowered'
                ELSE 'sufficient'
            END AS sample_size_flag

        FROM pivoted
        ORDER BY lift_pp DESC
    """)
    n = con.execute("SELECT COUNT(*) FROM gold_ab_test_results").fetchone()[0]
    print(f"  gold_ab_test_results : {n} rows  (one per A/B test)")


# ============================================================================
# EXPORT — Dump gold tables to CSV for BI tools / sharing
# ============================================================================

def export_gold() -> None:
    print("\n[EXPORT] Writing gold CSVs …")
    tables = [
        "gold_funnel_overview",
        "gold_dropoff_analysis",
        "gold_time_to_convert",
        "gold_ab_test_results",
    ]
    for tbl in tables:
        path = GOLD_DIR / f"{tbl}.csv"
        con.execute(f"COPY {tbl} TO '{path}' (HEADER, DELIMITER ',')")
        n = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  {tbl}.csv  ({n} rows)")


# ============================================================================
# SUMMARY — Executive print of the key insights
# ============================================================================

def print_summary() -> None:
    print("\n" + "=" * 65)
    print("  PIPELINE SUMMARY — KEY BUSINESS INSIGHTS")
    print("=" * 65)

    # ── Best channel overall ──────────────────────────────────────────────
    best_channel = con.execute("""
        SELECT channel,
               ROUND(AVG(pct_overall_conversion), 2) AS avg_conv
        FROM gold_funnel_overview
        GROUP BY channel
        ORDER BY avg_conv DESC
        LIMIT 1
    """).fetchone()
    print(f"\n  Best acquisition channel  :  {best_channel[0]}"
          f"  ({best_channel[1]}% visit→paid)")

    # ── Stage with the most absolute drop-off ────────────────────────────
    # We count from silver directly: how many visitors dropped at each stage.
    biggest_drop = con.execute("""
        SELECT dropped_at,
               COUNT(*) AS n_dropped
        FROM silver_funnel
        WHERE dropped_at IS NOT NULL
        GROUP BY dropped_at
        ORDER BY n_dropped DESC
        LIMIT 1
    """).fetchone()
    print(f"  Biggest drop-off stage    :  {biggest_drop[0]}"
          f"  ({biggest_drop[1]:,} visitors lost)")

    # ── Best A/B test ─────────────────────────────────────────────────────
    best_ab = con.execute("""
        SELECT test_name, lift_pp, treatment_conv_rate, control_conv_rate
        FROM gold_ab_test_results
        ORDER BY lift_pp DESC
        LIMIT 1
    """).fetchone()
    print(f"  Best A/B test             :  {best_ab[0]}"
          f"  (+{best_ab[1]} pp  |  {best_ab[3]}% → {best_ab[2]}%)")

    # ── Best device ───────────────────────────────────────────────────────
    best_device = con.execute("""
        SELECT device,
               ROUND(100.0 * SUM(CAST(converted_paid AS INT)) / COUNT(*), 2) AS conv_rate
        FROM silver_funnel
        GROUP BY device
        ORDER BY conv_rate DESC
        LIMIT 1
    """).fetchone()
    print(f"  Best converting device    :  {best_device[0]}"
          f"  ({best_device[1]}% visit→paid)")

    # ── Time-to-convert: referral vs paid_social ──────────────────────────
    speed = con.execute("""
        SELECT channel, avg_days_full_journey
        FROM gold_time_to_convert
        WHERE channel IN ('referral', 'paid_social')
        ORDER BY channel
    """).fetchall()
    speed_map = {row[0]: row[1] for row in speed}
    ref  = speed_map.get("referral",    "N/A")
    paid = speed_map.get("paid_social", "N/A")
    print(f"\n  Avg days visit→paid  —  referral: {ref}d  |  paid_social: {paid}d")
    if isinstance(ref, float) and isinstance(paid, float):
        faster = "referral" if ref < paid else "paid_social"
        print(f"  → {faster} converts faster end-to-end")

    # ── Best channel+device combo ─────────────────────────────────────────
    best_combo = con.execute("""
        SELECT channel, device,
               ROUND(100.0 * SUM(CAST(converted_paid AS INT)) / COUNT(*), 2) AS conv_rate
        FROM silver_funnel
        GROUP BY channel, device
        ORDER BY conv_rate DESC
        LIMIT 1
    """).fetchone()
    print(f"\n  Best channel+device combo :  {best_combo[0]} × {best_combo[1]}"
          f"  ({best_combo[2]}% conversion)")

    print("\n" + "=" * 65)


# ============================================================================
# MAIN
# ============================================================================

def main():
    print(f"DuckDB {duckdb.__version__}  |  DB: {DB_PATH}")
    print(f"Start: {datetime.utcnow().isoformat(timespec='seconds')} UTC")

    ingest_bronze()
    build_silver()
    build_gold()
    export_gold()
    print_summary()

    con.close()
    print(f"\nDone. All tables persisted in {DB_PATH}")


if __name__ == "__main__":
    main()
