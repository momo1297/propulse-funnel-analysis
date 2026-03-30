import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

SEED = 42
rng = np.random.default_rng(SEED)

N_VISITORS = 50_000
PARTNERS = ["HealthMedia", "WellnessPress", "NutriDigital"]
CHANNELS = ["organic_search", "email_campaign", "paid_social", "referral", "direct"]
COUNTRIES = ["US", "UK", "CA", "AU", "DE", "FR", "NL", "SE"]
DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.55, 0.35, 0.10]

# Conversion rates: visit→signup, signup→trial, trial→paid
CONV_RATES = {
    "organic_search": (0.35, 0.45, 0.30),
    "email_campaign": (0.52, 0.55, 0.42),
    "paid_social":    (0.28, 0.38, 0.22),
    "referral":       (0.61, 0.65, 0.55),
    "direct":         (0.44, 0.50, 0.38),
}

AB_TESTS = ["pricing_test", "cta_button", "trial_length"]
# Treatment lift per test (multiplicative on conversion probability)
AB_LIFT = {
    "pricing_test": 1.20,
    "cta_button":   1.15,
    "trial_length": 1.25,
}

START_DATE = datetime(2025, 1, 1)
END_DATE   = datetime(2025, 12, 31)
DATE_RANGE_DAYS = (END_DATE - START_DATE).days


def random_date(base: datetime, min_days: int, max_days: int) -> datetime:
    delta = rng.integers(min_days, max_days + 1)
    return base + timedelta(days=int(delta))


def build_funnel_events() -> pd.DataFrame:
    channels = rng.choice(CHANNELS, size=N_VISITORS, p=[0.30, 0.20, 0.25, 0.10, 0.15])
    partners = rng.choice(PARTNERS, size=N_VISITORS)
    countries = rng.choice(COUNTRIES, size=N_VISITORS)
    devices = rng.choice(DEVICES, size=N_VISITORS, p=DEVICE_WEIGHTS)

    landing_offsets = rng.integers(0, DATE_RANGE_DAYS, size=N_VISITORS)
    landing_dates = [START_DATE + timedelta(days=int(d)) for d in landing_offsets]

    rows = []
    for i in range(N_VISITORS):
        ch = channels[i]
        p_signup, p_trial, p_paid = CONV_RATES[ch]
        ld = landing_dates[i]

        # Step: signup
        signed_up = rng.random() < p_signup
        signup_date = random_date(ld, 0, 3) if signed_up else None

        # Step: trial (only if signed up)
        started_trial = (signed_up and rng.random() < p_trial)
        trial_date = random_date(signup_date, 1, 7) if started_trial else None

        # Step: paid (only if trial)
        converted_paid = (started_trial and rng.random() < p_paid)
        paid_date = random_date(trial_date, 7, 30) if converted_paid else None

        rows.append({
            "event_id":          f"EVT{i+1:07d}",
            "visitor_id":        f"VIS{i+1:07d}",
            "partner_id":        partners[i],
            "channel":           ch,
            "country":           countries[i],
            "device":            devices[i],
            "landing_date":      ld.date(),
            "visited":           True,
            "signed_up":         signed_up,
            "signup_date":       signup_date.date() if signup_date else None,
            "started_trial":     started_trial,
            "trial_date":        trial_date.date() if trial_date else None,
            "converted_paid":    converted_paid,
            "paid_date":         paid_date.date() if paid_date else None,
            # Boolean: did this step lead to the next?
            "visit_to_signup":   signed_up,
            "signup_to_trial":   started_trial,
            "trial_to_paid":     converted_paid,
        })

    return pd.DataFrame(rows)


def build_ab_tests(funnel_df: pd.DataFrame) -> pd.DataFrame:
    all_visitors = funnel_df["visitor_id"].values
    n_ab = int(N_VISITORS * 0.30)
    selected = rng.choice(all_visitors, size=n_ab, replace=False)

    # Assign one test per selected visitor
    tests = rng.choice(AB_TESTS, size=n_ab)
    variants = rng.choice(["control", "treatment"], size=n_ab, p=[0.50, 0.50])

    # Base conversion = whether they converted_paid in funnel
    base_conv = funnel_df.set_index("visitor_id")["converted_paid"].to_dict()

    rows = []
    for vid, test, variant in zip(selected, tests, variants):
        base = base_conv.get(vid, False)
        if variant == "treatment" and not base:
            # Apply lift: re-roll with boosted probability
            lift = AB_LIFT[test]
            # Approximate: use a base rate of 0.15 (average paid rate) * lift
            converted = rng.random() < (0.15 * lift)
        else:
            converted = bool(base)

        rows.append({
            "visitor_id": vid,
            "test_name":  test,
            "variant":    variant,
            "converted":  converted,
        })

    return pd.DataFrame(rows)


def print_summary(funnel_df: pd.DataFrame) -> None:
    print("\n" + "=" * 60)
    print("FUNNEL CONVERSION RATES BY CHANNEL")
    print("=" * 60)

    summary = (
        funnel_df.groupby("channel")
        .agg(
            visitors=("visited", "count"),
            signups=("signed_up", "sum"),
            trials=("started_trial", "sum"),
            paid=("converted_paid", "sum"),
        )
        .assign(
            visit_to_signup=lambda d: (d["signups"]  / d["visitors"]  * 100).round(1),
            signup_to_trial=lambda d: (d["trials"]   / d["signups"]   * 100).round(1),
            trial_to_paid  =lambda d: (d["paid"]     / d["trials"]    * 100).round(1),
            overall_conv   =lambda d: (d["paid"]     / d["visitors"]  * 100).round(2),
        )
    )

    print(summary[[
        "visitors", "signups", "trials", "paid",
        "visit_to_signup", "signup_to_trial", "trial_to_paid", "overall_conv"
    ]].to_string())

    visitors = int(funnel_df["visited"].count())
    signups  = int(funnel_df["signed_up"].sum())
    trials   = int(funnel_df["started_trial"].sum())
    paid     = int(funnel_df["converted_paid"].sum())
    print("\n" + "-" * 60)
    print(f"TOTAL  |  Visitors: {visitors:,}  "
          f"Signups: {signups:,} ({signups/visitors*100:.1f}%)  "
          f"Trials: {trials:,} ({trials/signups*100:.1f}%)  "
          f"Paid: {paid:,} ({paid/visitors*100:.2f}% overall)")
    print("=" * 60 + "\n")


def main():
    out_dir = Path(__file__).parent / "data" / "bronze"
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Generating funnel_events …")
    funnel_df = build_funnel_events()
    funnel_path = out_dir / "funnel_events.csv"
    funnel_df.to_csv(funnel_path, index=False)
    print(f"  Saved {len(funnel_df):,} rows → {funnel_path}")

    print("Generating ab_tests …")
    ab_df = build_ab_tests(funnel_df)
    ab_path = out_dir / "ab_tests.csv"
    ab_df.to_csv(ab_path, index=False)
    print(f"  Saved {len(ab_df):,} rows → {ab_path}")

    print_summary(funnel_df)


if __name__ == "__main__":
    main()
