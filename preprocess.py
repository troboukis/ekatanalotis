#!/usr/bin/env python3
"""
Διαβάζει το data.csv και παράγει dashboard_data.json
για τη σελίδα prices.html.
"""

import pandas as pd
import numpy as np
import json
import os
from scipy.stats import linregress

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))


def main():
    print("=" * 50)
    print("Preprocessing dashboard data")
    print("=" * 50)

    # Step 1: Load only needed columns
    print("\n[1/6] Φόρτωση data.csv...")
    df = pd.read_csv(
        os.path.join(SCRIPT_DIR, "data.csv"),
        parse_dates=["date"],
        date_format="%Y-%m-%d",
        usecols=["date", "category_name", "price"],
    )
    print(f"  {len(df):,} γραμμές")

    # Step 2: Derive cat_clean
    print("[2/6] Υπολογισμός cat_clean...")
    df["cat_clean"] = df["category_name"].str.rsplit(",", n=1).str[-1].str.strip()
    df.drop(columns=["category_name"], inplace=True)

    # Step 3: Weekly median per category
    print("[3/6] Εβδομαδιαία median τιμή ανά κατηγορία...")
    iso = df["date"].dt.isocalendar()
    df["year_week"] = iso["year"].astype(str) + "-W" + iso["week"].astype(str).str.zfill(2)
    df["week_start"] = df["date"] - pd.to_timedelta(df["date"].dt.dayofweek, unit="D")

    weekly_median = df.groupby(["cat_clean", "year_week"])["price"].median()
    pivot = weekly_median.unstack()
    week_order = sorted(pivot.columns)
    pivot = pivot[week_order]

    # Fill gaps: generate complete weekly sequence (no missing weeks)
    week_starts_map = df.groupby("year_week")["week_start"].first()
    first_monday = week_starts_map[week_order[0]]
    last_monday = week_starts_map[week_order[-1]]
    all_mondays = pd.date_range(first_monday, last_monday, freq="7D")

    complete_weeks = []
    complete_dates = []
    for monday in all_mondays:
        iso = monday.isocalendar()
        complete_weeks.append(f"{iso.year}-W{iso.week:02d}")
        complete_dates.append(monday.strftime("%Y-%m-%d"))

    missing = set(complete_weeks) - set(week_order)
    if missing:
        print(f"  Συμπλήρωση {len(missing)} εβδομάδων χωρίς δεδομένα: {sorted(missing)}")

    pivot = pivot.reindex(columns=complete_weeks)
    week_order = complete_weeks
    week_start_dates = complete_dates

    # Step 4: Percent change from baseline (median of first 8 weeks)
    print("[4/6] Υπολογισμός % change from baseline (median πρώτων 8 εβδομάδων)...")
    baseline_weeks = 8
    baseline_cols = week_order[:baseline_weeks]
    baseline = pivot[baseline_cols].median(axis=1)  # median τιμή ανά κατηγορία στις πρώτες 8 εβδομάδες
    pct_from_baseline = pivot.subtract(baseline, axis=0).divide(baseline, axis=0) * 100

    # Step 5: Per-category stats
    print("[5/6] Υπολογισμός stats (CAGR, slope, R²)...")

    # CAGR — using first and last weekly median with actual time span
    first_week_date = pd.Timestamp(week_start_dates[0])
    last_week_date = pd.Timestamp(week_start_dates[-1])
    n_years_actual = (last_week_date - first_week_date).days / 365

    # V(t0) = median of first week, V(tn) = median of last week (per category)
    first_week_col = week_order[0]
    last_week_col = week_order[-1]
    v_start = pivot[first_week_col]
    v_end = pivot[last_week_col]

    if n_years_actual > 0:
        cagr = ((v_end / v_start) ** (1 / n_years_actual) - 1) * 100
    else:
        cagr = pd.Series(0.0, index=pivot.index)

    # Total pct change & direction (also based on first/last week)
    yearly = df.groupby(["cat_clean", df["date"].dt.year])["price"].median().unstack()
    years = sorted(yearly.columns)
    first_year, last_year = years[0], years[-1]

    # Total pct change
    total_pct = ((yearly[last_year] - yearly[first_year]) / yearly[first_year] * 100)

    # Direction
    direction = np.sign(total_pct).map({1.0: "Αύξηση", -1.0: "Μείωση", 0.0: "Αμετάβλητη"}).fillna("Αμετάβλητη")

    # Slope per month via linear regression
    monthly = df.groupby([df["date"].dt.to_period("M"), "cat_clean"])["price"].median().reset_index()
    monthly.columns = ["month", "cat_clean", "price"]
    monthly["month_num"] = monthly["month"].apply(lambda x: x.ordinal)

    def calc_slope(g):
        if len(g) < 3:
            return pd.Series({"slope_per_month": np.nan, "r_squared": np.nan})
        slope, intercept, r, p, se = linregress(g["month_num"], g["price"])
        return pd.Series({"slope_per_month": round(slope, 4), "r_squared": round(r**2, 3)})

    growth = monthly.groupby("cat_clean").apply(calc_slope, include_groups=False)

    # Step 6: Weekly highlights — top 5 increases
    print("[6/6] Υπολογισμός weekly highlights...")

    last_week = week_order[-1]
    prev_week = week_order[-2] if len(week_order) >= 2 else None

    # Week-over-week
    top5_wow = []
    if prev_week:
        wow_change = ((pivot[last_week] - pivot[prev_week]) / pivot[prev_week] * 100).dropna()
        for cat in wow_change.nlargest(5).index:
            top5_wow.append({
                "name": cat,
                "pct_change": round(float(wow_change[cat]), 1),
                "current_median": round(float(pivot.loc[cat, last_week]), 2),
                "prev_median": round(float(pivot.loc[cat, prev_week]), 2),
            })

    # WoW summary counts (all categories)
    wow_summary = {"increase": 0, "decrease": 0, "unchanged": 0}
    if prev_week:
        for sign in wow_change:
            if sign > 0:
                wow_summary["increase"] += 1
            elif sign < 0:
                wow_summary["decrease"] += 1
            else:
                wow_summary["unchanged"] += 1

    # Year-over-year (same week number, previous year)
    top5_yoy = []
    # Parse last_week to find same week last year
    parts = last_week.split("-W")
    last_year_week = f"{int(parts[0]) - 1}-W{parts[1]}"
    if last_year_week in week_order:
        yoy_change = ((pivot[last_week] - pivot[last_year_week]) / pivot[last_year_week] * 100).dropna()
        for cat in yoy_change.nlargest(5).index:
            top5_yoy.append({
                "name": cat,
                "pct_change": round(float(yoy_change[cat]), 1),
                "current_median": round(float(pivot.loc[cat, last_week]), 2),
                "last_year_median": round(float(pivot.loc[cat, last_year_week]), 2),
            })

    # Exclude baseline period — show all weeks after the first 8
    heatmap_weeks = week_order[baseline_weeks:]
    heatmap_dates = week_start_dates[baseline_weeks:]

    # Assemble JSON
    print("\nΣυναρμολόγηση JSON...")
    categories_json = []
    for cat in sorted(pivot.index):
        cat_stats = {
            "cagr": round(float(cagr.get(cat, np.nan)), 2) if pd.notna(cagr.get(cat, np.nan)) else None,
            "slope_per_month": float(growth.loc[cat, "slope_per_month"]) if cat in growth.index and pd.notna(growth.loc[cat, "slope_per_month"]) else None,
            "r_squared": float(growth.loc[cat, "r_squared"]) if cat in growth.index and pd.notna(growth.loc[cat, "r_squared"]) else None,
            "direction": direction.get(cat, "Αμετάβλητη"),
            "pct_change_total": round(float(total_pct.get(cat, np.nan)), 1) if pd.notna(total_pct.get(cat, np.nan)) else None,
        }

        # Full weekly_median for line chart, trimmed pct for heatmap
        categories_json.append({
            "name": cat,
            "weekly_median": [round(float(v), 2) if pd.notna(v) else None for v in pivot.loc[cat]],
            "pct_from_baseline": [round(float(v), 1) if pd.notna(v) else None for v in pct_from_baseline.loc[cat, heatmap_weeks]],
            "stats": cat_stats,
        })

    output = {
        "weeks": heatmap_weeks,
        "week_start_dates": heatmap_dates,
        "all_weeks": week_order,
        "all_week_start_dates": week_start_dates,
        "last_updated": pd.Timestamp.now().strftime("%Y-%m-%d"),
        "last_week": last_week,
        "prev_week": prev_week,
        "last_year_week": last_year_week if last_year_week in week_order else None,
        "baseline_period": f"{week_start_dates[0]} — {week_start_dates[baseline_weeks - 1]}",
        "baseline_weeks_count": baseline_weeks,
        "weekly_highlights": {
            "top5_wow": top5_wow,
            "top5_yoy": top5_yoy,
            "wow_summary": wow_summary,
        },
        "categories": categories_json,
    }

    # Write
    output_path = os.path.join(SCRIPT_DIR, "dashboard_data.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False)

    file_size = os.path.getsize(output_path)
    print(f"\n{'=' * 50}")
    print(f"Dashboard data: {output_path}")
    print(f"  Κατηγορίες: {len(categories_json)}")
    print(f"  Εβδομάδες: {len(week_order)}")
    print(f"  Μέγεθος: {file_size / 1024:.0f} KB")
    print(f"  Top 5 WoW: {[x['name'] for x in top5_wow]}")
    print(f"  Top 5 YoY: {[x['name'] for x in top5_yoy]}")


if __name__ == "__main__":
    main()
