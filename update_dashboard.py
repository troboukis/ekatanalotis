#!/usr/bin/env python3
"""
Updates dashboard_data.json incrementally from new daily data_*.csv files.

Normal operation:
  - Rebuilds the weekly-median pivot from the existing dashboard_data.json
    (which contains all historical aggregated data).
  - Loads only the newest daily CSV to get the current week's medians.
  - Appends or updates that week in the pivot, then recomputes all stats.

Bootstrap (no JSON yet):
  - Loads all available data_*.csv files from scratch.
  - Requires enough historical files to compute a meaningful baseline.
"""

import glob
import json
import os
import re

import numpy as np
import pandas as pd
from scipy.stats import linregress

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASELINE_WEEKS = 8
CURRENT_WEEK_DATA = os.path.join(SCRIPT_DIR, "current_week_data.csv")
DEDUP_COLS = ["product_id", "merchant", "date", "category_name", "price"]
CURRENT_WEEK_COLS = [
    "product_id", "name", "date", "unit", "category_name", "category_codes",
    "monimi_meiosi", "promo", "supplier_name", "supplier_code", "merchant", "price",
]


# ── Early-exit check ──────────────────────────────────────────────────────────

def _filename_date(path: str) -> pd.Timestamp:
    """Parse the D_M_Y date embedded in a data_D_M_Y.csv filename."""
    m = re.search(r"data_(\d+)_(\d+)_(\d+)\.csv$", os.path.basename(path))
    if m:
        return pd.Timestamp(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    return pd.Timestamp.min


def is_up_to_date(json_path: str, csv_files: list) -> bool:
    """
    Returns True if dashboard_data.json already reflects the newest CSV's data.

    Compares the API date inside the newest CSV to last_data_date in the JSON.
    The API date changes whenever the API publishes new prices — even if the
    ISO week number stays the same — so a date change means the week's medians
    need to be recalculated with fresh prices.
    """
    if not os.path.exists(json_path):
        return False

    with open(json_path, encoding="utf-8") as f:
        existing = json.load(f)
    last_data_date = existing.get("last_data_date")
    if not last_data_date:
        return False

    newest_csv = max(csv_files, key=_filename_date)
    raw_dates = pd.read_csv(newest_csv, usecols=["date"])["date"]
    csv_max_date = pd.to_datetime(raw_dates, format="%d-%m-%Y").max()

    if csv_max_date <= pd.Timestamp(last_data_date):
        print(f"  Ήδη ενημερωμένο (JSON: {last_data_date}, νεότερο CSV: {csv_max_date.date()})")
        return True

    print(f"  Νέα δεδομένα βρέθηκαν (JSON: {last_data_date} → CSV: {csv_max_date.date()})")
    return False


# ── Pivot reconstruction from JSON ────────────────────────────────────────────

def pivot_from_json(data: dict) -> tuple:
    """
    Reconstruct the weekly-median pivot DataFrame from dashboard_data.json.

    Returns:
        pivot          – DataFrame (categories × weeks) of median prices
        week_dates_map – dict {year_week: "YYYY-MM-DD"}
    """
    all_weeks = data["all_weeks"]
    all_dates = data["all_week_start_dates"]
    week_dates_map = dict(zip(all_weeks, all_dates))

    rows = {cat["name"]: dict(zip(all_weeks, cat["weekly_median"]))
            for cat in data["categories"]}
    pivot = pd.DataFrame(rows).T        # rows = categories, cols = year_weeks
    pivot.columns = all_weeks
    return pivot, week_dates_map


# ── New CSV ingestion ─────────────────────────────────────────────────────────

def load_new_csv_week_rows(csv_path: str) -> tuple:
    """
    Load one daily CSV and return the raw rows that belong to its dominant week.

    Returns:
        year_week      – e.g. "2026-W11"
        week_start     – "YYYY-MM-DD" of Monday that starts the week
        week_rows      – DataFrame of raw rows for the active week
        last_data_date – "YYYY-MM-DD" of the most recent date in the CSV
    """
    df = pd.read_csv(
        csv_path,
        usecols=CURRENT_WEEK_COLS,
        dtype={"product_id": str, "price": float},
    )
    df["date_dt"] = pd.to_datetime(df["date"], format="%d-%m-%Y")

    iso = df["date_dt"].dt.isocalendar()
    df["year_week"] = iso["year"].astype(str) + "-W" + iso["week"].astype(str).str.zfill(2)
    df["week_start"] = df["date_dt"] - pd.to_timedelta(df["date_dt"].dt.dayofweek, unit="D")

    # Take the dominant week (in case dates straddle a week boundary)
    dominant_week = df["year_week"].value_counts().index[0]
    week_df = df[df["year_week"] == dominant_week].copy()

    week_start = week_df["week_start"].iloc[0].strftime("%Y-%m-%d")
    last_data_date = df["date_dt"].max().strftime("%Y-%m-%d")
    week_df = week_df[CURRENT_WEEK_COLS].copy()

    return dominant_week, week_start, week_df, last_data_date


def rebuild_week_medians(week_df: pd.DataFrame) -> dict:
    """Compute category medians from the accumulated raw rows of the active week."""
    med_df = week_df[["category_name", "price"]].copy()
    med_df["cat_clean"] = med_df["category_name"].str.rsplit(",", n=1).str[-1].str.strip()
    return med_df.groupby("cat_clean")["price"].median().to_dict()


def get_week_info_from_rows(df: pd.DataFrame) -> tuple:
    """Infer year-week and Monday start from an accumulated current-week CSV."""
    dates = pd.to_datetime(df["date"], format="%d-%m-%Y")
    iso = dates.dt.isocalendar()
    year_week = (iso["year"].astype(str) + "-W" + iso["week"].astype(str).str.zfill(2)).value_counts().index[0]
    week_start = (dates - pd.to_timedelta(dates.dt.dayofweek, unit="D")).iloc[0].strftime("%Y-%m-%d")
    return year_week, week_start


def update_current_week_data(current_week_path: str, new_week: str, new_rows: pd.DataFrame) -> tuple:
    """
    Maintain a tracked accumulator for the active week and return its medians.

    - Same week: append new rows and deduplicate.
    - New week: replace the accumulator with the new week's first snapshot.
    """
    if os.path.exists(current_week_path):
        existing = pd.read_csv(
            current_week_path,
            usecols=CURRENT_WEEK_COLS,
            dtype={"product_id": str, "price": float},
        )
        existing_week, _ = get_week_info_from_rows(existing)
        if existing_week == new_week:
            combined = pd.concat([existing, new_rows], ignore_index=True)
        else:
            combined = new_rows.copy()
    else:
        combined = new_rows.copy()

    before = len(combined)
    combined = combined.drop_duplicates(subset=DEDUP_COLS)
    removed = before - len(combined)
    if removed:
        print(f"  Αφαιρέθηκαν {removed:,} διπλότυπα από το current_week_data.csv")

    combined = combined.sort_values(["date", "category_name", "product_id", "merchant"]).reset_index(drop=True)
    combined.to_csv(current_week_path, index=False)

    stored_week, stored_week_start = get_week_info_from_rows(combined)
    stored_last_data_date = pd.to_datetime(combined["date"], format="%d-%m-%Y").max().strftime("%Y-%m-%d")
    stored_medians = rebuild_week_medians(combined)

    return stored_week, stored_week_start, stored_medians, stored_last_data_date


def update_pivot(pivot: pd.DataFrame, week_dates_map: dict,
                 new_week: str, new_week_start: str, new_medians: dict) -> pd.DataFrame:
    """
    Add or update one week's data in the pivot, preserving all historical data.

    - If new_week is already a column, its values are overwritten.
    - New categories that appear in new_medians but not in the pivot are added
      (with NaN for all historical weeks).
    - Fills any gap weeks between the last known week and new_week.
    """
    # Add new categories
    new_cats = set(new_medians) - set(pivot.index)
    if new_cats:
        print(f"  {len(new_cats)} νέες κατηγορίες")
        for cat in new_cats:
            pivot.loc[cat] = np.nan

    # Add the new week column if needed
    if new_week not in pivot.columns:
        pivot[new_week] = np.nan
    week_dates_map[new_week] = new_week_start

    for cat, median in new_medians.items():
        pivot.loc[cat, new_week] = median

    # Re-sort columns and fill any gap weeks
    sorted_weeks = sorted(pivot.columns)
    all_dates = [week_dates_map[w] for w in sorted_weeks]
    first_monday = pd.Timestamp(all_dates[0])
    last_monday = pd.Timestamp(all_dates[-1])

    complete_weeks = []
    for monday in pd.date_range(first_monday, last_monday, freq="7D"):
        iso_w = monday.isocalendar()
        w = f"{iso_w.year}-W{iso_w.week:02d}"
        complete_weeks.append(w)
        if w not in week_dates_map:
            week_dates_map[w] = monday.strftime("%Y-%m-%d")

    missing = set(complete_weeks) - set(pivot.columns)
    if missing:
        print(f"  Συμπλήρωση {len(missing)} εβδομάδων χωρίς δεδομένα: {sorted(missing)}")

    pivot = pivot.reindex(columns=complete_weeks)
    return pivot


# ── Bootstrap: build pivot from raw CSVs ─────────────────────────────────────

def pivot_from_raw_csvs(files: list) -> tuple:
    """
    Build the full pivot from scratch by loading all available CSV files.
    Used only when no dashboard_data.json exists yet.

    Returns:
        pivot          – DataFrame (categories × weeks)
        week_dates_map – dict {year_week: "YYYY-MM-DD"}
        last_data_date – "YYYY-MM-DD"
    """
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_csv(f, usecols=DEDUP_COLS,
                                   dtype={"product_id": str, "price": float}))
        except Exception as e:
            print(f"  Παράλειψη {os.path.basename(f)}: {e}")

    if not dfs:
        raise RuntimeError("Κανένα αρχείο CSV δεν φορτώθηκε.")

    merged = pd.concat(dfs, ignore_index=True)
    before = len(merged)
    merged = merged.drop_duplicates(subset=DEDUP_COLS)
    if before != len(merged):
        print(f"  Αφαιρέθηκαν {before - len(merged):,} διπλότυπα")

    merged["date"] = pd.to_datetime(merged["date"], format="%d-%m-%Y")
    merged = merged.sort_values("date").reset_index(drop=True)

    df = merged[["date", "category_name", "price"]].copy()
    df["cat_clean"] = df["category_name"].str.rsplit(",", n=1).str[-1].str.strip()
    iso = df["date"].dt.isocalendar()
    df["year_week"] = iso["year"].astype(str) + "-W" + iso["week"].astype(str).str.zfill(2)
    df["week_start"] = df["date"] - pd.to_timedelta(df["date"].dt.dayofweek, unit="D")

    week_starts_raw = df.groupby("year_week")["week_start"].first()
    weekly_median = df.groupby(["cat_clean", "year_week"])["price"].median()
    pivot = weekly_median.unstack()
    week_order = sorted(pivot.columns)
    pivot = pivot[week_order]

    first_monday = week_starts_raw[week_order[0]]
    last_monday = week_starts_raw[week_order[-1]]
    week_dates_map = {}
    complete_weeks = []
    for monday in pd.date_range(first_monday, last_monday, freq="7D"):
        iso_w = monday.isocalendar()
        w = f"{iso_w.year}-W{iso_w.week:02d}"
        complete_weeks.append(w)
        week_dates_map[w] = monday.strftime("%Y-%m-%d")

    pivot = pivot.reindex(columns=complete_weeks)
    last_data_date = df["date"].max().strftime("%Y-%m-%d")
    return pivot, week_dates_map, last_data_date


# ── Stats computation ─────────────────────────────────────────────────────────

def compute_stats(pivot: pd.DataFrame, week_dates_map: dict, last_data_date: str) -> dict:
    """Compute all dashboard stats from the weekly-median pivot."""
    week_order = list(pivot.columns)
    week_start_dates = [week_dates_map[w] for w in week_order]

    # Baseline: median of first 8 weeks
    baseline_cols = week_order[:BASELINE_WEEKS]
    baseline = pivot[baseline_cols].median(axis=1)
    pct_from_baseline = pivot.subtract(baseline, axis=0).divide(baseline, axis=0) * 100

    # CAGR
    baseline_end_date = pd.Timestamp(week_start_dates[BASELINE_WEEKS - 1])
    last_week_date = pd.Timestamp(week_start_dates[-1])
    n_years = (last_week_date - baseline_end_date).days / 365
    last_week_col = week_order[-1]
    v_end = pivot[last_week_col]
    if n_years > 0:
        cagr = ((v_end / baseline) ** (1 / n_years) - 1) * 100
    else:
        cagr = pd.Series(0.0, index=pivot.index)

    pct_from_baseline_last = (v_end - baseline) / baseline * 100
    direction = (
        np.sign(pct_from_baseline_last)
        .map({1.0: "Αύξηση", -1.0: "Μείωση", 0.0: "Αμετάβλητη"})
        .fillna("Αμετάβλητη")
    )
    typical_pct = pct_from_baseline[week_order[BASELINE_WEEKS:]].median(axis=1)

    # Slope per month: derived from weekly medians grouped by calendar month.
    # This closely approximates the raw-data monthly median used in preprocess.py.
    dates = pd.to_datetime(week_start_dates)
    month_nums = pd.Series(
        [d.to_period("M").ordinal for d in dates], index=week_order
    )

    growth_data = {}
    for cat in pivot.index:
        vals = pivot.loc[cat]
        mask = vals.notna()
        if mask.sum() < 3:
            growth_data[cat] = {"slope_per_month": np.nan, "r_squared": np.nan}
            continue
        monthly = (
            pd.DataFrame({"month_num": month_nums[mask].values, "price": vals[mask].values})
            .groupby("month_num")["price"].median()
        )
        if len(monthly) < 3:
            growth_data[cat] = {"slope_per_month": np.nan, "r_squared": np.nan}
            continue
        slope, _, r, _, _ = linregress(monthly.index, monthly.values)
        growth_data[cat] = {
            "slope_per_month": round(float(slope), 4),
            "r_squared": round(float(r ** 2), 3),
        }
    growth = pd.DataFrame(growth_data).T

    # Weekly highlights
    weeks_with_data = [w for w in week_order if pivot[w].notna().any()]
    last_week = weeks_with_data[-1]
    prev_week = weeks_with_data[-2] if len(weeks_with_data) >= 2 else None

    top5_wow = []
    wow_summary = {"increase": 0, "decrease": 0, "unchanged": 0}
    if prev_week:
        wow_change = ((pivot[last_week] - pivot[prev_week]) / pivot[prev_week] * 100).dropna()
        for cat in wow_change.nlargest(5).index:
            top5_wow.append({
                "name": cat,
                "pct_change": round(float(wow_change[cat]), 1),
                "current_median": round(float(pivot.loc[cat, last_week]), 2),
                "prev_median": round(float(pivot.loc[cat, prev_week]), 2),
            })
        for sign in wow_change:
            if sign > 0:
                wow_summary["increase"] += 1
            elif sign < 0:
                wow_summary["decrease"] += 1
            else:
                wow_summary["unchanged"] += 1

    parts = last_week.split("-W")
    last_year_week = f"{int(parts[0]) - 1}-W{parts[1]}"
    top5_yoy = []
    if last_year_week in week_order:
        yoy_change = (
            (pivot[last_week] - pivot[last_year_week]) / pivot[last_year_week] * 100
        ).dropna()
        for cat in yoy_change.nlargest(5).index:
            top5_yoy.append({
                "name": cat,
                "pct_change": round(float(yoy_change[cat]), 1),
                "current_median": round(float(pivot.loc[cat, last_week]), 2),
                "last_year_median": round(float(pivot.loc[cat, last_year_week]), 2),
            })

    wow_all = pivot.pct_change(axis=1) * 100
    wow_median = wow_all.median(axis=1)
    wow_mad = (
        wow_all.subtract(wow_median, axis=0).abs().median(axis=1) * 1.4826
    ).clip(lower=0.5)
    if prev_week:
        latest_wow = (pivot[last_week] - pivot[prev_week]) / pivot[prev_week] * 100
    else:
        latest_wow = pd.Series(np.nan, index=pivot.index)
    zscore_latest = (latest_wow - wow_median) / wow_mad

    top5_zscore = []
    for cat in zscore_latest.dropna().nlargest(5).index:
        top5_zscore.append({
            "name": cat,
            "zscore": round(float(zscore_latest[cat]), 2),
            "wow_pct": round(float(latest_wow[cat]), 1),
            "current_median": round(float(pivot.loc[cat, last_week]), 2),
            "prev_median": (
                round(float(pivot.loc[cat, prev_week]), 2)
                if prev_week and pd.notna(pivot.loc[cat, prev_week])
                else None
            ),
        })

    heatmap_weeks = week_order[BASELINE_WEEKS:]
    heatmap_dates = week_start_dates[BASELINE_WEEKS:]

    categories_json = []
    for cat in sorted(pivot.index):
        cat_cagr = cagr.get(cat, np.nan)
        cat_pct = pct_from_baseline_last.get(cat, np.nan)
        cat_typical = typical_pct.get(cat, np.nan)
        cat_zscore = zscore_latest.get(cat, np.nan)
        cat_wow = latest_wow.get(cat, np.nan)
        g_slope = growth.loc[cat, "slope_per_month"] if cat in growth.index else np.nan
        g_r2 = growth.loc[cat, "r_squared"] if cat in growth.index else np.nan

        categories_json.append({
            "name": cat,
            "weekly_median": [
                round(float(v), 2) if pd.notna(v) else None for v in pivot.loc[cat]
            ],
            "pct_from_baseline": [
                round(float(v), 1) if pd.notna(v) else None
                for v in pct_from_baseline.loc[cat, heatmap_weeks]
            ],
            "stats": {
                "cagr": round(float(cat_cagr), 2) if pd.notna(cat_cagr) else None,
                "slope_per_month": round(float(g_slope), 4) if pd.notna(g_slope) else None,
                "r_squared": round(float(g_r2), 3) if pd.notna(g_r2) else None,
                "direction": direction.get(cat, "Αμετάβλητη"),
                "pct_from_baseline": round(float(cat_pct), 1) if pd.notna(cat_pct) else None,
                "pct_typical": round(float(cat_typical), 1) if pd.notna(cat_typical) else None,
                "zscore": round(float(cat_zscore), 2) if pd.notna(cat_zscore) else None,
                "wow_latest": round(float(cat_wow), 1) if pd.notna(cat_wow) else None,
            },
        })

    return {
        "weeks": heatmap_weeks,
        "week_start_dates": heatmap_dates,
        "all_weeks": week_order,
        "all_week_start_dates": week_start_dates,
        "last_updated": pd.Timestamp.now().strftime("%Y-%m-%d"),
        "last_data_date": last_data_date,
        "last_week": last_week,
        "prev_week": prev_week,
        "last_year_week": last_year_week if last_year_week in week_order else None,
        "baseline_period": f"{week_start_dates[0]} — {week_start_dates[BASELINE_WEEKS - 1]}",
        "baseline_weeks_count": BASELINE_WEEKS,
        "weekly_highlights": {
            "top5_wow": top5_wow,
            "top5_yoy": top5_yoy,
            "wow_summary": wow_summary,
            "top5_zscore": top5_zscore,
        },
        "categories": categories_json,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 50)
    print("Update dashboard_data.json")
    print("=" * 50)

    json_path = os.path.join(SCRIPT_DIR, "dashboard_data.json")
    csv_files = sorted(glob.glob(os.path.join(SCRIPT_DIR, "data_*.csv")))

    if not csv_files:
        print("  Δεν βρέθηκαν αρχεία data_*.csv — τίποτα να γίνει.")
        return

    print("\n[0/3] Έλεγχος αν χρειάζεται ενημέρωση...")
    if is_up_to_date(json_path, csv_files):
        return

    newest_csv = max(csv_files, key=_filename_date)

    if os.path.exists(json_path):
        # ── Incremental update ────────────────────────────────────────────────
        # Historical data lives in the JSON; we only need the newest CSV.
        print(f"\n[1/3] Φόρτωση ιστορικών δεδομένων από JSON...")
        with open(json_path, encoding="utf-8") as f:
            existing = json.load(f)
        pivot, week_dates_map = pivot_from_json(existing)
        print(f"  {len(pivot.index)} κατηγορίες, {len(pivot.columns)} εβδομάδες από JSON")

        print(f"\n[2/3] Φόρτωση νέου CSV: {os.path.basename(newest_csv)}")
        new_week, new_week_start, new_rows, csv_last_data_date = load_new_csv_week_rows(newest_csv)
        print(f"  Εβδομάδα: {new_week}, ημερομηνία CSV: {csv_last_data_date}, γραμμές: {len(new_rows):,}")

        new_week, new_week_start, new_medians, last_data_date = update_current_week_data(
            CURRENT_WEEK_DATA, new_week, new_rows
        )
        print(f"  current_week_data.csv: εβδομάδα {new_week}, ημερομηνία {last_data_date}, κατηγορίες: {len(new_medians)}")

        pivot = update_pivot(pivot, week_dates_map, new_week, new_week_start, new_medians)

    else:
        # ── Bootstrap from raw CSVs ───────────────────────────────────────────
        print(f"\n[1/3] Δεν υπάρχει JSON — φόρτωση όλων των CSV από μηδέν...")
        print(f"  (Απαιτούνται αρκετά ιστορικά αρχεία CSV)")
        pivot, week_dates_map, last_data_date = pivot_from_raw_csvs(csv_files)
        print(f"  {len(pivot.index)} κατηγορίες, {len(pivot.columns)} εβδομάδες")

    print("\n[3/3] Υπολογισμός στατιστικών...")
    output = compute_stats(pivot, week_dates_map, last_data_date)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False)

    file_size = os.path.getsize(json_path)
    print(f"\n{'=' * 50}")
    print(f"  dashboard_data.json ενημερώθηκε")
    print(f"  Κατηγορίες: {len(output['categories'])}")
    print(f"  Εβδομάδες: {len(output['all_weeks'])}")
    print(f"  Τελευταία ημερομηνία: {output['last_data_date']}")
    print(f"  Μέγεθος: {file_size / 1024:.0f} KB")


if __name__ == "__main__":
    main()
