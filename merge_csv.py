#!/usr/bin/env python3
"""
Διαβάζει τα CSV από το csv_archive/ και τα ενώνει σε:
- data.csv (από data_*.csv)
- fresh_basket.csv (από fresh_basket_*.csv)
"""

import pandas as pd
import glob
import os
from tqdm import tqdm

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ARCHIVE_DIR = os.path.join(SCRIPT_DIR, "csv_archive")


def merge_files(pattern: str, output_file: str, sort_by: str = None, date_columns: list = None, dtype: dict = None) -> int:
    """Ενώνει αρχεία που ταιριάζουν με το pattern σε ένα output file."""
    files = glob.glob(os.path.join(ARCHIVE_DIR, pattern))

    if not files:
        print(f"Δεν βρέθηκαν αρχεία για {pattern}")
        return 0

    print(f"Βρέθηκαν {len(files)} αρχεία για {pattern}")

    # Διάβασμα με progress bar
    dfs = []
    for f in tqdm(files, desc="  Διάβασμα", unit="αρχείο", mininterval=0, ncols=80):
        try:
            df = pd.read_csv(f, dtype=dtype)
            dfs.append(df)
        except Exception as e:
            tqdm.write(f"  Σφάλμα στο {os.path.basename(f)}: {e}")

    if not dfs:
        return 0

    # Ένωση
    print("  Ένωση αρχείων...")
    merged = pd.concat(dfs, ignore_index=True)

    # Αφαίρεση διπλότυπων
    before = len(merged)
    merged = merged.drop_duplicates()
    after = len(merged)

    if before != after:
        print(f"  Αφαιρέθηκαν {before - after:,} διπλότυπα")

    # Μετατροπή ημερομηνιών και ταξινόμηση
    if date_columns:
        for col in date_columns:
            if col in merged.columns:
                merged[col] = pd.to_datetime(merged[col], format="%d-%m-%Y")

    if sort_by and sort_by in merged.columns:
        print(f"  Ταξινόμηση κατά {sort_by}...")
        merged = merged.sort_values(by=sort_by).reset_index(drop=True)

    # Αποθήκευση
    print("  Αποθήκευση...")
    output_path = os.path.join(SCRIPT_DIR, output_file)
    merged.to_csv(output_path, index=False)
    print(f"  Αποθηκεύτηκε: {output_file} ({len(merged):,} γραμμές)")

    return len(merged)


def main():
    print("=" * 50)
    print("Ένωση CSV αρχείων από csv_archive/")
    print("=" * 50)

    # Έλεγχος ύπαρξης φακέλου
    if not os.path.exists(ARCHIVE_DIR):
        print(f"Ο φάκελος {ARCHIVE_DIR} δεν υπάρχει!")
        return

    print()

    # Ένωση data files
    print("[1/2] Επεξεργασία data_*.csv...")
    data_rows = merge_files("data_*.csv", "data.csv",
                            sort_by="date", date_columns=["date"],
                            dtype={"product_id": str})

    print()

    # Ένωση fresh_basket files
    print("[2/2] Επεξεργασία fresh_basket_*.csv...")
    fb_rows = merge_files("fresh_basket_*.csv", "fresh_basket.csv",
                          sort_by="from", date_columns=["from", "to"])

    print()
    print("=" * 50)
    print(f"Ολοκληρώθηκε!")
    print(f"  data.csv: {data_rows:,} γραμμές")
    print(f"  fresh_basket.csv: {fb_rows:,} γραμμές")


if __name__ == "__main__":
    main()
