#!/bin/bash

# Workflow:
# 1. Commit τοπικές αλλαγές
# 2. Pull από GitHub
# 3. Μεταφορά dated CSVs στο csv_archive
# 4. Merge όλων των CSVs
# 5. Push στο GitHub

set -e  # Σταμάτα αν κάτι αποτύχει

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "=============================================="
echo "  E-Katanalotis CSV Workflow"
echo "=============================================="

# 1. Commit τοπικές αλλαγές
echo ""
echo "[1/5] Commit τοπικών αλλαγών..."
if git diff --quiet && git diff --cached --quiet; then
    echo "  Δεν υπάρχουν αλλαγές για commit"
else
    git add -A
    git commit -m "Update data $(date '+%d-%m-%Y')" || true
    echo "  Commit ολοκληρώθηκε"
fi

# 2. Pull από GitHub
echo ""
echo "[2/5] Pull από GitHub..."
git pull --rebase origin main
echo "  Pull ολοκληρώθηκε"

# 3. Μεταφορά dated CSVs στο csv_archive
echo ""
echo "[3/5] Μεταφορά dated CSVs στο csv_archive..."
ARCHIVE_DIR="$SCRIPT_DIR/csv_archive"
mkdir -p "$ARCHIVE_DIR"

count=0
for file in "$SCRIPT_DIR"/data_*_*_*.csv "$SCRIPT_DIR"/fresh_basket_*_*_*.csv; do
    if [ -f "$file" ]; then
        mv "$file" "$ARCHIVE_DIR/"
        echo "  Moved: $(basename "$file")"
        ((count++))
    fi
done
echo "  Μεταφέρθηκαν $count αρχεία"

# 4. Merge CSVs
echo ""
echo "[4/5] Merge CSV αρχείων..."
python3 "$SCRIPT_DIR/merge_csv.py"

# 5. Push στο GitHub
echo ""
echo "[5/5] Push στο GitHub..."
if git diff --quiet && git diff --cached --quiet; then
    echo "  Δεν υπάρχουν νέες αλλαγές για push"
else
    git add -A
    git commit -m "Update data.csv $(date '+%d-%m-%Y')"
    git push origin main
    echo "  Push ολοκληρώθηκε"
fi

echo ""
echo "=============================================="
echo "  Ολοκληρώθηκε!"
echo "=============================================="
