echo "==================================================================="
echo "Database Insertion Monitor"
echo "==================================================================="
echo ""
echo "Initial count from logs: 168,845"
echo "Target: Processing 26,000 FDA records (actual DB entries will be higher due to submissions×products)"
echo ""
echo "Press Ctrl+C to stop monitoring"
echo ""

while true; do
    current_count=$(psql -h localhost -U postgres -d quriousri_db -t -c "SELECT COUNT(*) FROM drug.drug_predicate_assessments;" 2>/dev/null | xargs)
    
    if [ -n "$current_count" ]; then
        inserted=$((current_count - 168845))
        timestamp=$(date '+%Y-%m-%d %H:%M:%S')
        echo "[$timestamp] Current count: $current_count | Inserted: $inserted new records"
    else
        echo "Error: Could not query database"
    fi
    
    sleep 10
done
