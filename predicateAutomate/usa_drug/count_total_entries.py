# predicateAutomate/usa_drug/count_total_entries.py
# Script to calculate total database entries from FDA records
# Each FDA record can have multiple submissions and products
# Total entries = Sum of (submissions × products) for each record

import json
import sys
from pathlib import Path

def analyze_fda_records(json_file_path: str):
    """
    Analyze FDA records and calculate total entries
    
    Args:
        json_file_path: Path to FDA raw JSON data file
    """
    print("=" * 80)
    print("FDA Drug Records Analysis - Database Entry Count")
    print("=" * 80)
    
    # Load FDA records
    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)
        
        # Handle different JSON structures
        if isinstance(data, dict) and 'data' in data:
            # Wrapper structure with metadata
            fda_records = data['data']
            print(f"✓ Loaded {len(fda_records)} FDA records from file")
            if 'total_records' in data:
                print(f"  (Total records in file: {data['total_records']})")
        elif isinstance(data, list):
            # Direct array of records
            fda_records = data
            print(f"✓ Loaded {len(fda_records)} FDA records from file")
        else:
            print(f"✗ Unexpected JSON structure")
            return
    except Exception as e:
        print(f"✗ Error loading file: {e}")
        return
    
    # Initialize counters
    total_fda_records = len(fda_records)
    total_entries = 0
    
    # Detailed breakdown
    records_by_entry_count = {}  # {entry_count: number_of_records}
    max_entries_per_record = 0
    max_entries_record = None
    
    # Statistics
    total_submissions = 0
    total_products = 0
    records_with_no_products = 0
    records_with_no_submissions = 0
    
    print("\nAnalyzing records...")
    print("-" * 80)
    
    # Analyze each FDA record
    for idx, record in enumerate(fda_records, 1):
        application_number = record.get('application_number', 'Unknown')
        submissions = record.get('submissions', [])
        products = record.get('products', [])
        
        num_submissions = len(submissions)
        num_products = len(products)
        
        # Calculate entries for this record (cross join)
        entries_for_record = num_submissions * num_products
        total_entries += entries_for_record
        
        # Track statistics
        total_submissions += num_submissions
        total_products += num_products
        
        if num_products == 0:
            records_with_no_products += 1
        if num_submissions == 0:
            records_with_no_submissions += 1
        
        # Track entry count distribution
        if entries_for_record not in records_by_entry_count:
            records_by_entry_count[entries_for_record] = 0
        records_by_entry_count[entries_for_record] += 1
        
        # Track max entries
        if entries_for_record > max_entries_per_record:
            max_entries_per_record = entries_for_record
            max_entries_record = {
                'application_number': application_number,
                'submissions': num_submissions,
                'products': num_products,
                'entries': entries_for_record
            }
        
        # Show progress
        if idx % 1000 == 0 or idx == total_fda_records:
            print(f"Progress: {idx}/{total_fda_records} records ({idx*100//total_fda_records}%) | "
                  f"Entries so far: {total_entries:,}")
    
    # Calculate averages
    avg_submissions = total_submissions / total_fda_records if total_fda_records > 0 else 0
    avg_products = total_products / total_fda_records if total_fda_records > 0 else 0
    avg_entries = total_entries / total_fda_records if total_fda_records > 0 else 0
    
    # Display results
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total FDA Records:           {total_fda_records:,}")
    print(f"Total Submissions:           {total_submissions:,}")
    print(f"Total Products:              {total_products:,}")
    print(f"\n{'=' * 80}")
    print(f"TOTAL DATABASE ENTRIES:      {total_entries:,}")
    print(f"{'=' * 80}")
    
    print(f"\nAverages per FDA Record:")
    print(f"  - Submissions:             {avg_submissions:.2f}")
    print(f"  - Products:                {avg_products:.2f}")
    print(f"  - Database Entries:        {avg_entries:.2f}")
    
    print(f"\nRecords with Issues:")
    print(f"  - No Products:             {records_with_no_products:,}")
    print(f"  - No Submissions:          {records_with_no_submissions:,}")
    
    print(f"\nMaximum Entries from Single FDA Record:")
    if max_entries_record:
        print(f"  - Application:             {max_entries_record['application_number']}")
        print(f"  - Submissions:             {max_entries_record['submissions']}")
        print(f"  - Products:                {max_entries_record['products']}")
        print(f"  - Total Entries:           {max_entries_record['entries']:,}")
    
    # Show distribution (top 10 entry counts)
    print(f"\nDistribution of Entries per FDA Record (Top 10):")
    print(f"{'Entries/Record':<20} {'# of FDA Records':<20}")
    print("-" * 40)
    sorted_dist = sorted(records_by_entry_count.items(), key=lambda x: x[1], reverse=True)[:10]
    for entry_count, record_count in sorted_dist:
        print(f"{entry_count:<20} {record_count:<20,}")
    
    print("\n" + "=" * 80)
    print(f"✓ Analysis Complete")
    print(f"✓ Expected database insertions: {total_entries:,} records")
    print("=" * 80)
    
    # Save detailed report
    report_file = json_file_path.replace('.json', '_entry_count_report.json')
    report = {
        'total_fda_records': total_fda_records,
        'total_submissions': total_submissions,
        'total_products': total_products,
        'total_database_entries': total_entries,
        'averages': {
            'submissions_per_record': round(avg_submissions, 2),
            'products_per_record': round(avg_products, 2),
            'entries_per_record': round(avg_entries, 2)
        },
        'issues': {
            'records_with_no_products': records_with_no_products,
            'records_with_no_submissions': records_with_no_submissions
        },
        'max_entries_record': max_entries_record,
        'distribution': records_by_entry_count
    }
    
    try:
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"\n✓ Detailed report saved to: {report_file}")
    except Exception as e:
        print(f"\n✗ Could not save report: {e}")


def main():
    """Main function"""
    # Default to the raw data file
    json_file = Path(__file__).parent / 'output' / 'fda_drugs_raw.json'
    
    # Allow override from command line
    if len(sys.argv) > 1:
        json_file = Path(sys.argv[1])
    
    if not json_file.exists():
        print(f"✗ Error: File not found: {json_file}")
        print(f"\nUsage: python {Path(__file__).name} [path_to_fda_raw_json]")
        print(f"Default: {json_file}")
        sys.exit(1)
    
    analyze_fda_records(str(json_file))


if __name__ == "__main__":
    main()

