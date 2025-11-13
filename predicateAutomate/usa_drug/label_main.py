import json
import logging
import sys
import os
import requests
from datetime import datetime
from label_fetcher import FDALabelFetcher
from label_mapper import FDALabelMapper
from config import Config


logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('fda_label_fetcher.log')
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Main execution function for FDA Label data fetching and processing"""
    start_time = datetime.now()
    logger.info("=" * 80)
    logger.info("FDA Drug Label Data Fetcher - Started")
    logger.info("=" * 80)
    
    trial_mode = Config.TRIAL_LIMIT > 0
    if trial_mode:
        logger.info(f"TRIAL MODE: Processing only {Config.TRIAL_LIMIT} records")
    
    try:
        # Connect to database first
        mapper = FDALabelMapper()
        if not mapper.connect():
            logger.error("Failed to connect to database. Exiting.")
            return 1
        
        try:
            initial_count = mapper.get_table_count()
            logger.info(f"Initial database count: {initial_count}")
            
            # Step 1: Fetch and process incrementally
            fetcher = FDALabelFetcher()
            
            logger.info("Starting label data download and processing...")
            logger.info("=" * 80)
            logger.info("Processing files incrementally to save memory")
            logger.info("=" * 80)
            
            # Get metadata
            total_estimated = fetcher.get_metadata()
            if total_estimated:
                estimated_files = fetcher.calculate_required_files(total_estimated)
            else:
                logger.warning("Could not get metadata, will download until 404")
                estimated_files = 100
            
            # Process each file immediately after download
            all_stats = {
                'total_records': 0,
                'inserted': 0,
                'duplicates': 0,
                'skipped': 0,
                'errors': 0
            }
            
            records_processed = 0
            
            for part_num in range(1, estimated_files + 10):
                # Try different file name patterns
                patterns = [
                    f"drug-label-{part_num:04d}-of-{estimated_files:04d}.json.zip",
                    f"drug-label-{part_num:04d}-of-{estimated_files+5:04d}.json.zip",
                    f"drug-label-{part_num:04d}-of-{estimated_files+10:04d}.json.zip",
                ]
                
                file_records = None
                for pattern in patterns:
                    file_url = fetcher.base_url + pattern
                    local_path = os.path.join(fetcher.output_dir, pattern)
                    
                    # Check if already exists
                    if not os.path.exists(local_path):
                        try:
                            response = requests.head(file_url, timeout=10)
                            if response.status_code == 200:
                                logger.info(f"[Part {part_num}] Downloading {pattern}...")
                                if fetcher.download_file(file_url, local_path):
                                    file_records = fetcher.extract_labels_from_zip(local_path)
                                    # Delete ZIP file immediately to save disk space
                                    os.remove(local_path)
                                    logger.info(f"Cleaned up ZIP file: {pattern}")
                                    break
                        except:
                            continue
                    else:
                        logger.info(f"[Part {part_num}] Already exists, extracting...")
                        file_records = fetcher.extract_labels_from_zip(local_path)
                        os.remove(local_path)
                        logger.info(f"Cleaned up ZIP file: {pattern}")
                        break
                
                if not file_records and part_num > estimated_files:
                    logger.info(f"No more files found after part {part_num-1}")
                    break
                
                if not file_records:
                    continue
                
                # Apply trial mode limit if enabled
                if trial_mode and records_processed >= Config.TRIAL_LIMIT:
                    logger.info(f"TRIAL MODE: Reached limit of {Config.TRIAL_LIMIT} records")
                    break
                
                if trial_mode:
                    remaining = Config.TRIAL_LIMIT - records_processed
                    if len(file_records) > remaining:
                        file_records = file_records[:remaining]
                
                # Process this batch immediately
                logger.info(f"Processing batch of {len(file_records)} records from part {part_num}...")
                batch_stats = mapper.process_fda_records(file_records)
                
                # Aggregate stats
                all_stats['total_records'] += batch_stats['total_records']
                all_stats['inserted'] += batch_stats['inserted']
                all_stats['duplicates'] += batch_stats.get('duplicates', batch_stats.get('skipped', 0))
                all_stats['skipped'] += batch_stats['skipped']
                all_stats['errors'] += batch_stats['errors']
                
                records_processed += len(file_records)
                
                logger.info(f"Cumulative Stats: Processed: {all_stats['total_records']} | "
                           f"Inserted: {all_stats['inserted']} | "
                           f"Duplicates: {all_stats['duplicates']} | "
                           f"Skipped: {all_stats['skipped']} | "
                           f"Errors: {all_stats['errors']}")
                
                # Free memory
                file_records = None
            
            final_count = mapper.get_table_count()
            
            logger.info("=" * 80)
            logger.info("Database Insertion Statistics")
            logger.info("=" * 80)
            logger.info(f"Total Records Processed: {all_stats['total_records']}")
            logger.info(f"Successfully Inserted: {all_stats['inserted']}")
            logger.info(f"Duplicates Skipped: {all_stats['duplicates']}")
            logger.info(f"Skipped (Insufficient Data): {all_stats['skipped']}")
            logger.info(f"Errors: {all_stats['errors']}")
            logger.info(f"Database Count Before: {initial_count}")
            logger.info(f"Database Count After: {final_count}")
            logger.info(f"Net Increase: {final_count - initial_count}")
            logger.info("=" * 80)
            
            db_stats = all_stats
            total_records = all_stats['total_records']
            raw_file = "N/A (incremental processing)"
            stats_file = "N/A"
            stats = {'unique_brands': 'N/A', 'unique_manufacturers': 'N/A'}
            
        finally:
            mapper.close()
        
        # Final summary
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=" * 80)
        logger.info("FDA Drug Label Data Fetcher - Completed Successfully")
        logger.info("=" * 80)
        logger.info(f"Total records processed: {total_records}")
        logger.info(f"Records inserted: {db_stats['inserted']}")
        logger.info(f"Duration: {duration}")
        
        if trial_mode:
            logger.info(f"⚠️  TRIAL MODE: Only {Config.TRIAL_LIMIT} records processed")
            logger.info(f"To process all records, set TRIAL_LIMIT=0 in config.py")
        
        logger.info("=" * 80)
        
        return 0
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        logger.error("=" * 80)
        logger.error("FDA Drug Label Data Fetcher - Failed")
        logger.error("=" * 80)
        return 1


if __name__ == "__main__":
    sys.exit(main())

