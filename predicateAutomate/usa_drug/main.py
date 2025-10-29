import json
import logging
import sys
from datetime import datetime
from fetcher import FDADrugFetcher
from models import transform_to_flat_records
from db_mapper import FDADrugDBMapper
from config import Config


logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('fda_drug_fetcher.log')
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Main execution function"""
    start_time = datetime.now()
    logger.info("=" * 80)
    logger.info("FDA Drug Data Fetcher - Started")
    logger.info("=" * 80)
    trial_mode = Config.TRIAL_LIMIT > 0
    if trial_mode:
        logger.info(f"TRIAL MODE: Processing only {Config.TRIAL_LIMIT} records")
    
    try:
        fetcher = FDADrugFetcher()
        
        # Download and fetch all data from bulk file
        logger.info("Starting bulk data download and processing...")
        all_records = fetcher.fetch_all_data(
            save_intermediate=True, 
            max_skip=Config.FDA_MAX_SKIP  # Kept for compatibility, ignored in bulk download
        )
        
        total_records = len(all_records)
        logger.info(f"Total records downloaded: {total_records}")
        
        # Apply trial mode limit if enabled (after download)
        if trial_mode:
            logger.info(f"TRIAL MODE: Limiting processing to first {Config.TRIAL_LIMIT} records")
            all_records = all_records[:Config.TRIAL_LIMIT]
            logger.info(f"Processing {len(all_records)} records (trial mode)")
        else:
            logger.info(f"Processing all {len(all_records)} records (production mode)")
        
        logger.info("Saving raw data...")
        raw_file = fetcher.save_data(all_records)
        logger.info(f"Raw data saved to: {raw_file}")
        
        logger.info("Generating statistics...")
        stats = fetcher.get_statistics(all_records)
        logger.info(f"Statistics: {json.dumps(stats, indent=2, default=str)}")
        
        stats_file = Config.RAW_DATA_FILE.replace('.json', '_stats.json')
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2, default=str)
        logger.info(f"Statistics saved to: {stats_file}")
        
        logger.info("Transforming to flat records...")
        flat_records = transform_to_flat_records(all_records)
        logger.info(f"Transformed {len(flat_records)} flat records")
        
        processed_file = Config.PROCESSED_DATA_FILE
        with open(processed_file, 'w') as f:
            json.dump([record.dict() for record in flat_records], f, indent=2, default=str)
        logger.info(f"Processed data saved to: {processed_file}")
        
        logger.info("=" * 80)
        logger.info("Starting Database Insertion")
        logger.info("=" * 80)
        
        mapper = FDADrugDBMapper()
        
        if not mapper.connect():
            logger.error("Failed to connect to database. Skipping database insertion.")
        else:
            try:
                initial_count = mapper.get_table_count()
                logger.info(f"Initial database count: {initial_count}")
                
                logger.info(f"Processing {len(all_records)} FDA records...")
                db_stats = mapper.process_fda_records(all_records)
                
                final_count = mapper.get_table_count()
                
                logger.info("=" * 80)
                logger.info("Database Insertion Statistics")
                logger.info("=" * 80)
                logger.info(f"FDA Records Processed: {db_stats['total_records']}")
                logger.info(f"Total Entries (Submissions×Products): {db_stats['total_entries']}")
                logger.info(f"Successfully Inserted: {db_stats['inserted']}")
                logger.info(f"Duplicates Skipped: {db_stats['duplicates']}")
                logger.info(f"Errors: {db_stats['errors']}")
                logger.info(f"Database Count Before: {initial_count}")
                logger.info(f"Database Count After: {final_count}")
                logger.info(f"Net Increase: {final_count - initial_count}")
                logger.info("=" * 80)
                
            finally:
                mapper.close()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=" * 80)
        logger.info("FDA Drug Data Fetcher - Completed Successfully")
        logger.info("=" * 80)
        logger.info(f"Total records fetched: {len(all_records)}")
        logger.info(f"Total flat records: {len(flat_records)}")
        logger.info(f"Unique sponsors: {stats['unique_sponsors']}")
        logger.info(f"Unique dosage forms: {stats['unique_dosage_forms']}")
        logger.info(f"Duration: {duration}")
        logger.info(f"Raw data file: {raw_file}")
        logger.info(f"Processed data file: {processed_file}")
        logger.info(f"Statistics file: {stats_file}")
        if trial_mode:
            logger.info(f"⚠️  TRIAL MODE: Only {Config.TRIAL_LIMIT} records processed")
            logger.info(f"To process all records, set TRIAL_LIMIT=0 in config.py")
        logger.info("=" * 80)
        
        return 0
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        logger.error("=" * 80)
        logger.error("FDA Drug Data Fetcher - Failed")
        logger.error("=" * 80)
        return 1


if __name__ == "__main__":
    sys.exit(main())

