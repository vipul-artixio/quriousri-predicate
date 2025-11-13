import psycopg2
import psycopg2.extras
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional
from config import Config

logger = logging.getLogger(__name__)


class FDALabelMapper:
    """Maps FDA drug label data to source.usa_drug_label table"""
    
    def __init__(self, batch_size=1000):
        self.conn = None
        self.cursor = None
        self.batch_size = batch_size
        
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(
                host=Config.DB_HOST,
                port=Config.DB_PORT,
                database=Config.DB_NAME,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD
            )
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            logger.info("Database connection established")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")

    def transform_record(self, fda_record: Dict) -> Optional[Dict]:
        """
        Transform FDA label record to source.usa_drug_label format
        
        Args:
            fda_record: Raw FDA label record
            
        Returns:
            Transformed record dict or None if insufficient data
        """
        try:
            # Get spl_id and spl_set_id from the record (required fields)
            spl_id = fda_record.get('id')
            spl_set_id = fda_record.get('set_id')
            
            # Skip if spl_id or spl_set_id is missing
            if not spl_id or not spl_set_id:
                logger.debug(f"Skipping record: missing spl_id or spl_set_id")
                return None
            
            # Get openfda data (optional)
            openfda = fda_record.get('openfda', {})
            
            registration_number = None
            if openfda.get('application_number'):
                application_number = openfda['application_number']
                registration_number = application_number[0] if isinstance(application_number, list) and application_number else application_number
            
            generic_name_label = None
            if openfda.get('generic_name'):
                generic_names = openfda['generic_name']
                generic_name_label = generic_names[0] if isinstance(generic_names, list) and generic_names else generic_names
            
            manufacturer_label = None
            if openfda.get('manufacturer_name'):
                manufacturers = openfda['manufacturer_name']
                manufacturer_label = manufacturers[0] if isinstance(manufacturers, list) and manufacturers else manufacturers
            
            brand_name = None
            if openfda.get('brand_name'):
                brand_names = openfda['brand_name']
                brand_name = brand_names[0] if isinstance(brand_names, list) and brand_names else brand_names
            
            indications_and_usage = None
            if fda_record.get('indications_and_usage'):
                indications = fda_record['indications_and_usage']
                indications_and_usage = indications[0] if isinstance(indications, list) and indications else indications
            
            # Truncate fields to fit database constraints
            if spl_id and len(str(spl_id)) > 225:
                spl_id = str(spl_id)[:225]
            if spl_set_id and len(str(spl_set_id)) > 225:
                spl_set_id = str(spl_set_id)[:225]
            if registration_number and len(str(registration_number)) > 100:
                registration_number = str(registration_number)[:100]
            if generic_name_label and len(str(generic_name_label)) > 255:
                generic_name_label = str(generic_name_label)[:255]
            if manufacturer_label and len(str(manufacturer_label)) > 255:
                manufacturer_label = str(manufacturer_label)[:255]
            if brand_name and len(str(brand_name)) > 255:
                brand_name = str(brand_name)[:255]
            if indications_and_usage:
                indications_and_usage = str(indications_and_usage)
            
            if not registration_number:
                logger.debug(
                    "Skipping label record for SPL %s / %s due to missing registration_number",
                    spl_id,
                    spl_set_id,
                )
                return None

            record = {
                'spl_id': spl_id,
                'spl_set_id': spl_set_id,
                'registration_number': registration_number,
                'generic_name_label': generic_name_label,
                'manufacturer_label': manufacturer_label,
                'brand_name': brand_name,
                'indications_and_usage': indications_and_usage
            }
            
            return record
            
        except Exception as e:
            logger.error(f"Error transforming record: {e}")
            return None

    def batch_upsert_records(self, records: List[Dict]) -> Dict:
        """
        Batch upsert records using PostgreSQL's ON CONFLICT clause
        
        Args:
            records: List of transformed records
            
        Returns:
            Dict with inserted, updated, and error counts
        """
        if not records:
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
        # Use INSERT ... ON CONFLICT for true upsert
        upsert_query = """
            INSERT INTO source.usa_drug_label (
                spl_id,
                spl_set_id,
                registration_number,
                generic_name_label,
                manufacturer_label,
                brand_name,
                indications_and_usage
            ) VALUES %s
            ON CONFLICT (spl_id, spl_set_id, registration_number)
            DO UPDATE SET
                generic_name_label = EXCLUDED.generic_name_label,
                manufacturer_label = EXCLUDED.manufacturer_label,
                brand_name = EXCLUDED.brand_name,
                indications_and_usage = EXCLUDED.indications_and_usage,
                updated_at = CURRENT_TIMESTAMP
            WHERE (
                source.usa_drug_label.generic_name_label IS DISTINCT FROM EXCLUDED.generic_name_label OR
                source.usa_drug_label.manufacturer_label IS DISTINCT FROM EXCLUDED.manufacturer_label OR
                source.usa_drug_label.brand_name IS DISTINCT FROM EXCLUDED.brand_name OR
                source.usa_drug_label.indications_and_usage IS DISTINCT FROM EXCLUDED.indications_and_usage
            )
            RETURNING spl_id, spl_set_id, registration_number,
                     (xmax = 0) AS inserted
        """
        
        stats = {'inserted': 0, 'updated': 0, 'errors': 0}
        
        try:
            # Prepare values for batch insert
            values = [
                (
                    r['spl_id'],
                    r['spl_set_id'],
                    r['registration_number'],
                    r['generic_name_label'],
                    r['manufacturer_label'],
                    r['brand_name'],
                    r['indications_and_usage']
                )
                for r in records
            ]
            from psycopg2.extras import execute_values
            
            results = execute_values(
                self.cursor,
                upsert_query,
                values,
                template=None,
                page_size=1000,
                fetch=True
            )
            for result in results:
                inserted_flag = result[-1] if isinstance(result, tuple) else result.get('inserted')
                if inserted_flag:
                    stats['inserted'] += 1
                else:
                    stats['updated'] += 1
                    
        except Exception as e:
            logger.error(f"Error in batch upsert: {e}")
            stats['errors'] = len(records)
            raise
            
        return stats

    def process_fda_records(self, fda_records: List[Dict]) -> Dict:
        """
        Process FDA label records and insert into database using batch operations
        
        Args:
            fda_records: List of raw FDA label records
            
        Returns:
            Statistics dict
        """
        stats = {
            'total_records': len(fda_records),
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0
        }
        
        total_records = len(fda_records)
        progress_interval = max(1, total_records // 20)
        
        # Process in batches
        batch = []
        
        for idx, fda_record in enumerate(fda_records, 1):
            try:
                record = self.transform_record(fda_record)
                
                if not record:
                    stats['skipped'] += 1
                    continue
                
                batch.append(record)
                
                # Process batch when it reaches batch_size or at the end
                if len(batch) >= self.batch_size or idx == total_records:
                    try:
                        # Batch upsert
                        batch_stats = self.batch_upsert_records(batch)
                        stats['inserted'] += batch_stats['inserted']
                        stats['updated'] += batch_stats['updated']
                        stats['errors'] += batch_stats['errors']
                        self.conn.commit()
                        
                        logger.info(
                            f"Progress: {idx}/{total_records} records ({idx*100//total_records}%) | "
                            f"Batch: +{batch_stats['inserted']} inserted, "
                            f"+{batch_stats['updated']} updated | "
                            f"Total - Inserted: {stats['inserted']}, "
                            f"Updated: {stats['updated']}, "
                            f"Skipped: {stats['skipped']}, "
                            f"Errors: {stats['errors']}"
                        )
                        
                        batch = []
                        
                    except Exception as e:
                        logger.error(f"Error processing batch: {e}")
                        stats['errors'] += len(batch)
                        self.conn.rollback()
                        batch = []
                        
            except Exception as e:
                logger.error(f"Error transforming record: {e}")
                stats['skipped'] += 1
        
        return stats

    def get_table_count(self) -> int:
        """Get total count in source.usa_drug_label table"""
        try:
            self.cursor.execute(
                "SELECT COUNT(*) as count FROM source.usa_drug_label"
            )
            result = self.cursor.fetchone()
            return result['count']
        except Exception as e:
            logger.error(f"Error getting table count: {e}")
            return 0


def main():
    """Test the mapper"""
    mapper = FDALabelMapper(batch_size=1000)
    
    if mapper.connect():
        count = mapper.get_table_count()
        print(f"Current records in database: {count}")
        mapper.close()
    else:
        print("Failed to connect to database")


if __name__ == "__main__":
    main()