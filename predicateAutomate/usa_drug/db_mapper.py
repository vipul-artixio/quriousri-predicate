import psycopg2
import psycopg2.extras
import json
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from config import Config

logger = logging.getLogger(__name__)


class FDADrugDBMapper:
    """Maps FDA drug data to source.usa_drug_data table"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        
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
    
    def parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """
        Parse FDA date format (YYYYMMDD) to PostgreSQL date format (YYYY-MM-DD)
        
        Args:
            date_str: Date string in YYYYMMDD format
            
        Returns:
            Date string in YYYY-MM-DD format or None
        """
        if not date_str or len(date_str) != 8:
            return None
        
        try:
            year = date_str[:4]
            month = date_str[4:6]
            day = date_str[6:8]
            return f"{year}-{month}-{day}"
        except:
            return None
    
    def format_submission_date(self, date_str: Optional[str]) -> Optional[str]:
        """
        Format FDA date (YYYYMMDD) to YYYY-MM-DD format for submission_date field
        
        Args:
            date_str: Date string in YYYYMMDD format
            
        Returns:
            Date string in YYYY-MM-DD format or None
        """
        if not date_str or len(date_str) != 8:
            return None
        
        try:
            year = date_str[:4]
            month = date_str[4:6]
            day = date_str[6:8]
            return f"{year}-{month}-{day}"
        except:
            return None
    
    def format_strength(self, active_ingredients: List[Dict]) -> str:
        """
        Format active ingredients into strength string
        
        Args:
            active_ingredients: List of active ingredient dicts
            
        Returns:
            Formatted strength string (e.g., "CILOSTAZOL-50MG")
        """
        if not active_ingredients:
            return ""
        
        parts = []
        for ingredient in active_ingredients:
            name = ingredient.get('name', '')
            strength = ingredient.get('strength', '')
            if name and strength:
                parts.append(f"{name}-{strength}")
            elif name:
                parts.append(name)
        
        return ", ".join(parts)
    
    def format_ingredient_names(self, active_ingredients: List[Dict]) -> str:
        """
        Extract ingredient names (without strength) separated by commas
        
        Args:
            active_ingredients: List of active ingredient dicts
            
        Returns:
            Comma-separated ingredient names (e.g., "HYDROCORTISONE BUTYRATE" or "ACETAMINOPHEN, BUTALBITAL, CAFFEINE")
        """
        if not active_ingredients:
            return ""
        
        names = []
        for ingredient in active_ingredients:
            name = ingredient.get('name', '')
            if name:
                names.append(name)
        
        return ", ".join(names)
    
    def check_duplicate(
        self,
        application_number: str,
        product_name: str,
        submission_type: str,
        submission_number: str,
        submission_date: Optional[str],
        strength: str = None,
        dosage_form: str = None,
    ) -> bool:
        """
        Check if record already exists in database based on actual table columns
        
        Args:
            application_number: Application number (registration_number)
            product_name: Product/brand name
            submission_type: Submission type
            submission_number: Submission number
            submission_date: Submission date in YYYY-MM-DD
            strength: Strength (optional for more specific matching)
            dosage_form: Dosage form (optional for more specific matching)
            
        Returns:
            True if duplicate exists, False otherwise
        """
        try:
            query = """
                SELECT COUNT(*) as count
                FROM source.usa_drug_data
                WHERE registration_number = %s
                AND product_name = %s
                AND submission_type = %s
                AND submission_number = %s
                AND submission_date IS NOT DISTINCT FROM %s
            """
            params = [
                application_number,
                product_name,
                submission_type,
                submission_number,
                submission_date
            ]
            
            if strength:
                query += " AND strength = %s"
                params.append(strength)
            if dosage_form:
                query += " AND dosage_form = %s"
                params.append(dosage_form)
            
            self.cursor.execute(query, params)
            
            result = self.cursor.fetchone()
            return result['count'] > 0
            
        except Exception as e:
            logger.error(f"Error checking duplicate: {e}")
            return False
    
    def transform_record(self, fda_record: Dict, product: Dict, submission: Dict) -> Dict:
        """
        Transform FDA record to drug_predicate_assessments format
        
        Args:
            fda_record: Raw FDA record
            product: Product from FDA record
            submission: Submission from FDA record
            
        Returns:
            Transformed record dict
        """
        application_number = fda_record.get('application_number', '')
        sponsor_name = fda_record.get('sponsor_name', '')
        
        openfda = fda_record.get('openfda', {})
        generic_name = None
        if openfda and openfda.get('generic_name'):
            generic_name = openfda['generic_name'][0] if isinstance(openfda['generic_name'], list) else openfda['generic_name']
        manufacturer = None
        if openfda.get('manufacturer_name'):
            manufacturers = openfda['manufacturer_name']
            manufacturer = manufacturers[0] if isinstance(manufacturers, list) and manufacturers else None

        spl_id = None
        if openfda.get('spl_id'):
            values = openfda['spl_id']
            spl_id = values if isinstance(values, list) else [values]

        spl_set_id = None
        if openfda.get('spl_set_id'):
            values = openfda['spl_set_id']
            spl_set_id = values if isinstance(values, list) else [values]

        active_ingredients = product.get('active_ingredients', [])

        strength = self.format_strength(active_ingredients)
        
        ingredient_names = self.format_ingredient_names(active_ingredients)
        approval_date = None
        reference_drug_value = product.get('reference_drug', 'No')
        
        json_data = {
            'application_number': application_number,
            'product_number': product.get('product_number', ''),
            'submission': submission,
            'product': product,
            'openfda': openfda if openfda else None,
            'sponsor_name': sponsor_name
        }
        
        submission_date_formatted = self.format_submission_date(submission.get('submission_status_date'))
        
        application_type_value = None
        if application_number:
            import re
            match = re.match(r'^([A-Z]+)', application_number)
            if match:
                application_type_value = match.group(1)
        
        record = {
            'country_of_origin': 1,
            'product_name': product.get('brand_name', ''),
            'spl_id': spl_id,
            'spl_set_id': spl_set_id,
            'ingredient_name': ingredient_names,  
            'registration_number': application_number,
            'registration_holder': sponsor_name,
            'manufacturer': manufacturer,
            'generic_name': generic_name,
            'reference_drug': reference_drug_value,
            'dosage_form': product.get('dosage_form', ''),
            'strength': strength,
            'route_administration': product.get('route', ''),
            'marketing_status': product.get('marketing_status', ''),
            'application_type': application_type_value,
            'submission_type': submission.get('submission_type', ''),
            'submission_number': submission.get('submission_number', ''),
            'submission_date': submission_date_formatted,
            'json_data': json.dumps(json_data),
            'created_by': None
        }
        
        return record
    
    def upsert_record(self, record: Dict) -> Optional[Tuple[int, bool]]:
        """Insert a record into source.usa_drug_data if it does not already exist.

        Returns (record_id, True) when inserted, (record_id, False) when an existing
        row is detected, or None on failure.
        """

        payload = record.copy()
        payload['spl_id'] = payload.get('spl_id') or []
        payload['spl_set_id'] = payload.get('spl_set_id') or []

        insert_query = """
            INSERT INTO source.usa_drug_data (
                country_of_origin,
                product_name,
                ingredient_name,
                registration_number,
                registration_holder,
                manufacturer,
                generic_name,
                reference_drug,
                dosage_form,
                strength,
                route_administration,
                marketing_status,
                application_type,
                submission_type,
                submission_number,
                submission_date,
                json_data,
                created_at,
                updated_at,
                spl_id,
                spl_set_id,
                created_by
            ) VALUES (
                %(country_of_origin)s,
                %(product_name)s,
                %(ingredient_name)s,
                %(registration_number)s,
                %(registration_holder)s,
                %(manufacturer)s,
                %(generic_name)s,
                %(reference_drug)s,
                %(dosage_form)s,
                %(strength)s,
                %(route_administration)s,
                %(marketing_status)s,
                %(application_type)s,
                %(submission_type)s,
                %(submission_number)s,
                %(submission_date)s,
                %(json_data)s::jsonb,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                %(spl_id)s,
                %(spl_set_id)s,
                %(created_by)s
            )
            RETURNING id
        """

        select_existing_query = """
            SELECT id
            FROM source.usa_drug_data
            WHERE registration_number = %(registration_number)s
              AND product_name = %(product_name)s
              AND submission_type = %(submission_type)s
              AND submission_number = %(submission_number)s
              AND submission_date IS NOT DISTINCT FROM %(submission_date)s
              AND strength IS NOT DISTINCT FROM %(strength)s
            ORDER BY id
            LIMIT 1
        """

        try:
            is_duplicate = self.check_duplicate(
                payload.get('registration_number'),
                payload.get('product_name'),
                payload.get('submission_type'),
                payload.get('submission_number'),
                payload.get('submission_date'),
                payload.get('strength'),
                payload.get('dosage_form'),
            )

            if is_duplicate:
                self.cursor.execute(select_existing_query, payload)
                existing_row = self.cursor.fetchone()
                if existing_row and existing_row.get('id') is not None:
                    return existing_row['id'], False
                return None

            self.cursor.execute(insert_query, payload)
            inserted_row = self.cursor.fetchone()
            if inserted_row and inserted_row.get('id') is not None:
                return inserted_row['id'], True

            return None
        except Exception as exc:
            logger.error(
                "Failed to upsert usa_drug_data record for application %s and product %s: %s",
                payload.get('registration_number'),
                payload.get('product_name'),
                exc,
            )
            raise
    
    def process_fda_records(self, fda_records: List[Dict]) -> Dict:
        """
        Process FDA records and insert into database
        Each submission is linked with each product (cross join)
        
        Args:
            fda_records: List of raw FDA records
            
        Returns:
            Statistics dict
        """
        stats = {
            'total_records': len(fda_records),
            'total_entries': 0,
            'inserted': 0,
            'duplicates': 0,
            'errors': 0
        }
        
        total_records = len(fda_records)
        progress_interval = max(1, total_records // 20)
        
        for idx, fda_record in enumerate(fda_records, 1):
            if idx % progress_interval == 0 or idx == total_records:
                logger.info(
                    f"Progress: {idx}/{total_records} records ({idx*100//total_records}%) | "
                    f"Entries: {stats['total_entries']} | "
                    f"Inserted: {stats['inserted']} | "
                    f"Duplicates: {stats['duplicates']} | "
                    f"Errors: {stats['errors']}"
                )
            submissions = fda_record.get('submissions', [])
            products = fda_record.get('products', [])
            for submission in submissions:
                for product in products:
                    stats['total_entries'] += 1
                    application_number = fda_record.get('application_number', '')
                    product_name = product.get('brand_name', '')
                    submission_type = submission.get('submission_type', '')
                    submission_number = submission.get('submission_number', '')
                    
                    active_ingredients = product.get('active_ingredients', [])
                    strength = self.format_strength(active_ingredients)
                    dosage_form = product.get('dosage_form', '')
                    
                    try:
                        record = self.transform_record(fda_record, product, submission)
                        upsert_result = self.upsert_record(record)

                        if not upsert_result:
                            stats['errors'] += 1
                            logger.error(
                                "Failed to upsert record for %s - %s",
                                record.get('registration_number'),
                                record.get('product_name'),
                            )
                            self.conn.rollback()
                            continue

                        data_id, is_new = upsert_result

                        if is_new:
                            self.conn.commit()
                            stats['inserted'] += 1
                            logger.debug(
                                "Inserted: %s - %s",
                                record.get('product_name'),
                                record.get('registration_number'),
                            )
                        else:
                            stats['duplicates'] += 1
                            logger.debug(
                                "Duplicate skipped: %s - %s",
                                record.get('product_name'),
                                record.get('registration_number'),
                            )

                    except Exception as e:
                        logger.error(f"Error processing record: {e}")
                        stats['errors'] += 1
                        self.conn.rollback()
        
        return stats
    
    def get_table_count(self) -> int:
        """Get total count in drug_predicate_assessments table"""
        try:
            self.cursor.execute(
                "SELECT COUNT(*) as count FROM source.usa_drug_data"
            )
            result = self.cursor.fetchone()
            return result['count']
        except Exception as e:
            logger.error(f"Error getting table count: {e}")
            return 0


def main():
    """Test the mapper"""
    mapper = FDADrugDBMapper()
    
    if mapper.connect():
        count = mapper.get_table_count()
        print(f"Current records in database: {count}")
        mapper.close()
    else:
        print("Failed to connect to database")


if __name__ == "__main__":
    main()

