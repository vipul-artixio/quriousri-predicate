import requests
import json
import time
import logging
import zipfile
import os
from typing import Dict, List, Optional
from tenacity import retry, stop_after_attempt, wait_exponential
from config import Config

logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FDADrugFetcher:
    """Fetches drug data from FDA bulk download (no 25k limit)"""
    
    def __init__(self):
        self.download_url = Config.FDA_BULK_DOWNLOAD_URL
        self.session = requests.Session()
        self.total_records = 0
        self.fetched_records = 0
        
        # Ensure output directory exists
        Config.ensure_output_dir()
        
        # Paths for downloaded files
        self.zip_path = os.path.join(Config.OUTPUT_DIR, 'fda_drugs_bulk.zip')
        self.json_path = os.path.join(Config.OUTPUT_DIR, 'fda_drugs_bulk.json')
        
    @retry(
        stop=stop_after_attempt(Config.MAX_RETRIES),
        wait=wait_exponential(multiplier=Config.RETRY_DELAY, min=1, max=10)
    )
    def download_bulk_file(self) -> str:
        """
        Downloads the bulk ZIP file from FDA
        
        Returns:
            Path to downloaded ZIP file
        """
        try:
            logger.info(f"Downloading bulk file from: {self.download_url}")
            logger.info("This may take a few minutes for large files...")
            
            response = self.session.get(
                self.download_url,
                stream=True,
                timeout=Config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            with open(self.zip_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0 and downloaded % (10 * 1024 * 1024) == 0:
                            progress = (downloaded / total_size) * 100
                            logger.info(f"Download progress: {progress:.1f}% ({downloaded / (1024*1024):.1f} MB)")
            
            file_size_mb = os.path.getsize(self.zip_path) / (1024 * 1024)
            logger.info(f"Download complete: {file_size_mb:.1f} MB")
            logger.info(f"Saved to: {self.zip_path}")
            
            return self.zip_path
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Download failed: {e}")
            raise
    
    def extract_json_from_zip(self) -> str:
        """
        Extracts JSON file from the downloaded ZIP
        
        Returns:
            Path to extracted JSON file
        """
        try:
            logger.info(f"Extracting ZIP file: {self.zip_path}")
            
            with zipfile.ZipFile(self.zip_path, 'r') as zip_ref:
                # List all files in the ZIP
                file_list = zip_ref.namelist()
                logger.info(f"Files in ZIP: {file_list}")
                
                # Find the JSON file (should be drug-drugsfda-0001-of-0001.json)
                json_files = [f for f in file_list if f.endswith('.json')]
                
                if not json_files:
                    raise ValueError("No JSON file found in ZIP archive")
                
                json_filename = json_files[0]
                logger.info(f"Extracting: {json_filename}")
                
                # Extract to output directory
                zip_ref.extract(json_filename, Config.OUTPUT_DIR)
                
                # Rename to our standard name
                extracted_path = os.path.join(Config.OUTPUT_DIR, json_filename)
                os.rename(extracted_path, self.json_path)
                
                file_size_mb = os.path.getsize(self.json_path) / (1024 * 1024)
                logger.info(f"JSON extracted: {file_size_mb:.1f} MB")
                logger.info(f"Saved to: {self.json_path}")
                
                return self.json_path
                
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise
    
    def load_json_data(self) -> Dict:
        """
        Loads the JSON data from extracted file
        
        Returns:
            Parsed JSON data as dictionary
        """
        try:
            logger.info(f"Loading JSON data from: {self.json_path}")
            
            with open(self.json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            logger.info("JSON data loaded successfully")
            return data
            
        except Exception as e:
            logger.error(f"Failed to load JSON: {e}")
            raise
    
    def cleanup_temp_files(self, keep_json: bool = True):
        """
        Cleans up downloaded files
        
        Args:
            keep_json: If True, keeps the extracted JSON file
        """
        try:
            # Always remove ZIP file to save space
            if os.path.exists(self.zip_path):
                os.remove(self.zip_path)
                logger.info(f"Cleaned up ZIP file: {self.zip_path}")
            
            # Optionally remove JSON file
            if not keep_json and os.path.exists(self.json_path):
                os.remove(self.json_path)
                logger.info(f"Cleaned up JSON file: {self.json_path}")
                
        except Exception as e:
            logger.warning(f"Cleanup warning: {e}")
    
    def get_total_records(self) -> int:
        """
        Returns the total number of records
        This is called after data is loaded
        
        Returns:
            Total number of records
        """
        return self.total_records
    
    def fetch_all_data(self, save_intermediate: bool = True, max_skip: int = None) -> List[Dict]:
        """
        Downloads and processes bulk FDA drug data
        
        Note: max_skip parameter is kept for backwards compatibility but ignored
        since we're downloading the complete bulk file
        
        Args:
            save_intermediate: If True, saves data after processing
            max_skip: Ignored (kept for backwards compatibility)
            
        Returns:
            List of all drug records
        """
        try:
            # Step 1: Download the ZIP file
            logger.info("=" * 80)
            logger.info("STEP 1: Downloading bulk file")
            logger.info("=" * 80)
            self.download_bulk_file()
            
            # Step 2: Extract JSON from ZIP
            logger.info("=" * 80)
            logger.info("STEP 2: Extracting JSON from ZIP")
            logger.info("=" * 80)
            self.extract_json_from_zip()
            
            # Step 3: Load and parse JSON data
            logger.info("=" * 80)
            logger.info("STEP 3: Loading JSON data")
            logger.info("=" * 80)
            data = self.load_json_data()
            
            # Extract results from the FDA data structure
            all_records = data.get('results', [])
            self.total_records = len(all_records)
            self.fetched_records = self.total_records
            
            logger.info("=" * 80)
            logger.info(f"SUCCESS: Loaded {self.total_records} records from bulk file")
            logger.info("No 25,000 record limit with bulk download!")
            logger.info("=" * 80)
            
            # Step 4: Cleanup temporary files
            logger.info("Cleaning up temporary files...")
            self.cleanup_temp_files(keep_json=False)  # Remove JSON after loading
            
            return all_records
            
        except Exception as e:
            logger.error(f"Failed to fetch bulk data: {e}")
            raise
    
    def save_data(self, records: List[Dict]) -> str:
        """
        Saves records to the raw data file
        
        Args:
            records: List of drug records to save
            
        Returns:
            Path to saved file
        """
        try:
            logger.info(f"Saving {len(records)} records to: {Config.RAW_DATA_FILE}")
            
            # Create the data structure matching FDA API format
            data = {
                'meta': {
                    'results': {
                        'total': len(records)
                    }
                },
                'results': records
            }
            
            with open(Config.RAW_DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, default=str)
            
            file_size_mb = os.path.getsize(Config.RAW_DATA_FILE) / (1024 * 1024)
            logger.info(f"Data saved successfully ({file_size_mb:.1f} MB)")
            
            return Config.RAW_DATA_FILE
            
        except Exception as e:
            logger.error(f"Failed to save data: {e}")
            raise
    
    def get_statistics(self, records: List[Dict]) -> Dict:
        """
        Generates statistics about the fetched data
        
        Args:
            records: List of drug records
            
        Returns:
            Dictionary containing statistics
        """
        try:
            sponsors = set()
            dosage_forms = set()
            submission_types = set()
            application_types = set()
            
            for record in records:
                # Get sponsor name
                sponsor = record.get('sponsor_name', 'Unknown')
                if sponsor:
                    sponsors.add(sponsor)
                
                # Get submission info
                submissions = record.get('submissions', [])
                for submission in submissions:
                    submission_type = submission.get('submission_type', 'Unknown')
                    if submission_type:
                        submission_types.add(submission_type)
                
                # Get product info
                products = record.get('products', [])
                for product in products:
                    dosage_form = product.get('dosage_form', 'Unknown')
                    if dosage_form:
                        dosage_forms.add(dosage_form)
                
                # Get application info
                app_type = record.get('application_type', 'Unknown')
                if app_type:
                    application_types.add(app_type)
            
            stats = {
                'total_records': len(records),
                'unique_sponsors': len(sponsors),
                'unique_dosage_forms': len(dosage_forms),
                'unique_submission_types': len(submission_types),
                'unique_application_types': len(application_types),
                'sponsor_list': sorted(list(sponsors))[:20],  # First 20 sponsors
                'dosage_form_list': sorted(list(dosage_forms)),
                'submission_type_list': sorted(list(submission_types)),
                'application_type_list': sorted(list(application_types))
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to generate statistics: {e}")
            return {
                'total_records': len(records),
                'error': str(e)
            }
