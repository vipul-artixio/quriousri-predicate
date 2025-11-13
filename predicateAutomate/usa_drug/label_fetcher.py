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


class FDALabelFetcher:
    """Fetches drug label data from FDA"""
    
    def __init__(self):
        self.base_url = Config.FDA_LABEL_BASE_URL
        self.session = requests.Session()
        self.total_records = 0
        self.fetched_records = 0
        Config.ensure_output_dir()
        self.output_dir = os.path.join(Config.OUTPUT_DIR, 'fda_labels')
        os.makedirs(self.output_dir, exist_ok=True)

    def get_metadata(self) -> Optional[int]:
        """Get metadata about the dataset"""
        try:
            api_url = "https://api.fda.gov/drug/label.json?limit=1"
            response = requests.get(api_url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                total_records = data['meta']['results']['total']
                logger.info(f"Total records in dataset: {total_records:,}")
                return total_records
        except Exception as e:
            logger.error(f"Failed to get metadata: {e}")
        return None

    def calculate_required_files(self, total_records: int, records_per_file: int = 20000) -> int:
        """Calculate how many files we need"""
        import math
        num_files = math.ceil(total_records / records_per_file)
        logger.info(f"Estimated number of files: {num_files}")
        return num_files

    @retry(
        stop=stop_after_attempt(Config.MAX_RETRIES),
        wait=wait_exponential(multiplier=Config.RETRY_DELAY, min=1, max=10)
    )
    def download_file(self, file_url: str, local_path: str) -> bool:
        """Download a single file"""
        try:
            response = requests.get(file_url, stream=True, timeout=Config.REQUEST_TIMEOUT)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
            
            file_size = os.path.getsize(local_path) / 1024 / 1024
            logger.info(f"Downloaded ({file_size:.2f} MB)")
            return True
        except Exception as e:
            logger.error(f"Failed to download {file_url}: {e}")
            return False

    def extract_labels_from_zip(self, zip_path: str) -> List[Dict]:
        """Extract and parse labels from a ZIP file"""
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                json_files = [f for f in zip_ref.namelist() if f.endswith('.json')]
                
                if not json_files:
                    logger.warning(f"No JSON files found in {zip_path}")
                    return []
                
                json_filename = json_files[0]
                with zip_ref.open(json_filename) as json_file:
                    data = json.load(json_file)
                    records = data.get('results', [])
                    logger.info(f"Extracted {len(records)} records from {json_filename}")
                    return records
        except Exception as e:
            logger.error(f"Failed to extract labels from {zip_path}: {e}")
            return []

    def fetch_all_labels(self) -> List[Dict]:
        """Smart download with metadata check"""
        logger.info("=" * 80)
        logger.info("Starting FDA Label Download")
        logger.info("=" * 80)
        
        all_records = []
        
        # Get metadata to determine number of files
        total_records = self.get_metadata()
        
        if total_records:
            estimated_files = self.calculate_required_files(total_records)
        else:
            logger.warning("Could not get metadata, will download until 404")
            estimated_files = 100

        downloaded_count = 0
        
        for part_num in range(1, estimated_files + 10):
            # Try different file name patterns
            patterns = [
                f"drug-label-{part_num:04d}-of-{estimated_files:04d}.json.zip",
                f"drug-label-{part_num:04d}-of-{estimated_files+5:04d}.json.zip",
                f"drug-label-{part_num:04d}-of-{estimated_files+10:04d}.json.zip",
            ]
            
            downloaded = False
            for pattern in patterns:
                file_url = self.base_url + pattern
                local_path = os.path.join(self.output_dir, pattern)
                
                # Check if already exists
                if os.path.exists(local_path):
                    logger.info(f"[Part {part_num}] Already exists, extracting...")
                    records = self.extract_labels_from_zip(local_path)
                    all_records.extend(records)
                    downloaded_count += 1
                    downloaded = True
                    break
                
                # Try to download
                try:
                    response = requests.head(file_url, timeout=10)
                    if response.status_code == 200:
                        logger.info(f"[Part {part_num}] Downloading {pattern}...")
                        if self.download_file(file_url, local_path):
                            records = self.extract_labels_from_zip(local_path)
                            all_records.extend(records)
                            downloaded_count += 1
                            downloaded = True
                            
                            # Log progress
                            logger.info(f"Progress: Downloaded {downloaded_count} files | Total records: {len(all_records):,}")
                            break
                except:
                    continue
            
            # If no file found after estimated files, stop
            if not downloaded and part_num > estimated_files:
                logger.info(f"No more files found after part {part_num-1}")
                break
        
        logger.info("=" * 80)
        logger.info(f"Download Complete!")
        logger.info(f"Total Files: {downloaded_count}")
        logger.info(f"Total Records: {len(all_records):,}")
        logger.info("=" * 80)
        
        self.total_records = len(all_records)
        self.fetched_records = len(all_records)
        
        return all_records

    def save_data(self, records: List[Dict], filename: str = 'fda_labels_raw.json') -> str:
        """Save records to JSON file"""
        try:
            output_path = os.path.join(Config.OUTPUT_DIR, filename)
            logger.info(f"Saving {len(records)} records to: {output_path}")
            
            data = {
                'meta': {
                    'results': {
                        'total': len(records)
                    }
                },
                'results': records
            }
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, default=str)
            
            file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
            logger.info(f"Data saved successfully ({file_size_mb:.1f} MB)")
            
            return output_path
        except Exception as e:
            logger.error(f"Failed to save data: {e}")
            raise


if __name__ == "__main__":
    fetcher = FDALabelFetcher()
    records = fetcher.fetch_all_labels()
    if records:
        fetcher.save_data(records)