from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import datetime


class ActiveIngredient(BaseModel):
    """Model for active ingredient in a drug product"""
    name: str
    strength: str


class Submission(BaseModel):
    """Model for drug submission"""
    submission_type: str
    submission_number: str
    submission_status: str
    submission_status_date: Optional[str] = None
    review_priority: Optional[str] = None
    submission_class_code: Optional[str] = None
    submission_class_code_description: Optional[str] = None


class Product(BaseModel):
    """Model for drug product"""
    product_number: str
    reference_drug: str
    brand_name: str
    active_ingredients: List[ActiveIngredient]
    reference_standard: str
    dosage_form: str
    route: str
    marketing_status: str
    te_code: Optional[str] = None


class FDADrugRecord(BaseModel):
    """Main model for FDA Drug record"""
    application_number: str
    sponsor_name: str
    submissions: List[Submission]
    products: List[Product]
    openfda: Optional[dict] = None


class FDADrugData(BaseModel):
    """Container model for fetched FDA data"""
    total_records: int
    fetch_timestamp: str
    source: str = "FDA Drugs@FDA API"
    data: List[FDADrugRecord]


class ProcessedDrugData(BaseModel):
    """Processed and flattened drug data for database insertion"""
    application_number: str
    sponsor_name: str
    
    product_number: str
    brand_name: str
    dosage_form: str
    route: Optional[str] = None
    marketing_status: str
    reference_drug: bool
    reference_standard: bool
    te_code: Optional[str] = None
    
    active_ingredients: List[dict]
    
    latest_submission_type: Optional[str] = None
    latest_submission_status: Optional[str] = None
    latest_submission_date: Optional[str] = None
    
    data_source: str = "FDA"
    fetched_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


def transform_to_flat_records(fda_records: List[dict]) -> List[ProcessedDrugData]:
    """
    Transforms nested FDA records into flat records for database insertion
    
    Args:
        fda_records: List of raw FDA drug records
        
    Returns:
        List of flattened ProcessedDrugData records
    """
    flat_records = []
    
    for record in fda_records:
        application_number = record.get('application_number', '')
        sponsor_name = record.get('sponsor_name', '')
        
        submissions = record.get('submissions', [])
        latest_submission = submissions[0] if submissions else {}
        
        products = record.get('products', [])
        for product in products:
            try:
                flat_record = ProcessedDrugData(
                    application_number=application_number,
                    sponsor_name=sponsor_name,
                    product_number=product.get('product_number', ''),
                    brand_name=product.get('brand_name', ''),
                    dosage_form=product.get('dosage_form', ''),
                    route=product.get('route') or None,
                    marketing_status=product.get('marketing_status', ''),
                    reference_drug=(product.get('reference_drug', 'No') == 'Yes'),
                    reference_standard=(product.get('reference_standard', 'No') == 'Yes'),
                    te_code=product.get('te_code'),
                    active_ingredients=product.get('active_ingredients', []),
                    latest_submission_type=latest_submission.get('submission_type'),
                    latest_submission_status=latest_submission.get('submission_status'),
                    latest_submission_date=latest_submission.get('submission_status_date')
                )
                flat_records.append(flat_record)
            except Exception as e:
                print(f"Error processing product {product.get('product_number')}: {e}")
                continue
    
    return flat_records

