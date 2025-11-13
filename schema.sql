-- this sql is for the database schema and for reference only 
-- the application does not create the schema and tables, it is created by the database administrator

CREATE TABLE IF NOT EXISTS drug.drug_predicate_assessments (
    id SERIAL PRIMARY KEY,
    ingredient_name VARCHAR(1000),
    product_name VARCHAR(255),
    country_of_origin INTEGER,
    approval_date DATE,
    end_date DATE,
    application_type VARCHAR(100),
    classification VARCHAR(100),
    registration_number VARCHAR(100),
    registration_holder VARCHAR(255),
    manufacturer VARCHAR(255),
    importer VARCHAR(255),
    generic_name VARCHAR(255),
    reference_drug VARCHAR(255),
    dosage_form VARCHAR(255),
    strength VARCHAR(1000),
    route_administration VARCHAR(255),
    indication TEXT,
    therapy_area VARCHAR(255),
    other_trade_name VARCHAR(255),
    patent_information TEXT,
    distributor VARCHAR(255),
    marketing_status VARCHAR(100),
    submission_type VARCHAR(100),
    submission_number VARCHAR(100),
    submission_date DATE,
    json_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by INTEGER
);

-- Add unique constraint (using DO block for IF NOT EXISTS support)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'uq_drug_predicate_assessments_record'
    ) THEN
        ALTER TABLE drug.drug_predicate_assessments
        ADD CONSTRAINT uq_drug_predicate_assessments_record
            UNIQUE NULLS NOT DISTINCT (
                registration_number, 
                product_name, 
                submission_type, 
                submission_number, 
                submission_date,
                strength
            );
    END IF;
END $$;

ALTER TABLE drug.drug_predicate_assessments
ADD CONSTRAINT fk_country_of_origin
FOREIGN KEY (country_of_origin)
REFERENCES public.country (id)
ON UPDATE CASCADE
ON DELETE SET NULL;


-- Indexes for drug predicate assessments
CREATE INDEX IF NOT EXISTS idx_drug_predicate_product_name ON drug.drug_predicate_assessments(product_name);
CREATE INDEX IF NOT EXISTS idx_drug_predicate_country_of_origin ON drug.drug_predicate_assessments(country_of_origin);
CREATE INDEX IF NOT EXISTS idx_drug_predicate_approval_date ON drug.drug_predicate_assessments(approval_date);
CREATE INDEX IF NOT EXISTS idx_drug_predicate_classification ON drug.drug_predicate_assessments(classification);
CREATE INDEX IF NOT EXISTS idx_drug_predicate_reg_holder ON drug.drug_predicate_assessments(registration_holder);
CREATE INDEX IF NOT EXISTS idx_drug_predicate_manufacturer ON drug.drug_predicate_assessments(manufacturer);
CREATE INDEX IF NOT EXISTS idx_drug_predicate_generic_name ON drug.drug_predicate_assessments(generic_name);



CREATE TABLE IF NOT EXISTS source.usa_drug_data (
    id SERIAL PRIMARY KEY,
    spl_id TEXT[],
    spl_set_id TEXT[],
    ingredient_name VARCHAR(1000),
    product_name VARCHAR(255),
    country_of_origin INTEGER REFERENCES public.country (id) ON UPDATE CASCADE ON DELETE SET NULL,
    application_type VARCHAR(100),
    registration_number VARCHAR(100),
    registration_holder VARCHAR(255),
    manufacturer VARCHAR(255),
    generic_name VARCHAR(255),
    reference_drug VARCHAR(255),
    dosage_form VARCHAR(255),
    strength VARCHAR(1000),
    route_administration VARCHAR(255),
    marketing_status VARCHAR(100),
    submission_type VARCHAR(100),
    submission_number VARCHAR(100),
    submission_date DATE,
    json_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by INTEGER
);
CREATE INDEX IF NOT EXISTS idx_usa_drug_data_product_name ON source.usa_drug_data(product_name);
CREATE INDEX IF NOT EXISTS idx_usa_drug_data_country_of_origin ON source.usa_drug_data(country_of_origin);
CREATE INDEX IF NOT EXISTS idx_usa_drug_data_reg_holder ON source.usa_drug_data(registration_holder);
CREATE INDEX IF NOT EXISTS idx_usa_drug_data_manufacturer ON source.usa_drug_data(manufacturer);
CREATE INDEX IF NOT EXISTS idx_usa_drug_data_generic_name ON source.usa_drug_data(generic_name);



CREATE TABLE IF NOT EXISTS source.usa_drug_label (
    spl_id VARCHAR(225),
    spl_set_id VARCHAR(225),
    registration_number VARCHAR(100),
    generic_name_label VARCHAR(255),
    manufacturer_label VARCHAR(255),
    brand_name VARCHAR(255),
    indications_and_usage TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_usa_drug_label_spl_ids ON source.usa_drug_label(spl_id, spl_set_id);
CREATE INDEX IF NOT EXISTS idx_usa_drug_label_registration_number ON source.usa_drug_label(registration_number);
CREATE INDEX IF NOT EXISTS idx_usa_drug_label_generic_name_label ON source.usa_drug_label(generic_name_label);
CREATE INDEX IF NOT EXISTS idx_usa_drug_label_manufacturer_label ON source.usa_drug_label(manufacturer_label);
CREATE INDEX IF NOT EXISTS idx_usa_drug_label_brand_name ON source.usa_drug_label(brand_name);


ALTER TABLE source.usa_drug_label
ADD CONSTRAINT uk_usa_drug_label_spl_ids
UNIQUE (spl_id, spl_set_id, registration_number);

