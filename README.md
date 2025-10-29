# Predicate Automate

A modular regulatory data automation platform that fetches, processes, and stores pharmaceutical drug registration data from regulatory authorities worldwide.

## Overview

**Predicate Automate** is designed to automatically collect and synchronize drug registration data from various regulatory authorities (FDA, HSA, etc.) into a unified PostgreSQL database. The platform features a modular architecture, allowing easy integration of new data sources and automated daily runs.

### Key Features

- 🔄 **Modular Architecture**: Easily add new regulatory data sources
- 🌍 **Multi-Country Support**: USA FDA (active), Singapore HSA (planned)
- 🐳 **Docker Ready**: Containerized for easy deployment and scheduling
- 📊 **Database Integration**: Direct PostgreSQL integration with duplicate prevention
- 🔧 **Configurable**: Enable/disable modules via `config.json`
- 📝 **Comprehensive Logging**: Detailed execution logs and statistics
- 🔁 **Retry Logic**: Automatic retry with exponential backoff
- 🧪 **Trial Mode**: Test with limited records before full runs

## Project Structure

```
quriousri-predicate/
├── docker-compose.yml          # Docker orchestration
├── Dockerfile                  # Container definition
├── README.md                   # This file
└── predicateAutomate/          # Main application directory
    ├── app.py                  # Module orchestrator/entry point
    ├── config.json             # Module enable/disable configuration
    ├── requirements.txt        # Python dependencies
    ├── setup.sh               # Local setup script
    ├── monitor_insertion.sh   # Database monitoring utility
    ├── usa_drug/              # USA FDA Drug Module
    │   ├── main.py            # Module entry point
    │   ├── fetcher.py         # API/bulk download logic
    │   ├── db_mapper.py       # Database mapping & insertion
    │   ├── models.py          # Pydantic data models
    │   ├── config.py          # Module configuration
    │   ├── count_total_entries.py  # Database count utility
    │   ├── README.md          # Module documentation
    │   └── output/            # Generated data files (created at runtime)
    └── singapore_drug/        # Singapore HSA Module (coming soon)
        ├── main.py            # Module entry point
        └── README.md          # Module documentation
```

## Available Modules

### 1. USA FDA Drug Module ✅ (Active)

**Status**: Production Ready  
**Source**: [FDA Drugs@FDA](https://open.fda.gov/apis/drug/drugsfda/)

Fetches complete FDA drug approval data (~29,000 records) via bulk download, processes it, and inserts it into the database.

**Features**:
- Downloads complete FDA bulk data file (no pagination limits)
- Processes drug applications, sponsors, products, and submissions
- Cross-joins submissions × products for comprehensive database entries
- Automatic duplicate prevention for safe daily runs
- Trial mode support (process limited records for testing)
- Comprehensive field mapping to `drug.drug_predicate_assessments` table

**See**: [predicateAutomate/usa_drug/README.md](predicateAutomate/usa_drug/README.md) for detailed documentation.

### 2. Singapore HSA Drug Module 🚧 (Coming Soon)

**Status**: Planned  
**Source**: Singapore Health Sciences Authority (HSA)

Future module for fetching drug registration data from Singapore's HSA.

**See**: [predicateAutomate/singapore_drug/README.md](predicateAutomate/singapore_drug/README.md) for planned features.

## Quick Start

### Prerequisites

- **Python**: 3.11+
- **PostgreSQL**: 9.6+ with database `quriousri_db`
- **Docker** (optional): For containerized deployment
- **Internet**: Required for downloading FDA data

### Option 1: Local Setup

1. **Clone and Navigate**
```bash
git clone <repository-url>
cd quriousri-predicate/predicateAutomate
```

2. **Run Setup Script**
```bash
chmod +x setup.sh
./setup.sh
```

3. **Configure Environment**

Create `.env` file with database credentials:
```env
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=quriousri_db
PG_USER=postgres
PG_PASSWORD=your_password
LOG_LEVEL=INFO
```

4. **Activate Virtual Environment**
```bash
source venv/bin/activate
```

5. **Run Modules**

```bash
# List available modules
python app.py --list

# Run all enabled modules
python app.py all

# Run specific module
python app.py usa_drug

# Ignore config.json and run anyway
python app.py usa_drug --ignore-config
```

### Option 2: Docker Setup

1. **Configure Environment**

Create `.env` file in project root:
```env
PG_HOST=host.docker.internal  # Or your database host
PG_PORT=5432
PG_DATABASE=quriousri_db
PG_USER=postgres
PG_PASSWORD=your_password
LOG_LEVEL=INFO
```

2. **Run with Docker Compose**

```bash
# Build and run
docker-compose up

# Run in background
docker-compose up -d

# View logs
docker-compose logs -f
```

3. **Run Specific Module**

```bash
docker-compose run quriousri-predicate python app.py usa_drug
```

## Configuration

### Module Configuration (`config.json`)

Control which modules run:

```json
{
  "modules": {
    "usa_drug": {
      "enabled": true,
      "description": "Downloads and processes USA FDA drug data"
    },
    "singapore_drug": {
      "enabled": false,
      "description": "Singapore HSA drug data (coming soon)"
    }
  },
  "settings": {
    "stop_on_error": false,
    "log_level": "INFO"
  }
}
```

### Module-Specific Configuration

Each module has its own `config.py`:

**USA Drug Module** (`predicateAutomate/usa_drug/config.py`):
```python
TRIAL_LIMIT = 0           # 0 = all records, N = first N records (for testing)
BATCH_SIZE = 1000         # Records per processing batch
MAX_RETRIES = 3           # API retry attempts
REQUEST_TIMEOUT = 300     # Request timeout in seconds
```

## Database Schema

### Target Table: `drug.drug_predicate_assessments`

The USA FDA module inserts data with the following field mapping:

| Database Column | Source | Description |
|----------------|--------|-------------|
| `registration_number` | `application_number` | FDA application number (e.g., ANDA077831) |
| `registration_holder` | `sponsor_name` | Sponsor/manufacturer name |
| `country_of_origin` | Fixed: "USA" | Country of regulatory authority |
| `product_name` | `product.brand_name` | Brand name of the drug product |
| `generic_name` | `openfda.generic_name[0]` | Generic/active ingredient name |
| `manufacturer` | `openfda.manufacturer_name[0]` | Manufacturer name |
| `dosage_form` | `product.dosage_form` | Dosage form (e.g., TABLET, CAPSULE) |
| `strength` | Combined from ingredients | Ingredient-Strength (e.g., "CILOSTAZOL-50MG") |
| `route_administration` | `product.route` | Route of administration (e.g., ORAL) |
| `marketing_status` | `product.marketing_status` | Marketing status (Prescription, OTC) |
| `approval_date` | `submission.submission_status_date` | Approval date (parsed to YYYY-MM-DD) |
| `application_type` | `submission.submission_type` | Application type (ORIG, SUPPL, etc.) |
| `submission_type` | `submission.submission_type` | Submission type |
| `submission_number` | `submission.submission_number` | Submission number |
| `json_data` | Full FDA record | Complete JSON for reference |

**Duplicate Prevention**: The system checks for existing records using:
- `registration_number` (application_number)
- `product_number`
- `submission_type`
- `submission_number`

## Usage Examples

### Run All Modules

```bash
# Local
python app.py all

# Docker
docker-compose up
```

### Run Specific Module

```bash
# Local
python app.py usa_drug

# Docker
docker-compose run quriousri-predicate python app.py usa_drug
```

### Trial Mode (Testing)

Edit `predicateAutomate/usa_drug/config.py`:
```python
TRIAL_LIMIT = 10  # Process only 10 records
```

Then run:
```bash
python app.py usa_drug
```

### Production Mode

Edit `predicateAutomate/usa_drug/config.py`:
```python
TRIAL_LIMIT = 0  # Process all records (~29,000)
```

Then run:
```bash
python app.py usa_drug
```

### List Available Modules

```bash
python app.py --list
```

Output:
```
Available Modules:
================================================================================

usa_drug: [ENABLED]
  Name: USA FDA Drug
  Description: Downloads and processes complete FDA drug data

singapore_drug: [DISABLED]
  Name: Singapore HSA Drug
  Description: Fetches drug data from Singapore HSA (Coming Soon)
```

### Monitor Database Insertion

```bash
cd predicateAutomate
./monitor_insertion.sh
```

## Scheduling (Cron)

Set up daily automated runs:

```bash
# Edit crontab
crontab -e

# Add this line to run daily at 2 AM
0 2 * * * cd /path/to/quriousri-predicate && docker-compose up >> /var/log/predicate-automate.log 2>&1
```

## Output Files

The USA Drug module generates the following files in `predicateAutomate/usa_drug/output/`:

1. **`fda_drugs_raw.json`** (~70MB)
   - Complete raw data downloaded from FDA
   - Includes all metadata and nested structures

2. **`fda_drugs_processed.json`** (~50MB)
   - Flattened records ready for processing
   - One entry per product

3. **`fda_drugs_raw_stats.json`** (~5KB)
   - Statistics about the fetched data
   - Unique counts, distributions, summary

## Performance Metrics

| Metric | Value |
|--------|-------|
| **Total FDA Records** | ~29,000 |
| **Database Entries Generated** | ~100,000+ (varies by submissions × products) |
| **Processing Time** | 5-10 minutes |
| **Memory Usage** | ~200 MB peak |
| **Disk Space** | ~125 MB (JSON files) |
| **Network Usage** | ~70 MB download |

## Logging

### Log Locations

- **Local**: `predicateAutomate/usa_drug/fda_drug_fetcher.log`
- **Docker**: `docker-compose logs -f`
- **Console**: Real-time stdout logging

### Log Levels

Configure in `.env`:
```env
LOG_LEVEL=INFO    # DEBUG, INFO, WARNING, ERROR, CRITICAL
```

### Example Output

```
================================================================================
FDA Drug Data Fetcher - Started
================================================================================
Starting bulk data download and processing...
Total records downloaded: 29000
Processing all 29000 records (production mode)
Saving raw data...
Raw data saved to: output/fda_drugs_raw.json
Generating statistics...
Transforming to flat records...
================================================================================
Starting Database Insertion
================================================================================
Processing 29000 FDA records...
================================================================================
Database Insertion Statistics
================================================================================
FDA Records Processed: 29000
Total Entries (Submissions×Products): 105234
Successfully Inserted: 105234
Duplicates Skipped: 0
Errors: 0
Database Count Before: 0
Database Count After: 105234
Net Increase: 105234
================================================================================
FDA Drug Data Fetcher - Completed Successfully
================================================================================
Duration: 0:06:23
```

## Development

### Adding a New Module

1. Create new directory: `predicateAutomate/new_module/`
2. Implement `main.py` with a `main()` function that returns `0` on success
3. Add module to `MODULES` dict in `app.py`
4. Add module configuration to `config.json`
5. Create module-specific `README.md`

Example module structure:
```python
# predicateAutomate/new_module/main.py
import logging

logger = logging.getLogger(__name__)

def main():
    """Module entry point"""
    try:
        logger.info("Starting new module...")
        # Your module logic here
        logger.info("Module completed successfully")
        return 0
    except Exception as e:
        logger.error(f"Module failed: {e}")
        return 1
```

### Running Tests

```bash
cd predicateAutomate/usa_drug
python -m pytest test_fetcher.py -v
```

### Code Style

- Follow PEP 8 guidelines
- Use type hints where applicable
- Add docstrings to all functions
- Use Pydantic for data validation

## Troubleshooting

### Database Connection Issues

```bash
# Test database connection
psql -h localhost -U postgres -d quriousri_db

# Check if database exists
psql -h localhost -U postgres -c "\l"
```

### Docker Network Issues

If using Docker and cannot connect to host database:
```yaml
# In docker-compose.yml, use:
environment:
  - PG_HOST=host.docker.internal  # For Mac/Windows
  # or
  - PG_HOST=172.17.0.1            # For Linux
```

### Memory Issues

Reduce batch size in `config.py`:
```python
BATCH_SIZE = 500  # Reduce from 1000
```

### Missing Dependencies

```bash
# Reinstall dependencies
pip install -r predicateAutomate/requirements.txt
```

## API Rate Limits & Limitations

### USA FDA
- ✅ Uses bulk download (no rate limits)
- ✅ Gets complete dataset (~29,000 records)
- ✅ No API key required
- ✅ Single download operation

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/new-module`
3. Commit changes: `git commit -am 'Add new module'`
4. Push to branch: `git push origin feature/new-module`
5. Submit a Pull Request

## Dependencies

See [`predicateAutomate/requirements.txt`](predicateAutomate/requirements.txt) for complete list.

**Key Dependencies**:
- `fastapi==0.115.0` - Web framework (future API)
- `pandas==2.2.2` - Data processing
- `psycopg2-binary==2.9.9` - PostgreSQL adapter
- `sqlalchemy==2.0.30` - Database ORM
- `pydantic==2.8.2` - Data validation
- `requests==2.31.0` - HTTP client
- `tenacity==8.2.3` - Retry logic

## Roadmap

- [x] USA FDA Drug Module
- [x] Docker containerization
- [x] Database integration with duplicate prevention
- [x] Module orchestration system
- [x] Configuration management
- [ ] Singapore HSA Drug Module
- [ ] Incremental updates (delta sync)
- [ ] Web API for querying data
- [ ] Email notifications
- [ ] Data quality validation
- [ ] Additional regulatory authorities (EMA, PMDA, etc.)

## License

[Add your license here]

## Support

For issues, questions, or contributions:
- **Issues**: Create a GitHub issue
- **Discussions**: Use GitHub Discussions
- **Email**: [Add contact email]

## Authors

[Add author information]

## Acknowledgments

- FDA OpenFDA API for providing open access to drug data
- PostgreSQL community
- Python community

---

**Version**: 1.0.0  
**Last Updated**: October 29, 2024  
**Status**: Production Ready ✅
