import argparse
import json
import logging
import sys
from datetime import datetime
import importlib
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


CONFIG_FILE = Path(__file__).parent / 'config.json'

MODULES = {
    'usa_drug': {
        'name': 'USA FDA Drug',
        'path': 'usa_drug',
        'main_file': 'main',
        'description': 'Downloads and processes complete FDA drug data from bulk file (no 25k limit)'
    },
    'usa_drug_label': {
        'name': 'USA FDA Drug Labels',
        'path': 'usa_drug',
        'main_file': 'label_main',
        'description': 'Downloads and processes FDA drug label data'
    },
    'singapore_drug': {
        'name': 'Singapore HSA Drug',
        'path': 'singapore_drug',
        'main_file': 'main',
        'description': 'Fetches drug data from Singapore HSA (Coming Soon)'
    }
}


def load_config() -> dict:
    """
    Load configuration from config.json
    
    Returns:
        Configuration dictionary with module enabled status
    """
    try:
        if CONFIG_FILE.exists():
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)
                logger.info(f"Configuration loaded from {CONFIG_FILE}")
                return config
        else:
            logger.warning(f"Configuration file not found: {CONFIG_FILE}")
            logger.warning("Using default configuration (all modules enabled)")
            return {
                'modules': {key: {'enabled': True} for key in MODULES.keys()},
                'settings': {'stop_on_error': False, 'log_level': 'INFO'}
            }
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        logger.warning("Using default configuration (all modules enabled)")
        return {
            'modules': {key: {'enabled': True} for key in MODULES.keys()},
            'settings': {'stop_on_error': False, 'log_level': 'INFO'}
        }


def is_module_enabled(module_key: str, config: dict) -> bool:
    """
    Check if a module is enabled in configuration
    
    Args:
        module_key: Key of the module to check
        config: Configuration dictionary
        
    Returns:
        True if module is enabled, False otherwise
    """
    if 'modules' not in config:
        return True
    
    if module_key not in config['modules']:
        return True
    
    return config['modules'][module_key].get('enabled', True)


def run_module(module_key: str, config: dict = None) -> bool:
    """
    Runs a specific module
    
    Args:
        module_key: Key of the module to run
        config: Configuration dictionary (optional)
        
    Returns:
        True if successful, False otherwise
    """
    if module_key not in MODULES:
        logger.error(f"Unknown module: {module_key}")
        return False
    
    if config and not is_module_enabled(module_key, config):
        logger.info(f"⊘ {MODULES[module_key]['name']} is DISABLED in config - skipping")
        return True
    
    module_info = MODULES[module_key]
    module_path = Path(__file__).parent / module_info['path']
    
    if not module_path.exists():
        logger.warning(f"Module not found: {module_path}")
        logger.warning(f"Skipping {module_info['name']}")
        return False
    
    logger.info("=" * 80)
    logger.info(f"Running: {module_info['name']}")
    logger.info(f"Description: {module_info['description']}")
    logger.info("=" * 80)
    
    sys.path.insert(0, str(module_path))
    try:
        main_file = module_info.get('main_file', 'main')
        module_main = importlib.import_module(main_file)

        result = module_main.main()

        if result == 0:
            logger.info(f"✓ {module_info['name']} completed successfully")
            return True
        else:
            logger.error(f"✗ {module_info['name']} failed")
            return False
            
    except Exception as e:
        logger.error(f"Error running {module_info['name']}: {e}", exc_info=True)
        return False
    finally:
        sys.path.pop(0)


def run_all_modules(config: dict = None) -> bool:
    """
    Runs all available modules sequentially (only enabled ones if config provided)
    
    Args:
        config: Configuration dictionary (optional)
    
    Returns:
        True if all succeeded, False if any failed
    """
    logger.info("=" * 80)
    logger.info("PREDICATE AUTOMATE - Running Modules")
    logger.info("=" * 80)
    
    if config:
        logger.info("\nModule Status:")
        for module_key, module_info in MODULES.items():
            enabled = is_module_enabled(module_key, config)
            status = "✓ ENABLED" if enabled else "⊘ DISABLED"
            logger.info(f"  {module_info['name']}: {status}")
        logger.info("")
    
    start_time = datetime.now()
    results = {}
    skipped = []
    
    for module_key in MODULES.keys():
        if config and not is_module_enabled(module_key, config):
            skipped.append(module_key)
            continue
        success = run_module(module_key, config)
        results[module_key] = success
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info("=" * 80)
    logger.info("EXECUTION SUMMARY")
    logger.info("=" * 80)
    
    for module_key, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"{MODULES[module_key]['name']}: {status}")
    
    if skipped:
        logger.info("\nSkipped (Disabled in config):")
        for module_key in skipped:
            logger.info(f"  ⊘ {MODULES[module_key]['name']}")
    
    total = len(results)
    succeeded = sum(1 for s in results.values() if s)
    failed = total - succeeded
    
    logger.info("-" * 80)
    logger.info(f"Total Modules: {len(MODULES)}")
    logger.info(f"Enabled: {total}")
    logger.info(f"Skipped: {len(skipped)}")
    logger.info(f"Succeeded: {succeeded}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Duration: {duration}")
    logger.info("=" * 80)
    
    return failed == 0


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Predicate Automate - Regulatory Data Fetcher'
    )
    parser.add_argument(
        'module',
        nargs='?',
        choices=list(MODULES.keys()) + ['all'],
        default='all',
        help='Module to run (default: all)'
    )
    parser.add_argument(
        '--list',
        action='store_true',
        help='List available modules and their status'
    )
    parser.add_argument(
        '--ignore-config',
        action='store_true',
        help='Ignore config.json and run specified modules regardless of enabled status'
    )
    
    args = parser.parse_args()
    config = None if args.ignore_config else load_config()
    if args.list:
        print("\nAvailable Modules:")
        print("=" * 80)
        for key, info in MODULES.items():
            enabled = "ENABLED" if (not config or is_module_enabled(key, config)) else "DISABLED"
            print(f"\n{key}: [{enabled}]")
            print(f"  Name: {info['name']}")
            print(f"  Description: {info['description']}")
            if config and not is_module_enabled(key, config):
                print(f"  Note: Disabled in config.json - will be skipped")
        print("\n" + "=" * 80)
        print(f"\nConfiguration file: {CONFIG_FILE}")
        print("Use --ignore-config flag to run modules regardless of config settings")
        return 0
    
    try:
        if args.module == 'all':
            success = run_all_modules(config)
        else:
            success = run_module(args.module, config)
        
        return 0 if success else 1
        
    except KeyboardInterrupt:
        logger.warning("\nInterrupted by user")
        return 130
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

