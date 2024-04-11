"""
Main module for running the service
"""
import sys
import logging as LOG

from service.utils import get_config
from service.data_cleaner import MetaDataFromArchiveCleaner

if __name__ == '__main__':
    # 0. Load config file
    config = get_config()

    # 1. Create LOG to logging ;-)
    LOG.basicConfig(
        level = LOG.DEBUG,
        format = "%(asctime)s [%(levelname)s] %(message)s",
        handlers = [
            LOG.StreamHandler(sys.stdout)
        ]
    )

    # 2. Load meta data-ZIP archive, unpackt and clean it 
    LOG.info("Start cleaning meta data of test drivers...")
    LOG.info(f"Loaded config-file: {config}")

    mdfa_cleaner = MetaDataFromArchiveCleaner()
    df_cleaned = mdfa_cleaner.run_cleaning()
    
    LOG.debug(f"Info about Data:\n {df_cleaned.info()}")
    LOG.debug(f"Info about Data Frame:\n {df_cleaned.describe(include='all')}")
    LOG.info("End cleaning meta data of test drivers. Success!!! Bye!!!")