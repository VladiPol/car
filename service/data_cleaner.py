from itertools import dropwhile
import zipfile
import pandas as pd
import json
import io
import os
import hashlib

import logging as LOG

from service import utils

config = utils.get_config()

class MetaDataFromArchiveCleaner():

    def __init__(self, path_to_archive: str = None) -> None:
        self.path_to_archive = path_to_archive


    def run_cleaning(self) -> pd.DataFrame:
        if self.path_to_archive is None:
            LOG.warning("No path to meta data archive given, try to load from config file")
            LOG.info(f"Using path to meta data archive from config: {config.FULL_PATH_TO_META_DATA_ARCHIVE}")
            self.path_to_archive = config.FULL_PATH_TO_META_DATA_ARCHIVE

        if not os.path.exists(self.path_to_archive):
            LOG.error(f"Meta data archive '{self.path_to_archive}' not found.")
            raise FileNotFoundError(f"Meta data archive '{self.path_to_archive}' not found.")

        df = self._unpack_archive()
        df_dropna = self._dropna(df)
        df_drop_negative_driven_km = self._drop_total_driven_km(df_dropna)
        df_drop_duplicates = self._drop_duplicates(df_drop_negative_driven_km)
        return df_drop_duplicates

    def _unpack_archive(self) -> pd.DataFrame:
        
        # Create an empty list to store dataframes
        # Open the zip file in read mode
        # Iterate over each file in the zip archive
        # Assumed: in the zip archive are only '.json' files
        # Open the file inside the zip archive as a stream
        # Load JSON data directly from the stream into a pandas dataframe
        # Calculate row-wise MD5 hash using all column values and add as a new column 'pk_hash' to find a duplicates
        # Append the dataframe to the list of dataframes
        # Concatenate all dataframes into a single dataframe

        LOG.debug("\n")
        LOG.debug("STEP: start of UNZIPPING OF DELIVERY")

        dfs = []
        count_files = 0
        error_files = 0

        # set default values
        pk_column=config.PK_COLUMN if config.PK_COLUMN is not None else 'pk_hash'
        column_file_name=config.COLUMN_FILE_NAME if config.COLUMN_FILE_NAME is not None else 'file_name'

        with zipfile.ZipFile(self.path_to_archive, 'r') as zip_ref:

            for file_name in zip_ref.namelist():
                count_files += 1
                with zip_ref.open(file_name) as file:
                    with io.TextIOWrapper(file, encoding='utf-8') as text_file:
                        try:
                            df = pd.DataFrame([json.loads(line) for line in text_file])
                            df[pk_column] = df.apply(lambda row: hashlib.sha256(','.join(map(str, row)).encode('utf-8')).hexdigest(), axis=1)
                            df[column_file_name] = file_name
                            dfs.append(df)
                        except Exception as error:
                            error_files += 1
                            LOG.warning(f"Error reading file: {file_name}, Error: {error}")


        combined_df = pd.concat(dfs, ignore_index=True)
        LOG.debug(f"Count of files total: {count_files}, files with errors total: {error_files}")
        LOG.debug("STEP: end of UNZIPPING OF DELIVERY")
        LOG.debug("\n")
        LOG.debug(f"Head of Data Frame:\n {combined_df.head()}")
        LOG.debug(f"Info about Data Frame:\n {combined_df.describe(include='all')}")
        return combined_df

    def _dropna(self, df: pd.DataFrame) -> pd.DataFrame:
        # Remove NaN from colums given as parameter or in the config file
        LOG.debug("\n")
        LOG.debug("STEP: start of DROP NA")

        col_list = config.COLUMNS_TO_DROPNA
        if col_list is None:
            LOG.warning(f"No column list to drop NaN found in {config.COLUMNS_TO_DROPNA}, nothing to do")
            LOG.debug("STEP: end of DROP NA")
            return df        

        import ast

        columns_to_check = ast.literal_eval(f"[{col_list}]")
        nan_count = df[columns_to_check].isna().sum()
        LOG.debug(f"Count NaN in '{columns_to_check}': {nan_count}")

        df_dropna = df.dropna(subset=columns_to_check)
        LOG.debug("STEP: end of DROP NA")
        return df_dropna

    def _drop_total_driven_km(self, df: pd.DataFrame) -> pd.DataFrame:
        # Convert 'total_driven_km' in int and remove negative values
        LOG.debug("STEP: start of DROP NEGATIVE TOTAL DRIVEN KM")
        
        df['total_driven_km'] = pd.to_numeric(df['total_driven_km'], errors='coerce')

        # Filter rows where 'total_driven_km' is less than 0 and log it
        negative_values_df = (df['total_driven_km'] < 0).sum()
        # Print out the values less than 0
        if negative_values_df > 0:
            LOG.debug(f"Total count values less than 0 in 'total_driven_km' column: {negative_values_df}")

        df_drop_negative_driven_km = df[df['total_driven_km'] >= 0]
        LOG.debug("STEP: end of DROP NEGATIVE TOTAL DRIVEN KM")
        return df_drop_negative_driven_km

    def _drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        # Identify rows with duplicate pk_hash values
        LOG.debug("STEP: start of DROP DUPLICATES")

        pk_column = config.PK_COLUMN
        if pk_column is None:
            LOG.warning("No PRIMAY KEY column found to drop duplicates in 'config.PK_COLUMN', nothing to do")
            LOG.debug("STEP: end of of DROP DUPLICATES")
            return df

        df_drop_duplicates = df.drop_duplicates(subset=pk_column, keep='first')
        LOG.debug("STEP: end of of DROP DUPLICATES")
        return df_drop_duplicates