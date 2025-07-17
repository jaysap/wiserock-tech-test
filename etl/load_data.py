################################################################################
## Analytics Engineer Technical Assessment (Part 1: Intermediate Python) #######
################################################################################

import os
import glob
import logging
import re
import csv
from io import StringIO
from typing import Optional, List, Dict, Any, Iterable, Tuple

from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from pandas.io.sql import SQLTable
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.types import Integer, Float, TEXT, DATE, DateTime, Boolean, BigInteger

from dotenv import load_dotenv
# Import configurations from the dedicated schema file
from etl_config import API_ENDPOINTS, SCHEMA_DEFINITIONS, CSV_LOAD_DTYPE_DICT, USE_MULTITHREADING, MAX_WORKERS


# Load environment variables from .env
load_dotenv()


def psql_insert_copy(table: SQLTable, conn: Engine, keys: List[str], data_iter: Iterable[Tuple[Any, ...]],) -> None:
    """
    High-performance COPY method for pandas to_sql.
    Args:
        table (SQLTable): The table object.
        conn (Engine): The database connection.
        keys (list[str]): Column names.
        data_iter (Iterable[tuple[Any, ...]]): Data rows.
    Returns:
        int: Number of rows inserted.
    """
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join(['"{}"'.format(k) for k in keys])
        if table.schema:
            table_name = f'"{table.schema}"."{table.name}"'
        else:
            table_name = f'"{table.name}"'

        # The COPY command is UNIQUE unique to PostgreSQL and is significantly
        # faster than regular chunked INSERT statements.
        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"
        cur.copy_expert(sql=sql, file=s_buf)


def _add_duplicate_flag(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a boolean column 'duplicate_flag' to the DataFrame.

    This function identifies rows that are complete duplicates across all columns.
    A row is marked as a duplicate if another identical row exists in the DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to process.

    Returns
    -------
    pd.DataFrame
        The DataFrame with the 'duplicate_flag' column added.
    """
    # The `keep=False` argument marks all occurrences of duplicates as True.
    df['duplicate_flag'] = df.duplicated(keep=False)
    return df


def _preprocess_dataframe(df: pd.DataFrame, schema_definition: Dict[str, Any]) -> pd.DataFrame:
    """
    Pre-processes a DataFrame to align with database schema definitions.

    This function serves two main purposes:
    1.  It forces columns defined as TEXT, DATE, or DateTime in the schema
        to be treated as string types within the DataFrame. This is a crucial
        data integrity step to prevent issues like Pandas dropping leading zeros API numbers.
    2.  It cleans up missing values (NaN, NaT) by converting them to None,
        which pandas correctly translates to SQL NULL.

    Parameters
    ----------
    df : pd.DataFrame
        The raw DataFrame to be processed.
    schema_definition : Dict[str, Any]
        The schema definition for the target table.

    Returns
    -------
    pd.DataFrame
        The processed and cleaned DataFrame.
    """
    for col, col_type in schema_definition.items():
        # Check if the column exists in the DataFrame to avoid KeyErrors
        if col in df.columns and isinstance(col_type, (TEXT, DATE, DateTime)):
            # Coerce to string and replace pandas' missing value representations with None
            df[col] = (df[col]
                       .astype(str)
                       .replace({"nan": None, "NaT": None, "None": None}))
    return df


class Config:
    """
    A centralized configuration class to hold all necessary parameters for the ETL process.
    """
    
    # --- PostgreSQL Database ---
    DB_USER: Optional[str] = os.environ.get("DB_USER")
    DB_PASSWORD: Optional[str] = os.environ.get("DB_PASSWORD")
    DB_HOST: Optional[str] = os.environ.get("DB_HOST")
    DB_PORT: str = os.environ.get("DB_PORT", "5432")
    DB_NAME: Optional[str] = os.environ.get("DB_NAME")
    DB_SCHEMA: str = os.environ.get("DB_SCHEMA", "stg")

    # --- CSV Data Source ---
    CSV_DIRECTORY: Optional[str] = os.environ.get("CSV_DIRECTORY")

    # --- API ---
    API_BASE_URL: Optional[str] = os.environ.get("API_BASE_URL")
    API_KEY: Optional[str] = os.environ.get("API_KEY")
    API_EMAIL: Optional[str] = os.environ.get("API_EMAIL")
    API_PASSWORD: Optional[str] = os.environ.get("API_PASSWORD")


# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,  # change to DEBUG for more verbose output
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class PostgresLoader:
    """
    Handles the connection to the PostgreSQL database and loading data into it.
    """

    def __init__(self, config: Config):
        """
        Initializes the database loader with connection settings from the Config object.
        """
        self.config: Config = config
        self.engine: Optional[Engine] = self._create_engine()
        if self.engine:
            self._create_schema_if_not_exists()

    def _create_engine(self) -> Optional[Engine]:
        """
        Creates and tests a SQLAlchemy engine for connecting to the PostgreSQL database.

        Returns
        -------
        Optional[Engine]
            The created engine object, or None if configuration is missing.
        """
        if not all(
            [
                self.config.DB_USER,
                self.config.DB_PASSWORD,
                self.config.DB_HOST,
                self.config.DB_NAME,
            ]
        ):
            logging.error("Database credentials are not fully configured. Please check your .env file.")
            return None
        try:
            connection_uri = (
                f"postgresql+psycopg2://{self.config.DB_USER}:{self.config.DB_PASSWORD}@"
                f"{self.config.DB_HOST}:{self.config.DB_PORT}/{self.config.DB_NAME}"
            )
            engine = create_engine(connection_uri)
            engine.connect()  # Test the connection
            logging.info("Successfully connected to the PostgreSQL database.")
            return engine
        except Exception as e:
            logging.error(f"Failed to create database engine: {e}")
            raise

    def _create_schema_if_not_exists(self):
        """
        Creates the specified schema in the database if it does not already exist.
        This helps in organizing raw data separately from transformed data.
        """
        if not self.engine:
            logging.error("Cannot create schema because database engine is not available.")
            return
        try:
            with self.engine.connect() as connection:
                connection.execute(
                    text(f"CREATE SCHEMA IF NOT EXISTS {self.config.DB_SCHEMA}")
                )
                connection.commit()
                logging.info(f"Schema '{self.config.DB_SCHEMA}' is ready.")
        except Exception as e:
            logging.error(f"Failed to create schema '{self.config.DB_SCHEMA}': {e}")
            raise

    def load_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Loads a pandas DataFrame into a specified table within the configured schema.
        If the table already exists, it will be replaced.

        Args:
            df (pd.DataFrame): The DataFrame to load.
            table_name (str): The name of the target table.
        """
        if df.empty:
            logging.warning(f"DataFrame for table '{table_name}' is empty. Skipping load.")
            return

        if self.engine is None:
            logging.error("Database engine is not initialized. Cannot load DataFrame.")
            raise ValueError("Database engine is not initialized.")

        dtype_mapping = SCHEMA_DEFINITIONS.get(table_name, {})
        df = _preprocess_dataframe(df, dtype_mapping)

        try:
            logging.info(f"'{self.config.DB_SCHEMA}.{table_name}' - Loading {len(df)} rows into table")

            df.to_sql(
                name=table_name,
                con=self.engine,
                schema=self.config.DB_SCHEMA,
                if_exists="replace",
                index=False,
                dtype=dtype_mapping,
                method=psql_insert_copy,  
            )
            
            logging.info(f"'{self.config.DB_SCHEMA}.{table_name}' - Finished loading data into table")

        except Exception as e:
            logging.error(f"Failed to load DataFrame into table '{table_name}': {e}")
            raise


class CsvExtractor:
    """Extracts data from all CSV files in a given directory."""

    def __init__(self, config: Config):
        self.config: Config = config

    def extract_and_load(self, db_loader: PostgresLoader) -> None:
        """
        Finds all CSV files, reads them with proactive type definitions, and loads them.
        """
        if not self.config.CSV_DIRECTORY:
            logging.warning("CSV_DIRECTORY is not set in the .env file. Skipping CSV extraction.")
            return

        logging.info(f"Starting CSV extraction from directory: {self.config.CSV_DIRECTORY}")
        csv_files = glob.glob(os.path.join(self.config.CSV_DIRECTORY, "*.csv"))

        if not csv_files:
            logging.warning(f"No CSV files found in the specified directory: {self.config.CSV_DIRECTORY}")
            return

        for file_path in csv_files:
            file_name = os.path.basename(file_path)
            table_name = os.path.splitext(file_name)[0]  # Get table name by removing the .csv extension            
            try:
                csv_load_dtypes = CSV_LOAD_DTYPE_DICT.get(table_name, {})
                logging.info(f"'{self.config.DB_SCHEMA}.{table_name}' - Reading CSV file with predefined types for: {list(csv_load_dtypes.keys())}")
                df = pd.read_csv(file_path, dtype=csv_load_dtypes)
                df = df.drop_duplicates().reset_index(drop=True)
                db_loader.load_dataframe(df, table_name)
                
            except Exception as e:
                logging.error(f"Failed to process CSV file {file_name}: {e}")
                continue  # Continue to the next file


class ApiExtractor:
    """Extracts data from API endpoints, handling authentication and parallel fetching."""

    def __init__(self, config: Config):
        self.config: Config = config
        self.session: requests.Session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Creates a requests.Session object with authentication headers."""
        session = requests.Session()
        access_token = self._get_auth_token()
        session_update_dict = {
            "apikey": self.config.API_KEY,
            "Authorization": f"Bearer {access_token}",
        }
        if access_token:
            session.headers.update(session_update_dict)
        return session

    def _get_auth_token(self) -> Optional[str]:
        """Authenticates with the API to retrieve a bearer token."""
        if not all(
            [
                self.config.API_BASE_URL,
                self.config.API_KEY,
                self.config.API_EMAIL,
                self.config.API_PASSWORD,
            ]
        ):
            logging.error("API credentials are not fully configured. Please check your .env file.")
            return None

        logging.info("Authenticating with the API to get access token...")
        auth_url = f"{self.config.API_BASE_URL}/auth/v1/token?grant_type=password"
        # Use a temporary requests call for authentication
        headers = {"apikey": self.config.API_KEY, "Content-Type": "application/json"}
        payload = {"email": self.config.API_EMAIL, "password": self.config.API_PASSWORD}

        try:
            response = requests.post(auth_url, headers=headers, json=payload)
            response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
            token = response.json().get("access_token")
            if not token:
                raise ValueError("Access token not found in API response.")
            logging.info("Successfully obtained API access token.")
            return token
        except requests.exceptions.RequestException as e:
            logging.error(f"API authentication failed: {e}")
            raise

    def _fetch_paginated_data(self, endpoint: str) -> List[Dict[str, Any]]:
        """Fetches all data from a single paginated endpoint with progress logging."""
        all_records: List[Dict[str, Any]] = []
        page_size = 999
        offset = 0
        total_records = -1
        next_log_percentage = 10.0  # Log progress at 10% intervals

        while True:
            # Set headers for the paginated request
            range_header = f"{offset}-{offset + page_size}"
            # Use a copy of the session headers and add the specific range for this request
            paginated_headers = dict(self.session.headers)
            paginated_headers["Range"] = range_header
            paginated_headers["Prefer"] = "count=exact"
            url = f"{self.config.API_BASE_URL}/rest/v1/{endpoint}"

            try:
                # Use the session object to make the request, which reuses connections
                response = self.session.get(url, headers=paginated_headers)
                response.raise_for_status()

                # Get total records count from the first response
                if total_records == -1:
                    content_range = response.headers.get("Content-Range")
                    # Parse 'Content-Range' header (e.g., "0-999/15000") using regex to determine total record count
                    # providing meaningful progress updates and knowing when to stop paginating.
                    if content_range and (match := re.search(r"\/(\d+)", content_range)):
                        total_records = int(match.group(1))
                        logging.info(f"Endpoint '{endpoint}' has a total of {total_records} records. Fetching data...")
                    else:
                        # If Content-Range is not present, assume single page
                        records = response.json()
                        total_records = len(records)
                        logging.info(f"Endpoint '{endpoint}' has a total of {total_records} records (single page).")

                records = response.json()
                if not records:
                    break  # No more data

                all_records.extend(records)
                # The pagination offset is incremented by the actual number of records received
                offset += len(records)

                # Conditional logging based on progress percentage
                if total_records > 0:
                    current_percentage = (offset / total_records) * 100
                    if current_percentage >= next_log_percentage:
                        logging.info(f"...fetched {offset} of {total_records} records ({current_percentage:.0f}%) from '{endpoint}'")
                        next_log_percentage += 10.0  # Set next milestone

                if offset >= total_records:
                    break  # Fetched all records

            except requests.exceptions.RequestException as e:
                logging.error(f"Failed to fetch data from {endpoint}: {e}")
                break  # Exit loop on error

        logging.info(f"Finished fetching {len(all_records)} records from '{endpoint}'.")
        return all_records

    def _fetch_endpoint_data(self, endpoint: str) -> Tuple[str, List[Dict[str, Any]]]:
        """A wrapper function to fetch data and return it along with the endpoint name."""
        data = self._fetch_paginated_data(endpoint)
        return endpoint, data

    def _log_sample_api_data(self, data: List[Dict[str, Any]], table_name: str) -> None:
        """
        If debug logging is enabled, logs a sample of non-null values for each 
        key in the fetched data. This is useful for inspecting API data structures.

        Args:
            data (List[Dict[str, Any]]): The list of records from the API.
            table_name (str): The name of the endpoint for logging purposes.
        """
        # Proceed only if debug logging is enabled and data is not empty
        if not logging.getLogger().isEnabledFor(logging.DEBUG) or not data:
            return
        try:
            debug_log_lines = [f"Sample non-null values for endpoint '{table_name}':"]
            # Get keys from the first record, assuming all records have the same structure
            keys = data[0].keys()
            for key in keys:
                # Find the first non-null value for the current key across all records
                first_valid_value = next((item.get(key) for item in data if item.get(key) is not None), "")
                # Append the key and a truncated version of the value to the log message
                debug_log_lines.append(f"  - {key}: {str(first_valid_value)[:100]}")
            logging.debug("\n".join(debug_log_lines))
        except Exception as e:
            # Log any error during the sampling process without halting the ETL
            logging.debug(f"Could not generate sample data log for '{table_name}': {e}")

    def extract_and_load(self, db_loader: PostgresLoader) -> None:
        """
        Fetches data from all API endpoints and loads it into the database.
        Uses a thread pool for parallel execution if USE_MULTITHREADING is True.
        Otherwise, processes endpoints sequentially.
        """
        if not self.session.headers.get("Authorization"):
            logging.error("Cannot proceed with API extraction without an access token.")
            return

        if USE_MULTITHREADING:
            # --- Parallel Execution Path ---
            logging.info(f"Starting PARALLEL API data extraction for {len(API_ENDPOINTS)} endpoints using up to {MAX_WORKERS} workers.")
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_endpoint = {executor.submit(self._fetch_endpoint_data, endpoint): endpoint for endpoint in API_ENDPOINTS}
                for future in as_completed(future_to_endpoint):
                    endpoint = future_to_endpoint[future]
                    try:
                        table_name, data = future.result()
                        if data:
                            self._log_sample_api_data(data, table_name)
                            df = pd.DataFrame(data)
                            df = df.drop_duplicates().reset_index(drop=True)
                            db_loader.load_dataframe(df, table_name)
                        else:
                            logging.warning(f"No data returned from endpoint '{table_name}'.")
                    except Exception as e:
                        # Continue to the next endpoint
                        logging.error(f"Failed to process API endpoint {endpoint}: {e}")
        else:
            # --- Sequential Execution Path ---
            logging.info(f"Starting SEQUENTIAL API data extraction for {len(API_ENDPOINTS)} endpoints.")
            for endpoint in API_ENDPOINTS:
                try:
                    table_name, data = self._fetch_endpoint_data(endpoint)
                    if data:
                        self._log_sample_api_data(data, table_name)
                        df = pd.DataFrame(data)
                        df = df.drop_duplicates().reset_index(drop=True)
                        db_loader.load_dataframe(df, table_name)
                    else:
                        logging.warning(f"No data returned from endpoint '{table_name}'.")
                except Exception as e:
                    logging.error(f"Failed to process API endpoint {endpoint}: {e}")
                    # Continue to the next endpoint
                    continue


def main() -> None:
    """Main function to orchestrate the ETL process."""
    logging.info("--- Starting ETL Process ---")
    config = Config()
    try:
        # Initialize the database loader
        db_loader = PostgresLoader(config)
        # Abort if the database connection failed
        if not db_loader.engine:
            raise ConnectionError("Database engine could not be initialized. Aborting ETL process.")

        # Process CSV files
        csv_extractor = CsvExtractor(config)
        csv_extractor.extract_and_load(db_loader)

        # Process API data
        api_extractor = ApiExtractor(config)
        api_extractor.extract_and_load(db_loader)

        logging.info("--- ETL Process Completed Successfully ---")
    except Exception as e:
        logging.critical(f"A critical error occurred during the ETL process: {e}", exc_info=True)


if __name__ == "__main__":
    main()
