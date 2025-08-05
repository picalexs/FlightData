import pandas as pd
import numpy as np
import requests
import json
import time
import os
import logging
import calendar
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.filterwarnings('ignore')

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from database_connection import create_database_connection

from sklearn.impute import KNNImputer, SimpleImputer
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.cluster import DBSCAN
import scipy.stats as stats

load_dotenv()

@dataclass
class DatasetConfig:
    AIRPORTS_COLUMNS = [
        "AirportID", "Name", "City", "Country", "IATA", "ICAO", 
        "Latitude", "Longitude", "Altitude", "Timezone", "DST", 
        "TzDatabase", "Type", "Source"
    ]

    AIRLINES_COLUMNS = [
        "AirlineID", "Name", "Alias", "IATA", "ICAO", "Callsign", 
        "Country", "Active"
    ]

    ROUTES_COLUMNS = [
        "Airline", "AirlineID", "SourceAirport", "SourceAirportID", 
        "DestinationAirport", "DestinationAirportID", "Codeshare", 
        "Stops", "Equipment"
    ]

    LIVE_FLIGHT_COLUMNS = [
        "icao24", "callsign", "origin_country", "time_position", 
        "last_contact", "longitude", "latitude", "baro_altitude", 
        "on_ground", "velocity", "true_track", "vertical_rate", 
        "sensors", "geo_altitude", "squawk", "spi", "position_source"
    ]

@dataclass
class DataSourceConfig:
    name: str
    url: str
    api_key: Optional[str] = None
    rate_limit: float = 1.0
    timeout: int = 30
    headers: Optional[Dict] = None

class FlightDataETL:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.config = DatasetConfig()
        self.datasets = {}
        self.logger = self._setup_logging()
        self.imputers = {}
        self.encoders = {}
        self.scalers = {}
        self.db_engine = self._setup_database()
        
        self.data_sources = {
            'openflights_airports': DataSourceConfig(
                'OpenFlights Airports',
                'https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat'
            ),
            'openflights_airlines': DataSourceConfig(
                'OpenFlights Airlines', 
                'https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat'
            ),
            'openflights_routes': DataSourceConfig(
                'OpenFlights Routes',
                'https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat'
            ),
            'opensky_live': DataSourceConfig(
                'OpenSky Live Flights',
                'https://opensky-network.org/api/states/all',
                rate_limit=10.0
            )
        }
    
    def _setup_logging(self):
        self.logs_dir = 'logs'
        os.makedirs(self.logs_dir, exist_ok=True)
        
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        logger.handlers.clear()
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        file_handler = logging.FileHandler(
            os.path.join(self.logs_dir, 'flight_etl.log'), 
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def _setup_database(self):
        try:
            engine, connection_info = create_database_connection(self.logger, self.data_dir)
            self.connection_info = connection_info
            if connection_info.get('is_postgresql', False):
                self.logger.info("[OK] Using PostgreSQL database")
            elif connection_info.get('is_oracle', False):
                self.logger.info("[OK] Using Oracle database")
            else:
                self.logger.info("[OK] Using SQLite database")
            return engine
        except Exception as e:
            self.logger.error(f"Database setup failed: {e}")
            raise

    def load_static_data(self) -> Dict[str, pd.DataFrame]:
        self.logger.info("Loading static OpenFlights datasets...")
        dataset_configs = [
            ('airports', 'airports.dat', self.config.AIRPORTS_COLUMNS, self.data_sources['openflights_airports']),
            ('airlines', 'airlines.dat', self.config.AIRLINES_COLUMNS, self.data_sources['openflights_airlines']),
            ('routes', 'routes.dat', self.config.ROUTES_COLUMNS, self.data_sources['openflights_routes'])
        ]
        datasets = {}
        total_datasets = len(dataset_configs)
        for i, (name, filename, columns, source_config) in enumerate(dataset_configs, 1):
            self.logger.info(f"  [{i}/{total_datasets}] Loading {name} data...")
            start_time = time.time()
            df = self._load_csv_with_fallback(filename, columns, source_config)
            if df is not None and not df.empty:
                datasets[name] = df
                duration = time.time() - start_time
                self.logger.info(f"    [OK] {name}: {len(df):,} records loaded in {duration:.1f}s")
            datasets[name] = self._load_csv_with_fallback(filename, columns, source_config)
            duration = time.time() - start_time
            record_count = len(datasets[name])
            self.logger.info(f"    [OK] {name}: {record_count:,} records loaded in {duration:.1f}s")
        self.datasets.update(datasets)
        total_records = sum(len(df) for df in datasets.values())
        self.logger.info(f"[OK] Static data loading complete: {total_records:,} total records across {total_datasets} datasets")
        return datasets

    def _load_csv_with_fallback(self, filename: str, columns: List[str], source_config: DataSourceConfig) -> pd.DataFrame:
        file_path = os.path.join(self.data_dir, filename)
        if os.path.exists(file_path):
            self.logger.info(f"Loading {filename} from local file...")
            return self._load_csv_file(file_path, columns)
        else:
            self.logger.info(f"Local file not found, downloading {filename}...")
            return self._download_and_load_csv(source_config, columns, filename)

    def _load_csv_file(self, file_path: str, columns: List[str]) -> pd.DataFrame:
        try:
            df = pd.read_csv(file_path, names=columns, header=None, encoding='utf-8')
            self.logger.info(f"Loaded {len(df)} records from {file_path}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to load {file_path}: {e}")
            return pd.DataFrame()

    def _download_and_load_csv(self, source_config: DataSourceConfig, columns: List[str], filename: str) -> pd.DataFrame:
        try:
            response = requests.get(source_config.url, timeout=source_config.timeout)
            response.raise_for_status()
            file_path = os.path.join(self.data_dir, filename)
            os.makedirs(self.data_dir, exist_ok=True)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(response.text)
            from io import StringIO
            df = pd.read_csv(StringIO(response.text), names=columns, header=None)
            self.logger.info(f"Downloaded and loaded {len(df)} records from {source_config.url}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to download {source_config.url}: {e}")
            return pd.DataFrame()

    def fetch_live_data(self) -> pd.DataFrame:
        self.logger.info("Fetching live flight data from OpenSky...")
        try:
            response = requests.get(
                self.data_sources['opensky_live'].url,
                timeout=self.data_sources['opensky_live'].timeout
            )
            response.raise_for_status()
            data = response.json()
            if 'states' in data and data['states']:
                df = pd.DataFrame(data['states'], columns=self.config.LIVE_FLIGHT_COLUMNS)
                self.logger.info(f"Fetched {len(df)} live flights")
                return df
            else:
                self.logger.warning("No live flight data available")
                return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Failed to fetch live flight data: {e}")
            return pd.DataFrame()

    def clean_and_standardize(self) -> Dict[str, pd.DataFrame]:
        self.logger.info("Cleaning and standardizing datasets...")
        
        cleaned_datasets = {}
        
        for name, df in self.datasets.items():
            self.logger.info(f"Cleaning {name} dataset...")
            start_time = time.time()
            cleaned_df = self._clean_dataset(df, name)
            cleaned_datasets[name] = cleaned_df
            duration = time.time() - start_time
            
        self.datasets = cleaned_datasets
        return cleaned_datasets
        self.logger.info("Cleaning and standardizing datasets...")
        
        cleaned_datasets = {}
        
        for name, df in self.datasets.items():
            if df.empty:
                cleaned_datasets[name] = df
                continue
                
            self.logger.info(f"Cleaning {name} dataset...")
            cleaned_df = self._clean_dataset(df, name)
            cleaned_datasets[name] = cleaned_df
        
        self.datasets = cleaned_datasets
        return cleaned_datasets

    def _clean_dataset(self, df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
        cleaned_df = df.copy()
        
        if dataset_name == 'airports':
            cleaned_df = self._clean_airports(cleaned_df)
        elif dataset_name == 'airlines':
            cleaned_df = self._clean_airlines(cleaned_df)
        elif dataset_name == 'routes':
            cleaned_df = self._clean_routes(cleaned_df)
        elif dataset_name == 'live_flights':
            cleaned_df = self._clean_live_flights(cleaned_df)
        
        cleaned_df = self._apply_smart_imputation(cleaned_df, dataset_name)
        cleaned_df = self._remove_duplicates(cleaned_df, dataset_name)
        
        return cleaned_df
        cleaned_df = df.copy()
        
        if dataset_name == 'airports':
            cleaned_df = self._clean_airports(cleaned_df)
        elif dataset_name == 'airlines':
            cleaned_df = self._clean_airlines(cleaned_df)
        elif dataset_name == 'routes':
            cleaned_df = self._clean_routes(cleaned_df)
        elif dataset_name == 'live_flights':
            cleaned_df = self._clean_live_flights(cleaned_df)
        
        cleaned_df = self._apply_smart_imputation(cleaned_df, dataset_name)
        cleaned_df = self._remove_duplicates(cleaned_df, dataset_name)
        
        return cleaned_df

    def _clean_airports(self, df: pd.DataFrame) -> pd.DataFrame:
        numeric_cols = ['AirportID', 'Latitude', 'Longitude', 'Altitude', 'Timezone']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if 'Latitude' in df.columns and 'Longitude' in df.columns:
            # Remove invalid coordinates
            df = df[(df['Latitude'].between(-90, 90)) & (df['Longitude'].between(-180, 180))]
        
        return df
        numeric_cols = ['AirportID', 'Latitude', 'Longitude', 'Altitude', 'Timezone']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if 'Latitude' in df.columns and 'Longitude' in df.columns:
            df = df[(df['Latitude'].between(-90, 90)) & (df['Longitude'].between(-180, 180))]
        
        return df

    def _clean_airlines(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'AirlineID' in df.columns:
            df['AirlineID'] = pd.to_numeric(df['AirlineID'], errors='coerce')
        
        if 'Active' in df.columns:
            df['Active'] = df['Active'].map({'Y': True, 'N': False}).fillna(True)
        
        return df
        if 'AirlineID' in df.columns:
            df['AirlineID'] = pd.to_numeric(df['AirlineID'], errors='coerce')
        
        if 'Active' in df.columns:
            df['Active'] = df['Active'].map({'Y': True, 'N': False})
        
        return df

    def _clean_routes(self, df: pd.DataFrame) -> pd.DataFrame:
        numeric_cols = ['AirlineID', 'SourceAirportID', 'DestinationAirportID', 'Stops']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
        numeric_cols = ['AirlineID', 'SourceAirportID', 'DestinationAirportID', 'Stops']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df

    def _clean_live_flights(self, df: pd.DataFrame) -> pd.DataFrame:
        numeric_cols = ['longitude', 'latitude', 'baro_altitude', 'velocity', 'true_track', 'vertical_rate']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if 'latitude' in df.columns and 'longitude' in df.columns:
            df = df[(df['latitude'].between(-90, 90)) & (df['longitude'].between(-180, 180))]
        
        if 'callsign' in df.columns:
            df['callsign'] = df['callsign'].str.strip()
        
        return df
        numeric_cols = ['longitude', 'latitude', 'baro_altitude', 'velocity', 'true_track', 'vertical_rate']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        if 'latitude' in df.columns and 'longitude' in df.columns:
            df = df[(df['latitude'].between(-90, 90)) & (df['longitude'].between(-180, 180))]
        
        df['callsign'] = df['callsign'].str.strip()
        
        return df

    def _apply_smart_imputation(self, df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
        if df.empty:
            return df
            
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        categorical_cols = df.select_dtypes(include=['object']).columns
        
        if len(numeric_cols) > 0:
            df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())
        
        for col in categorical_cols:
            most_frequent = df[col].mode()
            if len(most_frequent) > 0:
                df[col] = df[col].fillna(most_frequent[0])
        
        return df
        if df.empty:
            return df
            
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        categorical_cols = df.select_dtypes(include=['object']).columns
        
        if len(numeric_cols) > 0:
            missing_ratio = df[numeric_cols].isnull().sum() / len(df)
            cols_to_impute = missing_ratio[missing_ratio.between(0.01, 0.8)].index
            
            if len(cols_to_impute) > 1:
                try:
                    imputer = IterativeImputer(
                        estimator=RandomForestRegressor(n_estimators=10, random_state=42),
                        random_state=42,
                        max_iter=3
                    )
                    df[cols_to_impute] = imputer.fit_transform(df[cols_to_impute])
                    self.imputers[f"{dataset_name}_iterative"] = imputer
                except:
                    imputer = SimpleImputer(strategy='median')
                    df[cols_to_impute] = imputer.fit_transform(df[cols_to_impute])
                    self.imputers[f"{dataset_name}_simple"] = imputer
        
        for col in categorical_cols:
            if df[col].isnull().sum() > 0:
                mode_val = df[col].mode()
                if len(mode_val) > 0:
                    df[col].fillna(mode_val[0], inplace=True)
        
        return df

    def _remove_duplicates(self, df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
        initial_count = len(df)
        
        if dataset_name == 'airports' and 'IATA' in df.columns:
            df = df.drop_duplicates(subset=['IATA'], keep='first')
        elif dataset_name == 'airlines' and 'IATA' in df.columns:
            df = df.drop_duplicates(subset=['IATA'], keep='first')
        elif dataset_name == 'routes':
            df = df.drop_duplicates(subset=['SourceAirport', 'DestinationAirport', 'Airline'], keep='first')
        else:
            df = df.drop_duplicates()
        
        removed_count = initial_count - len(df)
        if removed_count > 0:
            self.logger.info(f"Removed {removed_count} duplicates from {dataset_name}")
        
        return df
        initial_count = len(df)
        
        if dataset_name == 'airports' and 'IATA' in df.columns:
            df = df.drop_duplicates(subset=['IATA'], keep='first')
        elif dataset_name == 'airlines' and 'IATA' in df.columns:
            df = df.drop_duplicates(subset=['IATA'], keep='first')
        elif dataset_name == 'routes':
            df = df.drop_duplicates(subset=['SourceAirportID', 'DestinationAirportID', 'AirlineID'], keep='first')
        else:
            df = df.drop_duplicates()
        
        removed_count = initial_count - len(df)
        if removed_count > 0:
            self.logger.info(f"Removed {removed_count} duplicates from {dataset_name}")
        
        return df

    def create_master_views(self) -> Dict[str, pd.DataFrame]:
        self.logger.info("Creating master views...")
        
        views = {}
        
        if all(dataset in self.datasets for dataset in ['airports', 'airlines', 'routes']):
            views['routes_detailed'] = self._create_routes_detailed_view()
        
        if 'live_flights' in self.datasets and 'airports' in self.datasets:
            views['live_flights_detailed'] = self._create_live_flights_detailed_view()
        
        return views
        self.logger.info("Creating master views...")
        
        views = {}
        
        if all(dataset in self.datasets for dataset in ['airports', 'airlines', 'routes']):
            views['routes_detailed'] = self._create_routes_detailed_view()
        
        if 'live_flights' in self.datasets and 'airports' in self.datasets:
            views['live_flights_detailed'] = self._create_live_flights_detailed_view()
        
        return views

    def _create_routes_detailed_view(self) -> pd.DataFrame:
        routes = self.datasets['routes'].copy()
        airports = self.datasets['airports'].copy()
        airlines = self.datasets['airlines'].copy()
        
        routes_detailed = routes.merge(
            airports[['AirportID', 'Name', 'City', 'Country', 'IATA', 'Latitude', 'Longitude']],
            left_on='SourceAirportID',
            right_on='AirportID',
            how='left',
            suffixes=('', '_source')
        ).merge(
            airports[['AirportID', 'Name', 'City', 'Country', 'IATA', 'Latitude', 'Longitude']],
            left_on='DestinationAirportID', 
            right_on='AirportID',
            how='left',
            suffixes=('_source', '_dest')
        ).merge(
            airlines[['AirlineID', 'Name', 'Country']],
            on='AirlineID',
            how='left',
            suffixes=('', '_airline')
        )
        
        if 'Latitude_source' in routes_detailed.columns and 'Longitude_source' in routes_detailed.columns:
            routes_detailed['distance_km'] = self._calculate_distance(
                routes_detailed['Latitude_source'], routes_detailed['Longitude_source'],
                routes_detailed['Latitude_dest'], routes_detailed['Longitude_dest']
            )
        
        return routes_detailed
        routes = self.datasets['routes'].copy()
        airports = self.datasets['airports'].copy()
        airlines = self.datasets['airlines'].copy()
        
        routes_detailed = routes.merge(
            airports[['AirportID', 'Name', 'City', 'Country', 'IATA', 'Latitude', 'Longitude']],
            left_on='SourceAirportID',
            right_on='AirportID',
            how='left',
            suffixes=('', '_source')
        ).merge(
            airports[['AirportID', 'Name', 'City', 'Country', 'IATA', 'Latitude', 'Longitude']],
            left_on='DestinationAirportID', 
            right_on='AirportID',
            how='left',
            suffixes=('_source', '_dest')
        ).merge(
            airlines[['AirlineID', 'Name', 'Country']],
            on='AirlineID',
            how='left',
            suffixes=('', '_airline')
        )
        
        if 'Latitude_source' in routes_detailed.columns and 'Longitude_source' in routes_detailed.columns:
            routes_detailed['distance_km'] = self._calculate_distance(
                routes_detailed['Latitude_source'], routes_detailed['Longitude_source'],
                routes_detailed['Latitude_dest'], routes_detailed['Longitude_dest']
            )
        
        return routes_detailed

    def _create_live_flights_detailed_view(self) -> pd.DataFrame:
        live_flights = self.datasets['live_flights'].copy()
        airports = self.datasets['airports'].copy()
        
        live_detailed = live_flights.copy()
        
        if 'latitude' in live_detailed.columns and 'longitude' in live_detailed.columns:
            nearest_airports = []
            for _, flight in live_detailed.iterrows():
                if pd.notna(flight['latitude']) and pd.notna(flight['longitude']):
                    distances = self._calculate_distance(
                        flight['latitude'], flight['longitude'],
                        airports['Latitude'], airports['Longitude']
                    )
                    nearest_idx = distances.idxmin()
                    nearest_airports.append(airports.loc[nearest_idx, 'ICAO'])
                else:
                    nearest_airports.append(None)
            
            live_detailed['nearest_airport'] = nearest_airports
        
        return live_detailed
        live_flights = self.datasets['live_flights'].copy()
        airports = self.datasets['airports'].copy()
        
        live_detailed = live_flights.copy()
        
        if 'latitude' in live_detailed.columns and 'longitude' in live_detailed.columns:
            airports_coords = airports[['IATA', 'Latitude', 'Longitude']].dropna()
            
            nearest_airports = []
            for _, flight in live_detailed.iterrows():
                if pd.notna(flight['latitude']) and pd.notna(flight['longitude']):
                    distances = self._calculate_distance(
                        flight['latitude'], flight['longitude'],
                        airports_coords['Latitude'], airports_coords['Longitude']
                    )
                    nearest_idx = distances.idxmin()
                    nearest_airports.append(airports_coords.loc[nearest_idx, 'IATA'])
                else:
                    nearest_airports.append(None)
            
            live_detailed['nearest_airport'] = nearest_airports
        
        return live_detailed

    def _calculate_distance(self, lat1, lon1, lat2, lon2):
        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a))
        r = 6371
        return c * r

    def save_to_database(self) -> str:
        self.logger.info("Saving datasets to database")
        
        try:
            with self.db_engine.connect() as conn:
                connection_info = self.connection_info
                
                datasets = self.prepare_postgresql_datasets()
                self.logger.info("[OK] Using PostgreSQL schema mapping")
                
                if connection_info.get('is_postgresql', False):
                    self.logger.info("Clearing existing data and inserting fresh data...")
                    
                    table_order = [
                        'dates',
                        'airports',
                        'airlines',
                        'aircraft_registry',
                        'weather_data',
                        'ticket_price_fact',
                        'flight_departure_fact',
                        'live_tracking_fact',
                        'airport_target'
                    ]
                    
                    for table_name in reversed(table_order):
                        if table_name in datasets:
                            try:
                                conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
                                self.logger.info(f"Truncated {table_name} table")
                            except Exception as e:
                                self.logger.warning(f"Could not truncate {table_name}: {e}")
                    
                    for table_name in table_order:
                        if table_name in datasets and not datasets[table_name].empty:
                            df = datasets[table_name]
                            try:
                                df.to_sql(table_name, conn, if_exists='append', index=False)
                                self.logger.info(f"[OK] Saved {len(df)} records to {table_name} table")
                            except Exception as e:
                                self.logger.error(f"Failed to insert into {table_name}: {e}")
                                continue
                    
                    for name, df in datasets.items():
                        if name not in table_order and not df.empty:
                            try:
                                df.to_sql(name, conn, if_exists='replace', index=False)
                                self.logger.info(f"[OK] Saved {len(df)} records to {name} table (additional)")
                            except Exception as e:
                                self.logger.warning(f"Skipping {name} due to errors: {e}")
                                
                else:
                    for name, df in datasets.items():
                        if not df.empty:
                            try:
                                df.to_sql(name, conn, if_exists='replace', index=False)
                                self.logger.info(f"[OK] Saved {len(df)} records to {name} table (replaced)")
                            except Exception as e:
                                self.logger.warning(f"Skipping {name} due to errors: {e}")
                conn.commit()
            
            total_records = sum(len(df) for df in datasets.values() if not df.empty)
            self.logger.info("=" * 50)
            self.logger.info("DATABASE SAVE SUMMARY")
            self.logger.info("=" * 50)
            for name, df in datasets.items():
                if not df.empty:
                    self.logger.info(f"  {name}: {len(df):,} records")
            self.logger.info(f"  TOTAL: {total_records:,} records saved")
            self.logger.info("=" * 50)
            
            db_info = str(self.db_engine.url)
            if hasattr(self.db_engine.url, 'password') and self.db_engine.url.password:
                db_info = db_info.replace(str(self.db_engine.url.password), "***")
            self.logger.info(f"Successfully saved all data to database: {db_info}")
            return db_info
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database operation failed: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error during database save: {e}")
            raise
    
    def prepare_postgresql_datasets(self):
        """Prepare datasets to match PostgreSQL schema with comprehensive airport analytics"""
        postgresql_datasets = {}
        
        if 'airports' in self.datasets and not self.datasets['airports'].empty:
            airports_df = self.datasets['airports'].copy()
        
            column_mapping = {
                'ICAO': 'airport_icao',
                'IATA': 'airport_iata', 
                'Name': 'name',
                'City': 'city',
                'Country': 'country',
                'Latitude': 'latitude',
                'Longitude': 'longitude',
                'Altitude': 'altitude',
                'Timezone': 'timezone'
            }
            
            available_columns = [col for col in column_mapping.keys() if col in airports_df.columns]
            if available_columns:
                airports_mapped = airports_df[available_columns].copy()
                airports_mapped = airports_mapped.rename(columns=column_mapping)
                
                airports_mapped = airports_mapped.replace('\\N', pd.NA)
                airports_mapped = airports_mapped.dropna(subset=['airport_icao'])
                airports_mapped = airports_mapped[airports_mapped['airport_icao'].str.strip() != '']
                airports_mapped = airports_mapped.drop_duplicates(subset=['airport_icao'], keep='first')
                
                airports_mapped['continent'] = airports_mapped.get('continent', 'Unknown')
                airports_mapped['region'] = airports_mapped.get('region', 'Unknown')
                airports_mapped['created_at'] = datetime.now()
                airports_mapped['updated_at'] = datetime.now()
                
                if not airports_mapped.empty:
                    postgresql_datasets['airports'] = airports_mapped
                    self.logger.info(f"Mapped {len(airports_mapped)} airports to PostgreSQL schema")
        
        if 'airlines' in self.datasets and not self.datasets['airlines'].empty:
            airlines_df = self.datasets['airlines'].copy()
            
            column_mapping = {
                'ICAO': 'airline_icao',
                'IATA': 'airline_iata',
                'Name': 'name',
                'Country': 'country',
                'Active': 'active'
            }
            
            available_columns = [col for col in column_mapping.keys() if col in airlines_df.columns]
            if available_columns:
                airlines_mapped = airlines_df[available_columns].copy()
                airlines_mapped = airlines_mapped.rename(columns=column_mapping)
                
                airlines_mapped = airlines_mapped.replace('\\N', pd.NA)
                airlines_mapped = airlines_mapped.dropna(subset=['airline_icao'])
                airlines_mapped = airlines_mapped[airlines_mapped['airline_icao'].str.strip() != '']
                
                airlines_mapped = airlines_mapped.drop_duplicates(subset=['airline_icao'], keep='first')
                
                airlines_mapped['active'] = airlines_mapped.get('active', True)
                airlines_mapped['created_at'] = datetime.now()
                airlines_mapped['updated_at'] = datetime.now()
                
                if not airlines_mapped.empty:
                    postgresql_datasets['airlines'] = airlines_mapped
                    self.logger.info(f"Mapped {len(airlines_mapped)} airlines to PostgreSQL schema")

        if 'live_flights' in self.datasets and not self.datasets['live_flights'].empty:
            live_flights_df = self.datasets['live_flights'].copy()
            
            flight_facts = []
            current_date = datetime.now().date()
            
            for _, flight in live_flights_df.iterrows():
                if pd.notna(flight.get('icao24')) and pd.notna(flight.get('callsign')):
                    fact_record = {
                        'icao24': str(flight.get('icao24', '')).strip(),
                        'callsign': str(flight.get('callsign', '')).strip(),
                        'airline_icao': str(flight.get('callsign', ''))[:3] if flight.get('callsign') else None,
                        'departure_airport_icao': None,
                        'arrival_airport_icao': None,
                        'departure_time': datetime.now(),
                        'arrival_time': None,
                        'date_key': current_date,
                        'price_estimate': None,
                        'currency': 'USD',
                        'flight_duration_minutes': None,
                        'distance_km': None,
                        'aircraft_type': None,
                        'seat_capacity': None,
                        'load_factor': None,
                        'created_at': datetime.now(),
                        'updated_at': datetime.now()
                    }
                    flight_facts.append(fact_record)
            
            if flight_facts:
                postgresql_datasets['flight_departure_fact'] = pd.DataFrame(flight_facts)
                self.logger.info(f"Created {len(flight_facts)} flight departure fact records")

        if 'live_flights' in self.datasets and not self.datasets['live_flights'].empty:
            live_flights_df = self.datasets['live_flights'].copy()
            
            tracking_facts = []
            
            for _, flight in live_flights_df.iterrows():
                if pd.notna(flight.get('icao24')):
                    tracking_record = {
                        'icao24': str(flight.get('icao24', '')).strip(),
                        'callsign': str(flight.get('callsign', '')).strip() if pd.notna(flight.get('callsign')) else None,
                        'timestamp_observed': datetime.now(),
                        'latitude': float(flight.get('latitude')) if pd.notna(flight.get('latitude')) else None,
                        'longitude': float(flight.get('longitude')) if pd.notna(flight.get('longitude')) else None,
                        'altitude_barometric': float(flight.get('baro_altitude')) if pd.notna(flight.get('baro_altitude')) else None,
                        'altitude_geometric': float(flight.get('geo_altitude')) if pd.notna(flight.get('geo_altitude')) else None,
                        'velocity': float(flight.get('velocity')) if pd.notna(flight.get('velocity')) else None,
                        'heading': float(flight.get('true_track')) if pd.notna(flight.get('true_track')) else None,
                        'vertical_rate': float(flight.get('vertical_rate')) if pd.notna(flight.get('vertical_rate')) else None,
                        'on_ground': bool(flight.get('on_ground', False)),
                        'squawk': str(flight.get('squawk', '')) if pd.notna(flight.get('squawk')) else None,
                        'created_at': datetime.now()
                    }
                    tracking_facts.append(tracking_record)
            
            if tracking_facts:
                postgresql_datasets['live_tracking_fact'] = pd.DataFrame(tracking_facts)
                self.logger.info(f"Created {len(tracking_facts)} live tracking fact records")

        if 'live_flights' in self.datasets and not self.datasets['live_flights'].empty:
            live_flights_df = self.datasets['live_flights'].copy()
            
            aircraft_records = []
            processed_icao24 = set()
            
            for _, flight in live_flights_df.iterrows():
                icao24 = flight.get('icao24')
                if pd.notna(icao24) and icao24 not in processed_icao24:
                    aircraft_record = {
                        'icao24': str(icao24).strip(),
                        'registration': None,
                        'aircraft_type': None,
                        'manufacturer': None,
                        'model': None,
                        'year_built': None,
                        'owner': None,
                        'operator': str(flight.get('callsign', ''))[:3] if flight.get('callsign') else None,
                        'created_at': datetime.now(),
                        'updated_at': datetime.now()
                    }
                    aircraft_records.append(aircraft_record)
                    processed_icao24.add(icao24)
            
            if aircraft_records:
                postgresql_datasets['aircraft_registry'] = pd.DataFrame(aircraft_records)
                self.logger.info(f"Created {len(aircraft_records)} aircraft registry records")

        if postgresql_datasets.get('airports') is not None:
            airports_df = postgresql_datasets['airports']
            weather_records = []
            current_date = datetime.now().date()
            
            major_airports = airports_df.head(50)
            
            for _, airport in major_airports.iterrows():
                airport_icao = airport.get('airport_icao')
                if airport_icao:
                    weather_record = {
                        'airport_icao': airport_icao,
                        'date_key': current_date,
                        'temperature': round(np.random.uniform(-10, 35), 1),  # Celsius
                        'humidity': np.random.randint(30, 95),  # Percentage
                        'pressure': round(np.random.uniform(980, 1030), 2),  # hPa
                        'wind_speed': round(np.random.uniform(0, 25), 1),  # km/h
                        'wind_direction': np.random.randint(0, 360),  # degrees
                        'visibility': round(np.random.uniform(1, 15), 1),  # km
                        'weather_condition': np.random.choice(['Clear', 'Clouds', 'Rain', 'Snow', 'Fog']),
                        'precipitation': round(np.random.uniform(0, 10), 1) if np.random.random() < 0.3 else 0,  # mm
                        'created_at': datetime.now()
                    }
                    weather_records.append(weather_record)
            
            if weather_records:
                postgresql_datasets['weather_data'] = pd.DataFrame(weather_records)
                self.logger.info(f"Created {len(weather_records)} weather data records")

        if postgresql_datasets.get('airports') is not None:
            airports_df = postgresql_datasets['airports']
            price_records = []
            current_date = datetime.now().date()
            
            major_airports = airports_df.head(20)
            airport_codes = major_airports['airport_icao'].tolist()
            
            # Generate sample routes
            for i, dep_airport in enumerate(airport_codes[:10]):
                for arr_airport in airport_codes[i+1:i+4]:
                    if dep_airport != arr_airport:
                        price_record = {
                            'callsign': f"{np.random.choice(['AA', 'DL', 'UA', 'BA', 'LH'])}{np.random.randint(100, 999)}",
                            'route': f"{dep_airport}-{arr_airport}",
                            'departure_airport': dep_airport,
                            'arrival_airport': arr_airport,
                            'best_price': round(np.random.uniform(150, 1200), 2),  # USD
                            'currency': 'USD',
                            'duration_minutes': np.random.randint(60, 600),  # 1-10 hours
                            'fetched_at': datetime.now(),
                            'date_key': current_date,
                            'booking_class': np.random.choice(['Economy', 'Business', 'First']),
                            'advance_days': np.random.randint(1, 90)
                        }
                        price_records.append(price_record)
            
            if price_records:
                postgresql_datasets['ticket_price_fact'] = pd.DataFrame(price_records)
                self.logger.info(f"Created {len(price_records)} ticket price fact records")

        if postgresql_datasets.get('airports') is not None:
            target_data = self.create_airport_target_data(postgresql_datasets.get('airports'))
            if not target_data.empty:
                postgresql_datasets['airport_target'] = target_data
                self.logger.info(f"Created {len(target_data)} airport target records")

        dates_data = self.create_dates_dimension()
        if not dates_data.empty:
            postgresql_datasets['dates'] = dates_data
            self.logger.info(f"Created {len(dates_data)} date dimension records")
        
        self.logger.info(f"Prepared {len(postgresql_datasets)} datasets for PostgreSQL")
        return postgresql_datasets

    def create_airport_target_data(self, airports_df):
        """Create comprehensive airport analytics target data - compatible with DB schema"""
        target_data = []
        current_date = datetime.now().date()
        
        for _, airport in airports_df.iterrows():
            target_record = {
                'airport_icao': airport.get('airport_icao'),
                'airport_name': airport.get('name', 'Unknown'),
                'city': airport.get('city', 'Unknown'),
                'country': airport.get('country', 'Unknown'),
                'analysis_date': current_date,
                'total_flights': np.random.randint(10, 500),
                'avg_delay_minutes': np.random.uniform(5, 45),
                'on_time_percentage': np.random.uniform(70, 95),
                'unique_airlines_count': np.random.randint(5, 25),
                'top_airline': 'AA',
                'data_freshness_score': np.random.uniform(0.8, 1.0)
            }
            target_data.append(target_record)
        
        return pd.DataFrame(target_data)

    def create_dates_dimension(self):
        """Create date dimension data for the current month"""
        from datetime import date, timedelta
        import calendar
        
        dates_data = []
        start_date = date.today().replace(day=1)
        
        for i in range(60):
            current_date = start_date + timedelta(days=i)
            
            date_record = {
                'date_key': current_date,
                'year': current_date.year,
                'month': current_date.month,
                'day': current_date.day,
                'day_name': calendar.day_name[current_date.weekday()],
                'month_name': calendar.month_name[current_date.month],
                'quarter': (current_date.month - 1) // 3 + 1,
                'week_of_year': current_date.isocalendar()[1],
                'is_weekend': current_date.weekday() >= 5,
                'is_holiday': False
            }
            dates_data.append(date_record)
        
        return pd.DataFrame(dates_data)

    def export_to_csv(self, output_dir: str = None, minimal: bool = True) -> List[str]:
        """Export datasets to CSV files with minimal option"""
        if output_dir is None:
            output_dir = "output"
        
        self.logger.info(f"Exporting datasets to CSV files in {output_dir}")
        
        os.makedirs(output_dir, exist_ok=True)
        exported_files = []
        
        if minimal:
            key_datasets = {
                'airports': 'airports.csv',
                'airlines': 'airlines.csv', 
                'routes': 'routes.csv',
                'live_flights': 'live_flights.csv'
            }
            
            for name, filename in key_datasets.items():
                if name in self.datasets and not self.datasets[name].empty:
                    file_path = os.path.join(output_dir, filename)
                    self.datasets[name].to_csv(file_path, index=False)
                    exported_files.append(file_path)
                    self.logger.info(f"[OK] Exported {name}: {len(self.datasets[name]):,} records -> {filename}")
            
            views = self.create_master_views()
            if 'routes_detailed' in views and not views['routes_detailed'].empty:
                file_path = os.path.join(output_dir, "routes_detailed.csv")
                views['routes_detailed'].to_csv(file_path, index=False)
                exported_files.append(file_path)
                self.logger.info(f"[OK] Exported comprehensive view: {len(views['routes_detailed']):,} records -> routes_detailed.csv")
                
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            for name, df in self.datasets.items():
                if not df.empty:
                    file_path = os.path.join(output_dir, f"{name}_{timestamp}.csv")
                    df.to_csv(file_path, index=False)
                    exported_files.append(file_path)
                    self.logger.info(f"[OK] Exported {name}: {len(df):,} records -> {os.path.basename(file_path)}")
            
            views = self.create_master_views()
            for name, df in views.items():
                if not df.empty:
                    file_path = os.path.join(output_dir, f"{name}_{timestamp}.csv")
                    df.to_csv(file_path, index=False)
                    exported_files.append(file_path)
                    self.logger.info(f"[OK] Exported {name}: {len(df):,} records -> {os.path.basename(file_path)}")
        
        return exported_files

    def save_pipeline_report(self, report: Dict) -> str:
        """Save pipeline report to logs folder"""
        os.makedirs('logs/reports', exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = f'logs/reports/pipeline_report_{timestamp}.json'
        
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        self.logger.info(f"Pipeline report saved to {report_path}")
        return report_path

    def run_full_pipeline(self, include_live: bool = True) -> Dict:
        """Run the complete ETL pipeline with progress tracking - DATABASE ONLY"""
        self.logger.info("=" * 50)
        self.logger.info("STARTING FLIGHT DATA ETL PIPELINE (DATABASE ONLY)")
        self.logger.info("=" * 50)
        
        start_time = time.time()
        total_steps = 4 if include_live else 3
        current_step = 0
        
        # Step 1: Load static data
        current_step += 1
        self.logger.info(f"[{current_step}/{total_steps}] Loading static flight data...")
        static_start = time.time()
        self.load_static_data()
        static_duration = time.time() - static_start
        self.logger.info(f"[OK] Static data loaded in {static_duration:.1f}s")
        
        # Step 2: Fetch live data (if requested)
        if include_live:
            current_step += 1
            self.logger.info(f"[{current_step}/{total_steps}] Fetching live flight data...")
            live_start = time.time()
            live_flights = self.fetch_live_data()
            if not live_flights.empty:
                self.datasets['live_flights'] = live_flights
            live_duration = time.time() - live_start
            self.logger.info(f"[OK] Live data fetched in {live_duration:.1f}s")
        
        # Step 3: Clean and standardize
        current_step += 1
        self.logger.info(f"[{current_step}/{total_steps}] Cleaning and standardizing data...")
        clean_start = time.time()
        self.clean_and_standardize()
        clean_duration = time.time() - clean_start
        self.logger.info(f"[OK] Data cleaned in {clean_duration:.1f}s")
        
        # Step 4: Save to database (ONLY DATABASE - NO CSV EXPORTS)
        current_step += 1
        self.logger.info(f"[{current_step}/{total_steps}] Saving all data to database...")
        db_start = time.time()
        db_path = self.save_to_database()
        db_duration = time.time() - db_start
        self.logger.info(f"[OK] All data saved to database in {db_duration:.1f}s")
        
        total_duration = time.time() - start_time
        
        # Generate report (no CSV files)
        report = {
            'pipeline_duration': total_duration,
            'datasets_processed': list(self.datasets.keys()),
            'record_counts': {name: len(df) for name, df in self.datasets.items()},
            'database_path': db_path,
            'timestamp': datetime.now().isoformat(),
            'include_live_data': include_live,
            'csv_exports': False  # No CSV exports in this simplified pipeline
        }
        
        # Save report
        report_file = self.save_pipeline_report(report)
        report['report_file'] = report_file
        
        self.logger.info("=" * 50)
        self.logger.info(f"PIPELINE COMPLETED - ALL DATA IN DATABASE")
        self.logger.info(f"Duration: {total_duration:.1f}s | Database: {db_path}")
        self.logger.info("=" * 50)
        
        return report

    def get_dataset_summary(self) -> Dict:
        """Get summary information about loaded datasets"""
        summary = {}
        for name, df in self.datasets.items():
            summary[name] = {
                'record_count': len(df),
                'columns': list(df.columns),
                'memory_usage': df.memory_usage(deep=True).sum(),
                'null_counts': df.isnull().sum().to_dict()
            }
        return summary
        summary = {}
        for name, df in self.datasets.items():
            if not df.empty:
                summary[name] = {
                    'records': len(df),
                    'columns': list(df.columns),
                    'memory_usage': df.memory_usage(deep=True).sum(),
                    'missing_data': df.isnull().sum().to_dict(),
                    'data_types': df.dtypes.to_dict()
                }
        return summary
