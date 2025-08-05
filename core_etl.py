import pandas as pd
import numpy as np
import requests
import json
import time
import os
import logging
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
        """Setup database connection using separate database module"""
        try:
            engine, connection_info = create_database_connection(self.logger, self.data_dir)
            
            if connection_info['is_oracle']:
                self.logger.info("[OK] Using Oracle database")
            else:
                self.logger.info("[OK] Using SQLite database")
                
            return engine
            
        except Exception as e:
            self.logger.error(f"❌ Database setup failed: {e}")
            raise

    def load_static_data(self) -> Dict[str, pd.DataFrame]:
        """Load static OpenFlights datasets with progress tracking"""
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
            df = pd.read_csv(
                file_path, 
                header=None, 
                names=columns,
                na_values=['\\N'],
                encoding='utf-8',
                quoting=1
            )
            self.logger.info(f"Loaded {len(df)} records from {file_path}")
            return df
        except Exception as e:
            self.logger.error(f"Error loading {file_path}: {e}")
            return pd.DataFrame(columns=columns)

    def _download_and_load_csv(self, source_config: DataSourceConfig, columns: List[str], filename: str) -> pd.DataFrame:
        try:
            response = requests.get(source_config.url, timeout=source_config.timeout)
            response.raise_for_status()
            
            os.makedirs(self.data_dir, exist_ok=True)
            file_path = os.path.join(self.data_dir, filename)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            return self._load_csv_file(file_path, columns)
            
        except Exception as e:
            self.logger.error(f"Error downloading {source_config.name}: {e}")
            return pd.DataFrame(columns=columns)

    def fetch_live_data(self) -> pd.DataFrame:
        self.logger.info("Fetching live flight data from OpenSky...")
        
        try:
            response = requests.get(
                self.data_sources['opensky_live'].url,
                timeout=self.data_sources['opensky_live'].timeout
            )
            response.raise_for_status()
            data = response.json()
            
            if 'states' not in data or not data['states']:
                self.logger.warning("No live flight data available")
                return pd.DataFrame(columns=self.config.LIVE_FLIGHT_COLUMNS)
            
            df = pd.DataFrame(data['states'], columns=self.config.LIVE_FLIGHT_COLUMNS)
            df['fetch_timestamp'] = datetime.now(timezone.utc)
            
            self.logger.info(f"Fetched {len(df)} live flights")
            self.datasets['live_flights'] = df
            return df
            
        except Exception as e:
            self.logger.error(f"Error fetching live data: {e}")
            return pd.DataFrame(columns=self.config.LIVE_FLIGHT_COLUMNS)

    def clean_and_standardize(self) -> Dict[str, pd.DataFrame]:
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

    def _clean_airports(self, df: pd.DataFrame) -> pd.DataFrame:
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
            df['Active'] = df['Active'].map({'Y': True, 'N': False})
        
        return df

    def _clean_routes(self, df: pd.DataFrame) -> pd.DataFrame:
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
        
        df['callsign'] = df['callsign'].str.strip()
        
        return df

    def _apply_smart_imputation(self, df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
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

    def _create_live_flights_detailed_view(self) -> pd.DataFrame:
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
        return 6371 * 2 * np.arcsin(np.sqrt(a))

    def save_to_database(self) -> str:
        self.logger.info("Saving datasets to database")
        
        try:
            with self.db_engine.connect() as conn:
                # Map datasets to Oracle schema
                oracle_datasets = self.prepare_oracle_datasets()
                
                for name, df in oracle_datasets.items():
                    if not df.empty:
                        try:
                            # Try replace first (works for SQLite and new Oracle tables)
                            df.to_sql(name, conn, if_exists='replace', index=False)
                            self.logger.info(f"[OK] Saved {len(df)} records to {name} table (replaced)")
                        except Exception as e:
                            if "ORA-02449" in str(e) or "foreign key" in str(e).lower():
                                # Oracle foreign key constraint - use append mode instead
                                self.logger.warning(f"Foreign key constraint on {name}, trying append mode...")
                                try:
                                    # First truncate the table content (keeps structure)
                                    conn.execute(text(f"TRUNCATE TABLE {name}"))
                                    # Then append new data
                                    df.to_sql(name, conn, if_exists='append', index=False)
                                    self.logger.info(f"[OK] Saved {len(df)} records to {name} table (truncated and appended)")
                                except Exception as e2:
                                    # If truncate fails, just append (will create duplicates but won't fail)
                                    self.logger.warning(f"Truncate failed, appending to {name}: {e2}")
                                    df.to_sql(name, conn, if_exists='append', index=False)
                                    self.logger.info(f"[OK] Saved {len(df)} records to {name} table (appended)")
                            else:
                                raise e
                
                # Skip views for now as they need special Oracle schema handling
                self.logger.info("Skipping views creation for Oracle compatibility")
                
                conn.commit()
            
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
    
    def prepare_oracle_datasets(self):
        """Prepare datasets to match existing Oracle schema"""
        oracle_datasets = {}
        
        # Map airports data to existing Oracle schema
        if 'airports' in self.datasets and not self.datasets['airports'].empty:
            airports_df = self.datasets['airports'].copy()
            
            # Map columns to Oracle schema: AIRPORT_ICAO, AIRPORT_IATA, NAME, CITY, COUNTRY, LATITUDE, LONGITUDE
            column_mapping = {
                'ICAO': 'AIRPORT_ICAO',
                'IATA': 'AIRPORT_IATA', 
                'Name': 'NAME',
                'City': 'CITY',
                'Country': 'COUNTRY',
                'Latitude': 'LATITUDE',
                'Longitude': 'LONGITUDE'
            }
            
            # Only keep columns that exist in both schemas
            available_columns = [col for col in column_mapping.keys() if col in airports_df.columns]
            if available_columns:
                airports_mapped = airports_df[available_columns].copy()
                airports_mapped = airports_mapped.rename(columns=column_mapping)
                
                # Ensure ICAO is not null (primary key) and not empty
                airports_mapped = airports_mapped.dropna(subset=['AIRPORT_ICAO'])
                airports_mapped = airports_mapped[airports_mapped['AIRPORT_ICAO'].str.strip() != '']
                
                if not airports_mapped.empty:
                    oracle_datasets['airports'] = airports_mapped
                    self.logger.info(f"Mapped {len(airports_mapped)} airports to Oracle schema")
        
        # Map airlines data to existing Oracle schema  
        if 'airlines' in self.datasets and not self.datasets['airlines'].empty:
            airlines_df = self.datasets['airlines'].copy()
            
            # Map columns to Oracle schema: AIRLINE_ICAO, NAME, COUNTRY
            column_mapping = {
                'ICAO': 'AIRLINE_ICAO',
                'Name': 'NAME',
                'Country': 'COUNTRY'
            }
            
            available_columns = [col for col in column_mapping.keys() if col in airlines_df.columns]
            if available_columns:
                airlines_mapped = airlines_df[available_columns].copy()
                airlines_mapped = airlines_mapped.rename(columns=column_mapping)
                
                # Ensure ICAO is not null (primary key) and not empty
                airlines_mapped = airlines_mapped.dropna(subset=['AIRLINE_ICAO'])
                airlines_mapped = airlines_mapped[airlines_mapped['AIRLINE_ICAO'].str.strip() != '']
                
                if not airlines_mapped.empty:
                    oracle_datasets['airlines'] = airlines_mapped
                    self.logger.info(f"Mapped {len(airlines_mapped)} airlines to Oracle schema")
        
        # Skip routes and live_flights for now - they need fact table structure
        self.logger.info(f"Prepared {len(oracle_datasets)} datasets for Oracle compatibility")
        return oracle_datasets

    def export_to_csv(self, output_dir: str = None) -> List[str]:
        """Export datasets to CSV files in logs directory"""
        if output_dir is None:
            output_dir = os.path.join(self.logs_dir, "reports")
        
        self.logger.info(f"Exporting datasets to CSV files in {output_dir}")
        
        os.makedirs(output_dir, exist_ok=True)
        exported_files = []
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
        reports_dir = os.path.join(self.logs_dir, "reports")
        os.makedirs(reports_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = os.path.join(reports_dir, f"pipeline_report_{timestamp}.json")
        
        try:
            import json
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, default=str)
            
            self.logger.info(f"[OK] Pipeline report saved -> {os.path.basename(report_file)}")
            return report_file
        except Exception as e:
            self.logger.error(f"Failed to save pipeline report: {e}")
            return ""

    def run_full_pipeline(self, include_live: bool = True) -> Dict:
        """Run the complete ETL pipeline with progress tracking"""
        self.logger.info("=" * 50)
        self.logger.info("STARTING FLIGHT DATA ETL PIPELINE")
        self.logger.info("=" * 50)
        start_time = time.time()
        
        total_steps = 5 if include_live else 4
        current_step = 0
        
        # Step 1: Load static data
        current_step += 1
        self.logger.info(f"[{current_step}/{total_steps}] Loading static flight data...")
        step_start = time.time()
        self.load_static_data()
        step_duration = time.time() - step_start
        self.logger.info(f"[OK] Static data loaded in {step_duration:.1f}s")
        
        # Step 2: Fetch live data (if requested)
        if include_live:
            current_step += 1
            self.logger.info(f"[{current_step}/{total_steps}] Fetching live flight data...")
            step_start = time.time()
            self.fetch_live_data()
            step_duration = time.time() - step_start
            self.logger.info(f"[OK] Live data fetched in {step_duration:.1f}s")
        
        # Step 3: Data cleaning and standardization
        current_step += 1
        self.logger.info(f"[{current_step}/{total_steps}] Cleaning and standardizing data...")
        step_start = time.time()
        self.clean_and_standardize()
        step_duration = time.time() - step_start
        self.logger.info(f"[OK] Data cleaned in {step_duration:.1f}s")
        
        # Step 4: Save to database
        current_step += 1
        self.logger.info(f"[{current_step}/{total_steps}] Saving data to database...")
        step_start = time.time()
        db_path = self.save_to_database()
        step_duration = time.time() - step_start
        self.logger.info(f"[OK] Database saved in {step_duration:.1f}s")
        
        # Step 5: Export to CSV
        current_step += 1
        self.logger.info(f"[{current_step}/{total_steps}] Exporting data to CSV files...")
        step_start = time.time()
        csv_files = self.export_to_csv()
        step_duration = time.time() - step_start
        self.logger.info(f"[OK] CSV files exported in {step_duration:.1f}s")
        
        end_time = time.time()
        total_duration = end_time - start_time
        
        # Generate comprehensive report
        report = {
            'pipeline_duration': total_duration,
            'datasets_processed': list(self.datasets.keys()),
            'record_counts': {name: len(df) for name, df in self.datasets.items()},
            'database_path': db_path,
            'csv_files': csv_files,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'include_live_data': include_live
        }
        
        # Save report to logs folder
        report_file = self.save_pipeline_report(report)
        if report_file:
            report['report_file'] = report_file
        
        # Summary logging
        self.logger.info("=" * 50)
        self.logger.info("PIPELINE COMPLETION SUMMARY")
        self.logger.info("=" * 50)
        self.logger.info(f"[OK] Total duration: {total_duration:.1f} seconds")
        self.logger.info(f"[OK] Datasets processed: {len(self.datasets)}")
        for name, count in report['record_counts'].items():
            self.logger.info(f"  - {name}: {count:,} records")
        self.logger.info(f"[OK] Database: {db_path}")
        self.logger.info(f"[OK] CSV files: {len(csv_files)} exported")
        self.logger.info("=" * 50)
        
        return report

    def get_dataset_summary(self) -> Dict:
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
