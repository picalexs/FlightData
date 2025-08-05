import pandas as pd
import numpy as np
import requests
import json
import time
import os
import random
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from real_apis import RealDataAPIs
warnings.filterwarnings('ignore')

load_dotenv()

class ExternalDataIntegrator:
    def fetch_real_aircraft_data(self, icao24: str) -> dict:
        """Fetch real aircraft data from multiple APIs"""
        aviationstack_data = self._fetch_aviationstack_aircraft(icao24)
        if aviationstack_data:
            return aviationstack_data

        opensky_data = self._fetch_opensky_aircraft(icao24)
        if opensky_data:
            return opensky_data
            
        return None
    
    def _fetch_aviationstack_aircraft(self, icao24: str) -> dict:
        """Fetch from AviationStack API"""
        api_key = os.getenv('AVIATIONSTACK_API_KEY')
        if not api_key:
            return None
        url = f'http://api.aviationstack.com/v1/aircrafts?icao24={icao24}&access_key={api_key}'
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get('data') and len(data['data']) > 0:
                    ac = data['data'][0]
                    return {
                        'icao24': icao24,
                        'registration': ac.get('registration_number'),
                        'aircraft_type': ac.get('type_code'),
                        'manufacturer': ac.get('manufacturer'),
                        'model': ac.get('model'),
                        'year_built': ac.get('built'),
                        'owner': ac.get('owner'),
                        'operator': ac.get('operator'),
                        'data_source': 'aviationstack_api',
                    }
        except Exception as e:
            self.logger.debug(f"AviationStack API error for {icao24}: {e}")
        return None
    
    def _fetch_opensky_aircraft(self, icao24: str) -> dict:
        """Fetch from OpenSky Network aircraft database (free)"""
        try:
            url = f'https://opensky-network.org/api/metadata/aircraft/icao/{icao24}'
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data:
                    return {
                        'icao24': icao24,
                        'registration': data.get('registration'),
                        'aircraft_type': data.get('typecode'),
                        'manufacturer': data.get('manufacturername'),
                        'model': data.get('model'),
                        'year_built': data.get('built'),
                        'owner': data.get('owner'),
                        'operator': data.get('operator'),
                        'data_source': 'opensky'
                    }
        except Exception as e:
            self.logger.debug(f"OpenSky API error for {icao24}: {e}")
        return None
    def __init__(self):
        self.logger = self._setup_logging()
        self.weather_cache = {}
        self.aircraft_cache = {}
        self.delay_cache = {}
        self.OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
        self.real_apis = RealDataAPIs()
    
    def _setup_logging(self):
        logs_dir = 'logs'
        os.makedirs(logs_dir, exist_ok=True)
        
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        logger.handlers.clear()
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        file_handler = logging.FileHandler(
            os.path.join(logs_dir, 'data_integration.log'), 
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

    def collect_weather_data(self, airports_df: pd.DataFrame, days_back: int = 7) -> pd.DataFrame:
        self.logger.info(f"Collecting weather data for {len(airports_df)} airports...")
        
        max_airports = 100
        if len(airports_df) > max_airports:
            airports_sample = airports_df.sample(n=max_airports, random_state=42)
            self.logger.info(f"Sampling {max_airports} airports for weather data collection")
        else:
            airports_sample = airports_df
        
        weather_data = []
        processed_count = 0
        
        def fetch_weather_for_airport(airport_row):
            try:
                airport_code = airport_row.get('IATA', airport_row.get('ICAO', 'UNKNOWN'))
                lat = airport_row.get('Latitude')
                lon = airport_row.get('Longitude')
                
                if pd.notna(lat) and pd.notna(lon):
                    if self.OPENWEATHER_API_KEY:
                        real_weather = self._fetch_real_weather_data(airport_code, lat, lon, days_back)
                        if real_weather:
                            return real_weather
                    
                    # Fallback to synthetic data
                    return self._generate_weather_data(airport_code, lat, lon, days_back)
                return []
            except Exception as e:
                self.logger.warning(f"Error processing weather for airport {airport_code}: {e}")
                return []
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_airport = {
                executor.submit(fetch_weather_for_airport, row): idx 
                for idx, row in airports_sample.iterrows()
            }
            
            total_airports = len(future_to_airport)
            for future in as_completed(future_to_airport):
                try:
                    weather = future.result()
                    if weather:
                        weather_data.extend(weather)
                    
                    processed_count += 1
                    if processed_count % 10 == 0 or processed_count == total_airports:
                        progress_pct = (processed_count / total_airports) * 100
                        self.logger.info(f"Weather data progress: {processed_count}/{total_airports} airports ({progress_pct:.1f}%)")
                        
                except Exception as e:
                    self.logger.error(f"Error in weather collection thread: {e}")
        
        if weather_data:
            weather_df = pd.DataFrame(weather_data)
            self.logger.info(f"Successfully collected weather data: {len(weather_df)} airport-days total")
            return weather_df
        else:
            self.logger.warning("No weather data collected, generating minimal synthetic data")
            return self._generate_fallback_weather_data(airports_sample, days_back)
    
    def _fetch_real_weather_data(self, airport_code: str, lat: float, lon: float, days_back: int) -> List[Dict]:
        """Fetch real weather data from OpenWeatherMap API"""
        weather_data = []
        
        try:
            url = "http://api.openweathermap.org/data/2.5/weather"
            params = {
                'lat': lat,
                'lon': lon,
                'appid': self.OPENWEATHER_API_KEY,
                'units': 'metric'
            }
            
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                
                weather_data.append({
                    'airport_code': airport_code,
                    'date': datetime.now(timezone.utc).date(),
                    'latitude': lat,
                    'longitude': lon,
                    'temperature_c': round(data['main']['temp'], 1),
                    'humidity_percent': data['main']['humidity'],
                    'wind_speed_kmh': round(data.get('wind', {}).get('speed', 0) * 3.6, 1),
                    'precipitation_mm': data.get('rain', {}).get('1h', 0),
                    'visibility_km': round(data.get('visibility', 10000) / 1000, 1),
                    'condition': data['weather'][0]['main'],
                    'data_source': 'openweather_api'
                })
                
                for day in range(1, days_back):
                    base_temp = data['main']['temp']
                    weather_data.append({
                        'airport_code': airport_code,
                        'date': (datetime.now(timezone.utc) - timedelta(days=day)).date(),
                        'latitude': lat,
                        'longitude': lon,
                        'temperature_c': round(base_temp + np.random.normal(0, 5), 1),
                        'humidity_percent': max(10, min(100, data['main']['humidity'] + np.random.randint(-20, 20))),
                        'wind_speed_kmh': round(max(0, data.get('wind', {}).get('speed', 10) * 3.6 + np.random.normal(0, 5)), 1),
                        'precipitation_mm': max(0, np.random.exponential(2) if np.random.random() < 0.3 else 0),
                        'visibility_km': round(np.random.uniform(5, 50), 1),
                        'condition': np.random.choice(['Clear', 'Cloudy', 'Rain', 'Snow'], p=[0.4, 0.3, 0.2, 0.1]),
                        'data_source': 'openweather_derived'
                    })
                
                time.sleep(0.2)  # Rate limiting
                return weather_data
                
        except Exception as e:
            self.logger.warning(f"Failed to fetch real weather for {airport_code}: {e}")
        
        return []

    def _generate_weather_data(self, airport_code: str, lat: float, lon: float, days_back: int) -> List[Dict]:
        weather_data = []
        base_date = datetime.now(timezone.utc) - timedelta(days=days_back)
        
        for day in range(days_back):
            date = base_date + timedelta(days=day)
            
            temperature = np.random.normal(15, 20)
            humidity = np.random.uniform(30, 90)
            wind_speed = np.random.exponential(10)
            precipitation = np.random.exponential(2) if np.random.random() < 0.3 else 0
            visibility = np.random.uniform(5, 50)
            
            conditions = ['Clear', 'Cloudy', 'Rainy', 'Foggy', 'Snowy']
            condition_weights = [0.4, 0.3, 0.15, 0.1, 0.05]
            condition = np.random.choice(conditions, p=condition_weights)
            
            weather_data.append({
                'airport_code': airport_code,
                'date': date.date(),
                'latitude': lat,
                'longitude': lon,
                'temperature_c': round(temperature, 1),
                'humidity_percent': round(humidity, 1),
                'wind_speed_kmh': round(wind_speed, 1),
                'precipitation_mm': round(precipitation, 1),
                'visibility_km': round(visibility, 1),
                'condition': condition,
                'data_source': 'synthetic_weather'
            })
        
        return weather_data

    def _generate_fallback_weather_data(self, airports_df: pd.DataFrame, days_back: int) -> pd.DataFrame:
        """Generate minimal synthetic weather data as last resort"""
        fallback_data = []
        
        for _, airport in airports_df.head(10).iterrows():  # Just top 10 for fallback
            airport_code = airport.get('IATA', airport.get('ICAO', 'UNKNOWN'))
            lat = airport.get('Latitude', 0)
            
            for i in range(min(days_back, 3)):  # Limit fallback days
                date = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
                
                fallback_data.append({
                    'airport_code': airport_code,
                    'date': date,
                    'temperature_c': 20 + random.uniform(-10, 15),
                    'humidity_percent': random.uniform(40, 80),
                    'pressure_hpa': 1013 + random.uniform(-20, 20),
                    'wind_speed_kmh': random.uniform(5, 25),
                    'wind_direction_deg': random.uniform(0, 360),
                    'weather_condition': random.choice(['Clear', 'Clouds', 'Rain']),
                    'visibility_km': random.uniform(8, 20),
                    'data_source': 'synthetic_fallback'
                })
        
        self.logger.info(f"Generated {len(fallback_data)} fallback weather records")
        return pd.DataFrame(fallback_data)

    def collect_aircraft_registry_data(self, live_flights_df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(f"Collecting aircraft registry data for {len(live_flights_df)} flights...")
        
        unique_icao24 = live_flights_df['icao24'].dropna().unique()
        limited_icao24 = unique_icao24[:1000]
        self.logger.info(f"Processing {len(limited_icao24)} unique aircraft registrations with multithreading...")
        
        aircraft_data = []
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_icao = {executor.submit(self._generate_aircraft_data, icao24): icao24 
                             for icao24 in limited_icao24}
            
            for i, future in enumerate(as_completed(future_to_icao), 1):
                try:
                    aircraft_info = future.result()
                    aircraft_data.append(aircraft_info)
                    
                    if i % 100 == 0:
                        self.logger.info(f"Processed {i}/{len(limited_icao24)} aircraft registrations ({i/len(limited_icao24)*100:.1f}%)")
                        
                except Exception as e:
                    icao24 = future_to_icao[future]
                    self.logger.warning(f"Failed to process aircraft {icao24}: {e}")
        
        if aircraft_data:
            aircraft_df = pd.DataFrame(aircraft_data)
            self.logger.info(f"Collected aircraft data for {len(aircraft_df)} aircraft")
            return aircraft_df
        else:
            return pd.DataFrame()

    def _generate_aircraft_data(self, icao24: str) -> dict:
        real_data = self.fetch_real_aircraft_data(icao24)
        if real_data:
            return real_data
            
        enhanced_data = self.real_apis.fetch_enhanced_aircraft_data(icao24)
        if enhanced_data:
            return enhanced_data
        
        country_patterns = {
            '3': 'United States', '4': 'United States', '7': 'United States',
            '0': 'United Kingdom', '1': 'United Kingdom', '2': 'United Kingdom',
            '5': 'Canada', 'C': 'Canada',
            'A': 'United States', 'B': 'China',
            'D': 'Germany', 'F': 'France', 'G': 'United Kingdom',
            'H': 'Hungary', 'I': 'Italy', 'J': 'Japan',
            'P': 'Netherlands', 'S': 'Sweden'
        }
        
        first_char = icao24[0].upper() if icao24 else '4'
        country = country_patterns.get(first_char, 'Unknown')
        
        common_aircraft = [
            {'type': 'Boeing 737-800', 'manufacturer': 'Boeing', 'typical_year': (2000, 2020), 'passengers': (162, 189)},
            {'type': 'Airbus A320', 'manufacturer': 'Airbus', 'typical_year': (1995, 2022), 'passengers': (140, 180)},
            {'type': 'Boeing 777-200', 'manufacturer': 'Boeing', 'typical_year': (1995, 2015), 'passengers': (300, 400)},
            {'type': 'Airbus A330-200', 'manufacturer': 'Airbus', 'typical_year': (1998, 2018), 'passengers': (250, 350)},
            {'type': 'Boeing 787-8', 'manufacturer': 'Boeing', 'typical_year': (2011, 2023), 'passengers': (210, 250)},
            {'type': 'Embraer E190', 'manufacturer': 'Embraer', 'typical_year': (2004, 2020), 'passengers': (96, 114)},
            {'type': 'ATR 72-600', 'manufacturer': 'ATR', 'typical_year': (2010, 2023), 'passengers': (68, 78)},
            {'type': 'Bombardier CRJ900', 'manufacturer': 'Bombardier', 'typical_year': (2003, 2020), 'passengers': (76, 90)}
        ]
        
        aircraft_info = np.random.choice(common_aircraft)
        year_range = aircraft_info['typical_year']
        passenger_range = aircraft_info['passengers']
        
        registration_patterns = {
            'United States': lambda: f"N{np.random.randint(100, 999)}{np.random.choice(['AA', 'AB', 'AC', 'AD', 'AE', 'AF', 'AG', 'AH', 'AI', 'AJ'])}",
            'United Kingdom': lambda: f"G-{np.random.choice(['A', 'B', 'C', 'D', 'E', 'F'])}{np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))}{np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))}{np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))}",
            'Germany': lambda: f"D-{np.random.choice(['A', 'B', 'C'])}{np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))}{np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))}{np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))}",
            'Canada': lambda: f"C-{np.random.choice(['F', 'G'])}{np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))}{np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))}{np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))}",
            'France': lambda: f"F-{np.random.choice(['H', 'G', 'O'])}{np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))}{np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))}{np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ'))}",
        }
        
        registration_func = registration_patterns.get(country, lambda: f"{np.random.choice(['N', 'D-', 'G-', 'F-', 'C-'])}{np.random.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'), size=4)}")
        registration = registration_func() if callable(registration_func) else registration_func
        
        major_operators = [
            'American Airlines', 'Delta Air Lines', 'United Airlines', 'Southwest Airlines',
            'Lufthansa', 'British Airways', 'Air France', 'KLM', 'Emirates', 'Qatar Airways',
            'Singapore Airlines', 'Cathay Pacific', 'Air Canada', 'Turkish Airlines',
            'Ryanair', 'EasyJet', 'JetBlue Airways', 'Alaska Airlines', 'Spirit Airlines'
        ]
        
        return {
            'icao24': icao24,
            'registration': registration,
            'aircraft_type': aircraft_info['type'],
            'manufacturer': aircraft_info['manufacturer'],
            'model': aircraft_info['type'].split(' ', 1)[1] if ' ' in aircraft_info['type'] else aircraft_info['type'],
            'year_built': np.random.randint(year_range[0], year_range[1] + 1),
            'owner': np.random.choice(major_operators),
            'operator': np.random.choice(major_operators),
            'data_source': 'enhanced_synthetic'
        }

    def collect_delay_data(self, routes_df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info(f"Collecting flight delay data for routes...")
        
        sample_routes = routes_df.sample(min(1000, len(routes_df))) if len(routes_df) > 1000 else routes_df
        self.logger.info(f"Processing delay data for {len(sample_routes)} routes with multithreading...")
        
        delay_data = []
        
        with ThreadPoolExecutor(max_workers=8) as executor:
            future_to_route = {
                executor.submit(
                    self._generate_delay_data,
                    row.get('SourceAirportID'), 
                    row.get('DestinationAirportID'),
                    row.get('AirlineID')
                ): idx for idx, row in sample_routes.iterrows()
            }
            
            for i, future in enumerate(as_completed(future_to_route), 1):
                try:
                    delays = future.result()
                    delay_data.extend(delays)
                    
                    if i % 100 == 0:
                        self.logger.info(f"Processed {i}/{len(sample_routes)} routes for delay data ({i/len(sample_routes)*100:.1f}%)")
                        
                except Exception as e:
                    route_idx = future_to_route[future]
                    self.logger.warning(f"Failed to process delay data for route {route_idx}: {e}")
        
        if delay_data:
            delay_df = pd.DataFrame(delay_data)
            self.logger.info(f"Collected delay data for {len(delay_df)} route-days")
            return delay_df
        else:
            return pd.DataFrame()

    def _generate_delay_data(self, source_airport_id: int, dest_airport_id: int, airline_id: int) -> List[Dict]:
        delay_data = []
        base_date = datetime.now(timezone.utc) - timedelta(days=30)
        
        for day in range(30):
            date = base_date + timedelta(days=day)
            
            num_flights = np.random.poisson(5)
            
            for flight_num in range(num_flights):
                departure_delay = max(0, np.random.normal(15, 30))
                arrival_delay = max(0, departure_delay + np.random.normal(5, 15))
                
                cancellation_prob = 0.02
                is_cancelled = np.random.random() < cancellation_prob
                
                delay_reasons = ['Weather', 'Air Traffic', 'Mechanical', 'Crew', 'Other']
                delay_reason = np.random.choice(delay_reasons) if departure_delay > 15 else 'On Time'
                
                delay_data.append({
                    'date': date.date(),
                    'source_airport_id': source_airport_id,
                    'destination_airport_id': dest_airport_id,
                    'airline_id': airline_id,
                    'flight_number': f"{airline_id}{np.random.randint(100, 9999)}",
                    'departure_delay_minutes': round(departure_delay, 1),
                    'arrival_delay_minutes': round(arrival_delay, 1),
                    'is_cancelled': is_cancelled,
                    'delay_reason': delay_reason,
                    'data_source': 'synthetic_delay_data'
                })
        
        return delay_data

    def integrate_all_sources(self, base_datasets: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        self.logger.info("Integrating all external data sources with base datasets...")
        
        integrated_datasets = base_datasets.copy()
        
        if 'airports' in base_datasets and not base_datasets['airports'].empty:
            weather_data = self.collect_weather_data(base_datasets['airports'])
            if not weather_data.empty:
                integrated_datasets['weather'] = weather_data
        
        if 'live_flights' in base_datasets and not base_datasets['live_flights'].empty:
            aircraft_data = self.collect_aircraft_registry_data(base_datasets['live_flights'])
            if not aircraft_data.empty:
                integrated_datasets['aircraft_registry'] = aircraft_data
        
        if 'routes' in base_datasets and not base_datasets['routes'].empty:
            delay_data = self.collect_delay_data(base_datasets['routes'])
            if not delay_data.empty:
                integrated_datasets['flight_delays'] = delay_data
        
        self.logger.info(f"Integration complete. Total datasets: {len(integrated_datasets)}")
        return integrated_datasets

    def create_enriched_views(self, datasets: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        self.logger.info("Creating enriched views with external data...")
        
        enriched_views = {}
        
        if all(key in datasets for key in ['live_flights', 'aircraft_registry']):
            enriched_views['live_flights_enriched'] = self._create_enriched_live_flights(
                datasets['live_flights'], 
                datasets['aircraft_registry']
            )
        
        if all(key in datasets for key in ['airports', 'weather']):
            enriched_views['airports_with_weather'] = self._create_airports_with_weather(
                datasets['airports'],
                datasets['weather']
            )
        
        if all(key in datasets for key in ['routes', 'flight_delays']):
            enriched_views['routes_with_delays'] = self._create_routes_with_delays(
                datasets['routes'],
                datasets['flight_delays']
            )
        
        return enriched_views

    def _create_enriched_live_flights(self, live_flights: pd.DataFrame, aircraft_registry: pd.DataFrame) -> pd.DataFrame:
        return live_flights.merge(
            aircraft_registry,
            on='icao24',
            how='left'
        )

    def _create_airports_with_weather(self, airports: pd.DataFrame, weather: pd.DataFrame) -> pd.DataFrame:
        latest_weather = weather.sort_values('date').groupby('airport_code').tail(1)
        
        weather_cols_to_rename = {
            'latitude': 'weather_latitude',
            'longitude': 'weather_longitude'
        }
        latest_weather = latest_weather.rename(columns=weather_cols_to_rename)
        
        return airports.merge(
            latest_weather,
            left_on='IATA',
            right_on='airport_code',
            how='left'
        )

    def _create_routes_with_delays(self, routes: pd.DataFrame, delays: pd.DataFrame) -> pd.DataFrame:
        delay_summary = delays.groupby(['source_airport_id', 'destination_airport_id', 'airline_id']).agg({
            'departure_delay_minutes': ['mean', 'std', 'count'],
            'arrival_delay_minutes': ['mean', 'std'],
            'is_cancelled': 'mean'
        }).round(2)
        
        delay_summary.columns = [
            'avg_departure_delay', 'std_departure_delay', 'flight_count',
            'avg_arrival_delay', 'std_arrival_delay', 'cancellation_rate'
        ]
        delay_summary = delay_summary.reset_index()
        
        return routes.merge(
            delay_summary,
            left_on=['SourceAirportID', 'DestinationAirportID', 'AirlineID'],
            right_on=['source_airport_id', 'destination_airport_id', 'airline_id'],
            how='left'
        )

    def generate_integration_report(self, datasets: Dict[str, pd.DataFrame]) -> Dict:
        report = {
            'integration_timestamp': datetime.now(timezone.utc).isoformat(),
            'datasets_integrated': {},
            'total_external_records': 0
        }
        
        external_datasets = ['weather', 'aircraft_registry', 'flight_delays']
        
        for dataset_name in external_datasets:
            if dataset_name in datasets:
                df = datasets[dataset_name]
                report['datasets_integrated'][dataset_name] = {
                    'record_count': len(df),
                    'columns': list(df.columns),
                    'data_sources': df.get('data_source', pd.Series()).unique().tolist()
                }
                report['total_external_records'] += len(df)
        
        enriched_views = self.create_enriched_views(datasets)
        report['enriched_views_created'] = list(enriched_views.keys())
        
        return report
