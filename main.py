
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Optional
import warnings
warnings.filterwarnings('ignore')

from core_etl import FlightDataETL
from data_integrator import ExternalDataIntegrator


class FlightDataOrchestrator:
    def __init__(self, data_dir: str = "data"):
        self.etl = FlightDataETL(data_dir)
        self.integrator = ExternalDataIntegrator()
        self.logger = self._setup_logging()

    def _setup_logging(self):
        os.makedirs('logs', exist_ok=True)
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler = logging.FileHandler('logs/flight_orchestrator.log', encoding='utf-8')
        file_handler.setFormatter(formatter)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        return logger

    def run_comprehensive_pipeline(self, include_live: bool = True, include_external: bool = True) -> Dict:
        self.logger.info("Starting comprehensive flight data pipeline (DATABASE ONLY)...")
        start_time = datetime.now(timezone.utc)
        etl_report = self.etl.run_full_pipeline(include_live=include_live)
        if include_external:
            self.logger.info("Integrating external data sources...")
            integrated_datasets = self.integrator.integrate_all_sources(self.etl.datasets)
            self.etl.datasets.update(integrated_datasets)
            enriched_views = self.integrator.create_enriched_views(self.etl.datasets)
            self.logger.info("Saving integrated datasets to database...")
            for name, df in integrated_datasets.items():
                if name not in etl_report.get('datasets_processed', []) and not df.empty:
                    self.etl.datasets[name] = df
            for name, df in enriched_views.items():
                if not df.empty:
                    self.etl.datasets[name] = df
            self.etl.save_to_database()
            integration_report = self.integrator.generate_integration_report(self.etl.datasets)
        else:
            integration_report = {}
        end_time = datetime.now(timezone.utc)
        comprehensive_report = {
            'pipeline_start': start_time.isoformat(),
            'pipeline_end': end_time.isoformat(),
            'total_duration': (end_time - start_time).total_seconds(),
            'etl_report': etl_report,
            'integration_report': integration_report,
            'final_datasets': {name: len(df) for name, df in self.etl.datasets.items()},
            'database_info': str(self.etl.db_engine.url).replace(str(self.etl.db_engine.url.password), "***") if hasattr(self.etl, 'db_engine') else "Database info not available",
            'includes_live_data': include_live,
            'includes_external_data': include_external,
            'csv_exports': False
        }
        os.makedirs('logs', exist_ok=True)
        report_path = os.path.join('logs', f"comprehensive_report_{start_time.strftime('%Y%m%d_%H%M%S')}.json")
        with open(report_path, 'w') as f:
            json.dump(comprehensive_report, f, indent=2, default=str)
        self.logger.info(f"Comprehensive pipeline completed in {comprehensive_report['total_duration']:.2f} seconds")
        self.logger.info(f"All data saved to database - Report: {report_path}")
        return comprehensive_report

    def get_live_flights_for_airport(self, airport_code: str) -> Dict:
        if 'live_flights_detailed' not in self.etl.datasets:
            self.logger.warning("Live flights detailed view not available")
            return {}
        live_detailed = self.etl.datasets['live_flights_detailed']
        if 'nearest_airport' in live_detailed.columns:
            airport_flights = live_detailed[live_detailed['nearest_airport'] == airport_code]
        else:
            airport_flights = live_detailed[live_detailed['callsign'].str.contains(airport_code, na=False)]
        return {
            'airport_code': airport_code,
            'flights_count': len(airport_flights),
            'flights': airport_flights.to_dict('records') if len(airport_flights) > 0 else []
        }

    def get_aircraft_route_info(self, icao24: str) -> Dict:
        aircraft_info = {}
        if 'aircraft_registry' in self.etl.datasets:
            aircraft_registry = self.etl.datasets['aircraft_registry']
            aircraft_match = aircraft_registry[aircraft_registry['icao24'] == icao24]
            if not aircraft_match.empty:
                aircraft_info.update(aircraft_match.iloc[0].to_dict())
        if 'live_flights_enriched' in self.etl.datasets:
            live_enriched = self.etl.datasets['live_flights_enriched']
            flight_match = live_enriched[live_enriched['icao24'] == icao24]
            if not flight_match.empty:
                aircraft_info.update(flight_match.iloc[0].to_dict())
        return aircraft_info

    def get_route_analysis(self, source_airport: str, dest_airport: str) -> Dict:
        analysis = {
            'route': f"{source_airport} -> {dest_airport}",
            'airlines': [],
            'delay_statistics': {},
            'weather_impact': {}
        }
        if 'routes_with_delays' in self.etl.datasets:
            routes_delays = self.etl.datasets['routes_with_delays']
            if 'IATA_source' in routes_delays.columns and 'IATA_dest' in routes_delays.columns:
                route_data = routes_delays[
                    (routes_delays['IATA_source'] == source_airport) & 
                    (routes_delays['IATA_dest'] == dest_airport)
                ]
            else:
                route_data = routes_delays[
                    (routes_delays['SourceAirport'] == source_airport) & 
                    (routes_delays['DestinationAirport'] == dest_airport)
                ]
            if not route_data.empty:
                analysis['airlines'] = route_data['Name_airline'].dropna().unique().tolist()
                analysis['delay_statistics'] = {
                    'avg_departure_delay': route_data['avg_departure_delay'].mean(),
                    'avg_arrival_delay': route_data['avg_arrival_delay'].mean(),
                    'cancellation_rate': route_data['cancellation_rate'].mean(),
                    'total_flights': route_data['flight_count'].sum()
                }
        return analysis

    def get_system_status(self) -> Dict:
        return {
            'datasets_loaded': list(self.etl.datasets.keys()),
            'dataset_summary': self.etl.get_dataset_summary(),
            'database_path': self.etl.db_path,
            'total_records': sum(len(df) for df in self.etl.datasets.values()),
            'last_update': datetime.now(timezone.utc).isoformat()
        }

if __name__ == "__main__":
    orchestrator = FlightDataOrchestrator()
    print("Running comprehensive flight data pipeline (DATABASE ONLY - NO CSV EXPORTS)...")
    report = orchestrator.run_comprehensive_pipeline(include_live=True, include_external=True)
    print(f"\nPipeline completed successfully!")
    print(f"Total datasets: {len(report['final_datasets'])}")
    print(f"Total records: {sum(report['final_datasets'].values())}")
    print(f"Database: {report['database_info']}")
    print(f"Duration: {report['total_duration']:.2f} seconds")
    print(f"CSV Exports: {report.get('csv_exports', 'Not applicable')}")
    print("\nDataset summary (ALL DATA SAVED TO DATABASE):")
    for name, count in report['final_datasets'].items():
        print(f"  {name}: {count:,} records")
    print(f"\nDetailed report saved to: comprehensive_report_*.json")
    print("All transformed flight data is now available in the PostgreSQL database!")
    print("Use SQL queries or database tools to export specific data as needed.")
