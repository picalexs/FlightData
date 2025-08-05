# Flight Data ETL Pipeline

A comprehensive ETL pipeline for processing flight data from multiple sources including OpenFlights datasets, live flight tracking, and external data integration.

## Project Structure

```
FlightData/
├── core_etl.py           # Main ETL pipeline class
├── data_integrator.py    # External data source integration
├── main.py              # Orchestrator and main entry point
├── flight_explorer.py   # Interactive CLI explorer
├── requirements.txt     # Python dependencies
├── data/               # Raw data files
└── output/             # Processed CSV outputs
```

## Features

- **Static Data Loading**: OpenFlights airports, airlines, and routes datasets
- **Live Data Integration**: Real-time flight tracking via OpenSky API
- **External Data Sources**: Weather, aircraft registry, and delay data
- **Advanced Data Cleaning**: ML-based imputation and duplicate removal
- **Master Views**: Enriched datasets with joined information
- **Multiple Output Formats**: SQLite database and CSV exports

## Quick Start

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the complete pipeline:
```bash
python main.py
```

3. Explore data interactively:
```bash
python flight_explorer.py
```

## Core Classes

### FlightDataETL
Main ETL pipeline that handles:
- Loading static OpenFlights data
- Fetching live flight data from OpenSky API
- Data cleaning and standardization
- Creating master views with joins
- Exporting to database and CSV

### ExternalDataIntegrator
Handles integration of external data:
- Synthetic weather data generation
- Aircraft registry information
- Flight delay statistics
- Enriched view creation

### FlightDataOrchestrator
Main orchestrator that:
- Coordinates ETL and integration processes
- Provides query methods for specific use cases
- Generates comprehensive reports
- Manages the complete pipeline execution

## Usage Examples

```python
from main import FlightDataOrchestrator

# Create orchestrator
orchestrator = FlightDataOrchestrator()

# Run complete pipeline
report = orchestrator.run_comprehensive_pipeline()

# Get live flights for airport
flights = orchestrator.get_live_flights_for_airport('LAX')

# Get aircraft information
aircraft = orchestrator.get_aircraft_route_info('abc123')

# Analyze route
route_info = orchestrator.get_route_analysis('LAX', 'JFK')
```

## Database Schema

The pipeline creates these tables:
- `airports` - Airport information with coordinates
- `airlines` - Airline details and metadata
- `routes` - Flight route connections
- `live_flights` - Real-time flight positions
- `weather` - Airport weather data
- `aircraft_registry` - Aircraft specifications
- `flight_delays` - Historical delay statistics

## Output Files

- `comprehensive_flight_data.db` - SQLite database with all data
- `output/*.csv` - Individual CSV files for each dataset
- `comprehensive_report_*.json` - Pipeline execution report
- `flight_orchestrator.log` - Execution logs
