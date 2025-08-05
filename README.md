# Airport Data ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline for airport and aviation data analytics using PostgreSQL as the target database. This project demonstrates professional-grade data engineering practices with real-world aviation data sources.

## 🎯 Project Overview

This ETL pipeline integrates multiple aviation data sources into a comprehensive PostgreSQL data warehouse, enabling advanced airport analytics and business intelligence. The project showcases:

- **Multi-source data integration**: Airports, airlines, flights, weather, and pricing data
- **Dimensional modeling**: Star schema design with fact and dimension tables
- **Target analytics table**: Comprehensive `airport_target` table linking all data sources
- **Production-ready code**: Error handling, logging, monitoring, and data quality checks
- **PostgreSQL optimization**: Indexes, views, and query performance tuning

## 🏗️ Architecture

```
Data Sources → ETL Pipeline → PostgreSQL Data Warehouse → Analytics
     ↓              ↓                    ↓                    ↓
• OpenSky API   • Python ETL      • Dimensional Model   • Business
• Airport Data  • Data Quality    • Target Table        Intelligence  
• Weather API   • Transformations • Performance Indexes • Reporting
• Pricing APIs  • Error Handling  • Analytics Views     • Dashboards
```

## 📊 Database Schema

### Core Tables
- **`airport_target`** - Main analytics table linking all data sources
- **`airports`** - Airport dimension (11,000+ airports worldwide)
- **`airlines`** - Airline dimension with ICAO/IATA codes
- **`flight_departure_fact`** - Flight operations fact table
- **`live_tracking_fact`** - Real-time flight tracking data
- **`weather_data`** - Airport weather conditions
- **`ticket_price_fact`** - Flight pricing analytics

### Key Features
- **Foreign key relationships** ensuring data integrity
- **Performance indexes** on commonly queried columns
- **Analytics view** (`airport_analytics_summary`) for quick insights
- **ETL metadata** tracking data freshness and quality

## 🚀 Quick Start

### 1. Prerequisites
- Python 3.8+
- PostgreSQL 12+ installed and running
- Git

### 2. Installation
```bash
# Clone the repository
git clone <your-repo-url>
cd FlightData

# Install Python dependencies
pip install -r requirements.txt

# Create environment file
touch .env
```

### 3. Database Setup
Run the PostgreSQL init script:
``init.sql``

This will:
- Create the PostgreSQL database and user
- Deploy the comprehensive schema
- Set up all tables, indexes, and views
- Test the connection

### 4. Run the ETL Pipeline
```bash
# Full ETL pipeline
python main.py

# Or run specific components
python core_etl.py  # Core data processing
```

## 📋 Configuration

### Environment Variables (.env)
```env
DB_USER=
DB_PASSWORD=
DB_HOST=
DB_PORT=
DB_NAME=

OPENWEATHER_API_KEY=
AVIATIONSTACK_API_KEY=

# ETL Configuration
ETL_BATCH_SIZE=1000
ETL_LOG_LEVEL=INFO
```

## 🗃️ Data Sources

### Primary Sources
1. **Airport Data**: Comprehensive global airport database
   - 11,000+ airports worldwide
   - ICAO/IATA codes, coordinates, timezone
   - Country, region, and operational status

2. **Airline Data**: Major airline information
   - ICAO/IATA airline codes
   - Airline names and countries
   - Fleet and operational data

3. **Live Flight Data**: Real-time flight tracking (OpenSky Network)
   - Current flights in airspace
   - Position, altitude, velocity
   - Origin/destination tracking

4. **Weather Data**: Airport weather conditions
   - Temperature, humidity, pressure
   - Wind speed and direction
   - Visibility and precipitation

## 📈 Analytics & Reporting

### Target Table Analytics
The `airport_target` table provides comprehensive airport metrics:

```sql
-- Top airports by daily flight volume
SELECT 
    airport_name,
    city,
    country,
    total_flights,
    on_time_percentage,
    avg_delay_minutes
FROM airport_target 
WHERE analysis_date = CURRENT_DATE
ORDER BY total_flights DESC
LIMIT 10;

-- Airport performance trends
SELECT 
    airport_icao,
    airport_name,
    analysis_date,
    total_flights,
    avg_delay_minutes,
    on_time_percentage
FROM airport_target 
WHERE airport_icao = 'KJFK'  -- JFK Airport
  AND analysis_date >= CURRENT_DATE - INTERVAL '30 DAY'
ORDER BY analysis_date;
```

### Pre-built Analytics View
```sql
-- Use the built-in analytics summary
SELECT * FROM airport_analytics_summary
WHERE country = 'United States'
ORDER BY total_flights DESC;
```

## 🔧 Key Features

### ETL Pipeline Features
- **Incremental loading**: Only process new/changed data
- **Data quality checks**: Validation and cleansing rules
- **Error handling**: Comprehensive exception management
- **Monitoring**: Detailed logging and progress tracking
- **Flexible scheduling**: Configurable batch processing

### Database Features
- **ACID compliance**: PostgreSQL transaction safety
- **Scalability**: Optimized for large datasets
- **Performance**: Strategic indexing and query optimization
- **Backup ready**: Standard PostgreSQL backup/restore
- **Security**: User permissions and connection encryption

### Analytics Features
- **Dimensional modeling**: Business-friendly star schema
- **Historical tracking**: Time-series airport performance
- **Data lineage**: ETL metadata and audit trails
- **Flexible querying**: SQL-based analytics and reporting

## 📁 Project Structure

```
FlightData/
├── core_etl.py              # Main ETL processing logic
├── database_connection.py    # Database connection management
├── main.py                   # ETL orchestration
├── setup_postgresql.py      # Database setup automation
├── requirements.txt          # Python dependencies
├── .env  # Environment configuration template
├── data/
│   ├── db/
│   │   └── init.sql         # PostgreSQL schema definition
│   ├── airports/            # Airport data files
│   └── processed/           # ETL processing outputs
└── logs/                    # ETL execution logs
```

## 🔍 Monitoring & Maintenance

### ETL Monitoring
- Check logs in `logs/` directory
- Monitor `airport_target.data_freshness_score`
- Track ETL execution times and batch sizes
