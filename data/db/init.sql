DROP TABLE IF EXISTS flight_departure_fact CASCADE;
DROP TABLE IF EXISTS ticket_price_fact CASCADE;
DROP TABLE IF EXISTS live_tracking_fact CASCADE;
DROP TABLE IF EXISTS airport_target CASCADE;
DROP TABLE IF EXISTS weather_data CASCADE;
DROP TABLE IF EXISTS aircraft_registry CASCADE;
DROP TABLE IF EXISTS airlines CASCADE;
DROP TABLE IF EXISTS airports CASCADE;
DROP TABLE IF EXISTS dates CASCADE;

CREATE TABLE dates (
  date_key     DATE PRIMARY KEY,
  year         INTEGER,
  month        INTEGER,
  day          INTEGER,
  day_name     VARCHAR(10),
  month_name   VARCHAR(10),
  quarter      INTEGER,
  week_of_year INTEGER,
  is_weekend   BOOLEAN,
  is_holiday   BOOLEAN DEFAULT FALSE
);

-- Airlines dimension
CREATE TABLE airlines (
  airline_icao VARCHAR(10) PRIMARY KEY,
  airline_iata VARCHAR(5),
  name         VARCHAR(200),
  country      VARCHAR(100),
  active       BOOLEAN DEFAULT TRUE,
  created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Airports dimension
CREATE TABLE airports (
  airport_icao VARCHAR(10) PRIMARY KEY,
  airport_iata VARCHAR(5),
  name         VARCHAR(200),
  city         VARCHAR(100),
  country      VARCHAR(100),
  latitude     DECIMAL(9,6),
  longitude    DECIMAL(9,6),
  altitude     INTEGER,
  timezone     VARCHAR(50),
  continent    VARCHAR(50),
  region       VARCHAR(100),
  created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Aircraft registry dimension
CREATE TABLE aircraft_registry (
  icao24        VARCHAR(10) PRIMARY KEY,
  registration  VARCHAR(20),
  aircraft_type VARCHAR(100),
  manufacturer  VARCHAR(100),
  model         VARCHAR(100),
  year_built    INTEGER,
  owner         VARCHAR(200),
  operator      VARCHAR(200),
  created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Weather data dimension
CREATE TABLE weather_data (
  id              SERIAL PRIMARY KEY,
  airport_icao    VARCHAR(10) NOT NULL,
  date_key        DATE NOT NULL,
  temperature     DECIMAL(5,2),
  humidity        INTEGER,
  pressure        DECIMAL(8,2),
  wind_speed      DECIMAL(6,2),
  wind_direction  INTEGER,
  visibility      DECIMAL(6,2),
  weather_condition VARCHAR(100),
  precipitation   DECIMAL(6,2),
  created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (airport_icao) REFERENCES airports (airport_icao),
  FOREIGN KEY (date_key) REFERENCES dates (date_key),
  UNIQUE(airport_icao, date_key)
);

-- Fact Tables
-- Flight departure facts
CREATE TABLE flight_departure_fact (
  departure_id             SERIAL PRIMARY KEY,
  icao24                   VARCHAR(10),
  callsign                 VARCHAR(20),
  airline_icao             VARCHAR(10),
  departure_airport_icao   VARCHAR(10),
  arrival_airport_icao     VARCHAR(10),
  departure_time           TIMESTAMP,
  arrival_time             TIMESTAMP,
  date_key                 DATE,
  price_estimate           DECIMAL(12,2),
  currency                 VARCHAR(10),
  duration_minutes         INTEGER,
  distance_km              DECIMAL(10,2),
  delay_minutes            INTEGER DEFAULT 0,
  status                   VARCHAR(50) DEFAULT 'scheduled',
  created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (airline_icao) REFERENCES airlines (airline_icao),
  FOREIGN KEY (departure_airport_icao) REFERENCES airports (airport_icao),
  FOREIGN KEY (arrival_airport_icao) REFERENCES airports (airport_icao),
  FOREIGN KEY (date_key) REFERENCES dates (date_key),
  FOREIGN KEY (icao24) REFERENCES aircraft_registry (icao24)
);

-- Live flight tracking facts
CREATE TABLE live_tracking_fact (
  snapshot_id        SERIAL PRIMARY KEY,
  icao24             VARCHAR(10),
  callsign           VARCHAR(20),
  origin_country     VARCHAR(100),
  latitude           DECIMAL(9,6),
  longitude          DECIMAL(9,6),
  altitude           DECIMAL(10,2),
  velocity           DECIMAL(10,2),
  heading            DECIMAL(5,2),
  on_ground          BOOLEAN,
  timestamp_observed TIMESTAMP,
  loaded_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  date_key           DATE,
  nearest_airport    VARCHAR(10),
  FOREIGN KEY (date_key) REFERENCES dates (date_key),
  FOREIGN KEY (icao24) REFERENCES aircraft_registry (icao24),
  FOREIGN KEY (nearest_airport) REFERENCES airports (airport_icao)
);

-- Ticket price facts
CREATE TABLE ticket_price_fact (
  price_id         SERIAL PRIMARY KEY,
  callsign         VARCHAR(20),
  route            VARCHAR(100),
  departure_airport VARCHAR(10),
  arrival_airport   VARCHAR(10),
  best_price       DECIMAL(12,2),
  currency         VARCHAR(10),
  duration_minutes INTEGER,
  fetched_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  date_key         DATE,
  booking_class    VARCHAR(20),
  advance_days     INTEGER,
  FOREIGN KEY (date_key) REFERENCES dates (date_key),
  FOREIGN KEY (departure_airport) REFERENCES airports (airport_icao),
  FOREIGN KEY (arrival_airport) REFERENCES airports (airport_icao)
);

-- TARGET TABLE: Comprehensive Airport Analytics
CREATE TABLE airport_target (
  id                      SERIAL PRIMARY KEY,
  airport_icao            VARCHAR(10) NOT NULL,
  analysis_date           DATE NOT NULL,
  
  -- Basic airport info
  airport_name            VARCHAR(200),
  city                    VARCHAR(100),
  country                 VARCHAR(100),
  latitude                DECIMAL(9,6),
  longitude               DECIMAL(9,6),
  
  -- Daily flight statistics
  total_departures        INTEGER DEFAULT 0,
  total_arrivals          INTEGER DEFAULT 0,
  total_flights           INTEGER DEFAULT 0,
  avg_delay_minutes       DECIMAL(8,2) DEFAULT 0,
  cancellation_rate       DECIMAL(5,4) DEFAULT 0,
  
  -- Aircraft statistics
  unique_aircraft_count   INTEGER DEFAULT 0,
  most_common_aircraft    VARCHAR(100),
  
  -- Airline statistics
  unique_airlines_count   INTEGER DEFAULT 0,
  top_airline             VARCHAR(200),
  top_airline_flights     INTEGER DEFAULT 0,
  
  -- Route statistics
  unique_routes_count     INTEGER DEFAULT 0,
  top_destination         VARCHAR(100),
  top_destination_flights INTEGER DEFAULT 0,
  
  -- Weather conditions
  avg_temperature         DECIMAL(5,2),
  avg_humidity            INTEGER,
  avg_wind_speed          DECIMAL(6,2),
  weather_condition       VARCHAR(100),
  
  -- Pricing statistics
  avg_ticket_price        DECIMAL(12,2),
  min_ticket_price        DECIMAL(12,2),
  max_ticket_price        DECIMAL(12,2),
  price_currency          VARCHAR(10),
  
  -- Performance metrics
  on_time_percentage      DECIMAL(5,2) DEFAULT 0,
  avg_flight_duration     DECIMAL(8,2),
  passenger_load_factor   DECIMAL(5,2),
  
  -- ETL metadata
  data_freshness_score    DECIMAL(3,2) DEFAULT 1.0,
  sources_count           INTEGER DEFAULT 1,
  last_updated            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  etl_batch_id            VARCHAR(50),
  
  FOREIGN KEY (airport_icao) REFERENCES airports (airport_icao),
  FOREIGN KEY (analysis_date) REFERENCES dates (date_key),
  UNIQUE(airport_icao, analysis_date)
);

-- Indexes for performance
CREATE INDEX idx_airport_target_date ON airport_target (analysis_date);
CREATE INDEX idx_airport_target_icao ON airport_target (airport_icao);
CREATE INDEX idx_airport_target_country ON airport_target (country);
CREATE INDEX idx_flight_departure_date ON flight_departure_fact (date_key);
CREATE INDEX idx_flight_departure_airport ON flight_departure_fact (departure_airport_icao);
CREATE INDEX idx_live_tracking_timestamp ON live_tracking_fact (timestamp_observed);
CREATE INDEX idx_weather_data_date ON weather_data (date_key);

-- Create a view for easy target analysis
CREATE OR REPLACE VIEW airport_analytics_summary AS
SELECT 
  at.airport_icao,
  at.airport_name,
  at.city,
  at.country,
  at.analysis_date,
  at.total_flights,
  at.avg_delay_minutes,
  at.on_time_percentage,
  at.unique_airlines_count,
  at.top_airline,
  at.avg_ticket_price,
  at.weather_condition,
  at.data_freshness_score,
  at.last_updated
FROM airport_target at
WHERE at.analysis_date >= CURRENT_DATE - INTERVAL '30' DAY
ORDER BY at.total_flights DESC, at.analysis_date DESC;