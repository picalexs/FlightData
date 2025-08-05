import pandas as pd
from live_etl import LiveFlightETL
import time

class FlightDataExplorer:
    def __init__(self):
        print("Initializing Flight Data Explorer...")
        self.etl = LiveFlightETL()
        self.current_airport = None
        self.departures = pd.DataFrame()
        self.arrivals = pd.DataFrame()
        
    def select_airport(self, airport_icao):
        self.current_airport = airport_icao.upper()
        print(f"Selected airport: {self.current_airport}")
        self.refresh_data()
        
    def refresh_data(self, hours_back=2):
        if not self.current_airport:
            print("No airport selected. Use select_airport() first.")
            return
            
        print(f"Refreshing data for {self.current_airport}...")
        summary, self.departures, self.arrivals = self.etl.get_airport_summary(
            self.current_airport, hours_back
        )
        
        print("\nCurrent Summary:")
        for key, value in summary.items():
            print(f"  {key}: {value}")
            
    def show_departures(self, limit=10):
        if self.departures.empty:
            print("No departure data available")
            return
            
        print(f"\nRecent Departures from {self.current_airport}:")
        print("-" * 60)
        
        display_cols = ['callsign', 'ArrivalAirportName', 'AirlineName', 'flight_type', 'departure_time']
        available_cols = [col for col in display_cols if col in self.departures.columns]
        
        sample = self.departures[available_cols].head(limit)
        for idx, row in sample.iterrows():
            print(f"Flight {row.get('callsign', 'N/A')}:")
            print(f"  To: {row.get('ArrivalAirportName', 'Unknown')}")
            print(f"  Airline: {row.get('AirlineName', 'Unknown')}")
            print(f"  Type: {row.get('flight_type', 'Unknown')}")
            if 'departure_time' in row:
                print(f"  Departure: {row['departure_time']}")
            print()
            
    def show_arrivals(self, limit=10):
        if self.arrivals.empty:
            print("No arrival data available")
            return
            
        print(f"\nRecent Arrivals to {self.current_airport}:")
        print("-" * 60)
        
        display_cols = ['callsign', 'DepartureAirportName', 'AirlineName', 'flight_type', 'last_seen_time']
        available_cols = [col for col in display_cols if col in self.arrivals.columns]
        
        sample = self.arrivals[available_cols].head(limit)
        for idx, row in sample.iterrows():
            print(f"Flight {row.get('callsign', 'N/A')}:")
            print(f"  From: {row.get('DepartureAirportName', 'Unknown')}")
            print(f"  Airline: {row.get('AirlineName', 'Unknown')}")
            print(f"  Type: {row.get('flight_type', 'Unknown')}")
            if 'last_seen_time' in row:
                print(f"  Last Seen: {row['last_seen_time']}")
            print()
            
    def search_flight(self, callsign):
        if not self.current_airport:
            print("No airport selected. Use select_airport() first.")
            return
            
        print(f"Searching for flight {callsign}...")
        results = self.etl.search_flight_by_callsign(callsign, self.current_airport)
        
        if results.empty:
            print(f"No flights found with callsign containing '{callsign}'")
            return
            
        print(f"\nFound {len(results)} matching flights:")
        print("-" * 60)
        
        for idx, flight in results.iterrows():
            print(f"Flight: {flight.get('callsign', 'N/A')}")
            print(f"  From: {flight.get('DepartureAirportName', 'Unknown')}")
            print(f"  To: {flight.get('ArrivalAirportName', 'Unknown')}")
            print(f"  Airline: {flight.get('AirlineName', 'Unknown')}")
            print(f"  Aircraft: {flight.get('icao24', 'Unknown')}")
            print(f"  Type: {flight.get('data_type', 'Unknown')}")
            if 'route_distance_km' in flight and pd.notna(flight['route_distance_km']):
                print(f"  Distance: {flight['route_distance_km']:.0f} km")
            print()
            
    def show_airline_stats(self):
        if self.departures.empty:
            print("No data available for airline statistics")
            return
            
        print(f"\nAirline Statistics for {self.current_airport}:")
        print("-" * 50)
        
        if 'AirlineName' in self.departures.columns:
            airline_counts = self.departures['AirlineName'].value_counts().head(10)
            print("Top 10 Airlines by Departures:")
            for airline, count in airline_counts.items():
                print(f"  {airline}: {count} flights")
                
    def show_destination_stats(self):
        if self.departures.empty:
            print("No data available for destination statistics")
            return
            
        print(f"\nTop Destinations from {self.current_airport}:")
        print("-" * 50)
        
        if 'ArrivalCity' in self.departures.columns:
            dest_counts = self.departures['ArrivalCity'].value_counts().head(10)
            print("Top 10 Destinations:")
            for dest, count in dest_counts.items():
                print(f"  {dest}: {count} flights")
                
    def interactive_mode(self):
        print("\nFlight Data Explorer - Interactive Mode")
        print("=" * 50)
        print("Commands:")
        print("  airport <ICAO>  - Select airport (e.g., 'airport EDDF')")
        print("  refresh         - Refresh current airport data")
        print("  departures      - Show recent departures")
        print("  arrivals        - Show recent arrivals")
        print("  search <flight> - Search for specific flight")
        print("  airlines        - Show airline statistics")
        print("  destinations    - Show destination statistics")
        print("  quit            - Exit interactive mode")
        print()
        
        while True:
            try:
                command = input("Enter command: ").strip().lower()
                
                if command == 'quit':
                    print("Goodbye!")
                    break
                elif command == 'refresh':
                    self.refresh_data()
                elif command == 'departures':
                    self.show_departures()
                elif command == 'arrivals':
                    self.show_arrivals()
                elif command == 'airlines':
                    self.show_airline_stats()
                elif command == 'destinations':
                    self.show_destination_stats()
                elif command.startswith('airport '):
                    airport = command.split(' ', 1)[1].upper()
                    self.select_airport(airport)
                elif command.startswith('search '):
                    flight = command.split(' ', 1)[1].upper()
                    self.search_flight(flight)
                else:
                    print("Unknown command. Type 'quit' to exit.")
                    
            except KeyboardInterrupt:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"Error: {e}")

def demo_mode():
    explorer = FlightDataExplorer()
    
    airports_to_try = ["EDDF", "EGLL", "KJFK", "KLAX"]
    
    for airport in airports_to_try:
        print(f"\n{'='*60}")
        print(f"Trying airport: {airport}")
        print(f"{'='*60}")
        
        explorer.select_airport(airport)
        
        if not explorer.departures.empty:
            print(f"\nSuccess! Found data for {airport}")
            explorer.show_departures(5)
            explorer.show_airline_stats()
            break
        else:
            print(f"No data available for {airport}")
    
    if explorer.departures.empty:
        print("\nNo live data available for any test airports.")
        print("This might be due to API limitations or timing.")
    else:
        print(f"\nStarting interactive mode with {explorer.current_airport}...")
        time.sleep(2)
        explorer.interactive_mode()

if __name__ == "__main__":
    choice = input("Choose mode - (d)emo or (i)nteractive: ").lower()
    
    if choice.startswith('d'):
        demo_mode()
    else:
        explorer = FlightDataExplorer()
        explorer.interactive_mode()
