select distinct category from aircraft;
select distinct from_airport from flight;
select distinct aircraft_code from flight;
select distinct * from fare;
select distinct flight_code from flight_fare;
select distinct flight.flight_code,aircraft.aircraft_type from flight,aircraft where flight.aircraft_code=aircraft.aircraft_code;
select distinct airline.airline_name,aircraft.aircraft_type from aircraft,airline,flight where flight.aircraft_code=aircraft.aircraft_code and flight.airline_code=airline.airline_code;
