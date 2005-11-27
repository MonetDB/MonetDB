select distinct category from aircraft order by category;
select distinct from_airport from flight order by from_airport;
select distinct aircraft_code from flight order by aircraft_code;
select distinct * from fare;
select distinct flight_code from flight_fare order by flight_code;
select distinct flight.flight_code,aircraft.aircraft_type from flight,aircraft where flight.aircraft_code=aircraft.aircraft_code order by flight.flight_code;
select distinct airline.airline_name,aircraft.aircraft_type from aircraft,airline,flight where flight.aircraft_code=aircraft.aircraft_code and flight.airline_code=airline.airline_code order by airline.airline_name, aircraft.aircraft_type;
