select city.city_name,state.state_name,city.city_code from city,state where city.city_code='MATL' and city.state_code=state.state_code;
select city.city_name,state.state_name,city.city_code from state,city where city.state_code=state.state_code;
select month_name.month_name,day_name.day_name from month_name,day_name where month_name.month_number=day_name.day_code;
select month_name.month_name,day_name.day_name from month_name,day_name where month_name.month_number=day_name.day_code and day_name.day_code >= 4;
select flight.flight_code,aircraft.aircraft_type from flight,aircraft where flight.aircraft_code=aircraft.aircraft_code order by flight.flight_code;
