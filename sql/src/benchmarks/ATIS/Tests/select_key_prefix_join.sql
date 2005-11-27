select fare.fare_code from restrict_carrier,airline,fare where restrict_carrier.airline_code=airline.airline_code and fare.restrict_code=restrict_carrier.restrict_code order by fare.fare_code;
