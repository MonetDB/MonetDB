INSERT INTO experiment
SELECT tick,
  (next(payload) - payload)/ CAST(next(tick)-tick AS MINUTE)
FROM timeseries;
