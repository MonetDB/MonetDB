START TRANSACTION;

CREATE TABLE sys.v_models_that_satisfy_criteria_dup (
	set_number int,
	set_corr_coef real,
	last_crit int,
	crit_number int,
	traffic_min real,
	traffic_max real,
	crit_corr_coef real,
	min_lift real,
	min_previous_precision real,
	target_value varchar(1024),
	model_number int,
	number_of_scenarios bigint,
	previous_precision real,
	new_precision real,
	lift real,
	total_traffic real,
	corr real,
	corrb real,
	expr1 real,
	expr2 real,
	expr3 real
);

CREATE VIEW v_models_that_satisfy_criteria_min_set_dup AS
SELECT a.*
  FROM v_models_that_satisfy_criteria_dup AS a
  WHERE a.set_number = (
    SELECT MIN(b.set_number)
      FROM v_models_that_satisfy_criteria_dup AS b
      WHERE b.model_number = a.model_number
    );

ROLLBACK;
