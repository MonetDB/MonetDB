start transaction;

CREATE TABLE "time" (
  "data_id" BIGINT NOT NULL,
  "header_id" BIGINT NOT NULL,
  "data_item" VARCHAR(24) NOT NULL,
  "opr_date" DATE NOT NULL,
  "opr_hr" INT NOT NULL,
  "opr_min" INT NOT NULL,
  "yyyymmddhh" BIGINT NOT NULL,
  "interval_num" INT NOT NULL,
  "svalue" VARCHAR(10) NOT NULL,
  "years" SMALLINT NOT NULL,
  "months" SMALLINT NOT NULL,
  "yyyymm" varchar(7) NOT NULL,
  "quarter" SMALLINT NOT NULL
);

INSERT INTO time( data_id, header_id, data_item, opr_date, opr_hr, opr_min, yyyymmddhh, interval_num, svalue, years, months, yyyymm, "quarter")
    VALUES
    (100, 14, 'ATL_PEAK_ON_OFF_FLG', '2009-04-01', 1, 0, 2009040101, 1, 'OFF', 2009, 4, '2009-04', 2 );
INSERT INTO time( data_id, header_id, data_item, opr_date, opr_hr, opr_min, yyyymmddhh, interval_num, svalue, years, months, yyyymm, "quarter")
    VALUES
    (101, 14, 'ATL_PEAK_ON_OFF_FLG', '2009-04-01', 2, 0, 2009040102, 2, 'OFF', 2009, 4, '2009-04', 2 );
INSERT INTO time( data_id, header_id, data_item, opr_date, opr_hr, opr_min, yyyymmddhh, interval_num, svalue, years, months, yyyymm, "quarter")
    VALUES
    (102, 14, 'ATL_PEAK_ON_OFF_FLG', '2009-04-01', 3, 0, 2009040103, 3, 'OFF', 2009, 4, '2009-04', 2 );
INSERT INTO time( data_id, header_id, data_item, opr_date, opr_hr, opr_min, yyyymmddhh, interval_num, svalue, years, months, yyyymm, "quarter")
    VALUES
    (103, 14, 'ATL_PEAK_ON_OFF_FLG', '2009-04-01', 4, 0, 2009040104, 4, 'OFF', 2009, 4, '2009-04', 2 );
INSERT INTO time( data_id, header_id, data_item, opr_date, opr_hr, opr_min, yyyymmddhh, interval_num, svalue, years, months, yyyymm, "quarter")
    VALUES
    (104, 14, 'ATL_PEAK_ON_OFF_FLG', '2009-04-01', 5, 0, 2009040105, 5, 'OFF', 2009, 4, '2009-04', 2 );
INSERT INTO time( data_id, header_id, data_item, opr_date, opr_hr, opr_min, yyyymmddhh, interval_num, svalue, years, months, yyyymm, "quarter")
    VALUES
    (105, 14, 'ATL_PEAK_ON_OFF_FLG', '2009-04-01', 6, 0, 2009040106, 6, 'OFF', 2009, 4, '2009-04', 2 );
INSERT INTO time( data_id, header_id, data_item, opr_date, opr_hr, opr_min, yyyymmddhh, interval_num, svalue, years, months, yyyymm, "quarter")
    VALUES
    (106, 14, 'ATL_PEAK_ON_OFF_FLG', '2009-04-01', 7, 0, 2009040107, 7, 'ON', 2009, 4, '2009-04', 2 );
INSERT INTO time( data_id, header_id, data_item, opr_date, opr_hr, opr_min, yyyymmddhh, interval_num, svalue, years, months, yyyymm, "quarter")
    VALUES
    (107, 14, 'ATL_PEAK_ON_OFF_FLG', '2009-04-01', 8, 0, 2009040108, 8, 'ON', 2009, 4, '2009-04', 2 );
INSERT INTO time( data_id, header_id, data_item, opr_date, opr_hr, opr_min, yyyymmddhh, interval_num, svalue, years, months, yyyymm, "quarter")
    VALUES
    (108, 14, 'ATL_PEAK_ON_OFF_FLG', '2009-04-01', 9, 0, 2009040109, 9, 'ON', 2009, 4, '2009-04', 2 );
INSERT INTO time( data_id, header_id, data_item, opr_date, opr_hr, opr_min, yyyymmddhh, interval_num, svalue, years, months, yyyymm, "quarter")
    VALUES
    (109, 14, 'ATL_PEAK_ON_OFF_FLG', '2009-04-01', 10, 0, 2009040110, 10, 'ON', 2009, 4, '2009-04', 2 );
INSERT INTO time( data_id, header_id, data_item, opr_date, opr_hr, opr_min, yyyymmddhh, interval_num, svalue, years, months, yyyymm, "quarter")
    VALUES
    (110, 14, 'ATL_PEAK_ON_OFF_FLG', '2009-04-01', 11, 0, 2009040111, 11, 'ON', 2009, 4, '2009-04', 2 );

select t1.opr_date, t1.opr_hr, t1.svalue,  t1.yyyymmddhh - t2.avg_yyyymmddhh
from time t1
left join -- works
(select extract(year from opr_date) as y, extract(month from opr_date) as m, svalue, cast(cast(avg(yyyymmddhh) as decimal(14,4)) as double) as avg_yyyymmddhh from time group by y, m, svalue) as t2
on extract(year from t1.opr_date) = t2.y
and extract(month from t1.opr_date) = t2.m
and t1.svalue = t2.svalue
order by t1.opr_hr;

select t1.opr_date, t1.opr_hr, t1.svalue,  t1.yyyymmddhh - t2.avg_yyyymmddhh
from time t1
join      -- crashes (assertion fails)
(select extract(year from opr_date) as y, extract(month from opr_date) as m, svalue, cast(cast(avg(yyyymmddhh) as decimal(14,4)) as double) as avg_yyyymmddhh from time group by y, m, svalue) as t2
on extract(year from t1.opr_date) = t2.y
and extract(month from t1.opr_date) = t2.m
and t1.svalue = t2.svalue
order by t1.opr_hr;

rollback;
