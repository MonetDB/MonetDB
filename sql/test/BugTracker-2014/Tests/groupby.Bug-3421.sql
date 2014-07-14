-- Bug 3421


create table OrderConnectorMetrics (
  toplevelorderid bigint not null, 
  businessdate date,
  connector varchar(256),
  filledValue real,
  filledQty real,
  numberOfFills int,
  chargeUsd real,
  unique (toplevelorderid, connector)
);

insert into OrderConnectorMetrics values (5508, '2013-02-01', 'C1', 3000000, 3000000, 4, null);
insert into OrderConnectorMetrics values (5508, '2013-02-01', 'C2', 2000000, 2000000, 2, null);

create table t2 as select * from orderconnectormetrics where
toplevelorderid = 5508 with data;

select * from t2;

-- first round
select toplevelorderid, count(*), sum(numberoffills) from t2 group by toplevelorderid;

select toplevelorderid, count(*), sum(numberoffills) from t2 where
toplevelorderid = 5508 group by toplevelorderid;

-- second round
select toplevelorderid, count(*), sum(numberoffills) from t2 group by toplevelorderid;

select toplevelorderid, count(*), sum(numberoffills) from t2 where
toplevelorderid = 5508 group by toplevelorderid;

select toplevelorderid, count(*), sum(numberoffills) from
orderconnectormetrics where toplevelorderid = 5508 group by toplevelorderid;

delete from t2;
insert into t2 select * from orderconnectormetrics where toplevelorderid = 5508;

-- problem less
insert into t2 values (5508, '2013-02-01', 'C1', 3000000, 3000000, 4, null);
insert into t2 values (5508, '2013-02-01', 'C2', 2000000, 2000000, 2, null);

select toplevelorderid, count(*), sum(numberoffills) from t2 where
toplevelorderid = 5508 group by toplevelorderid;

drop table t2;
drop table OrderConnectorMetrics;
