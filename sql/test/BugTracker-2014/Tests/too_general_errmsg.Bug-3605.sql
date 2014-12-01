start transaction;
CREATE TABLE "sys"."tt_20141016" (
"operatingday"          DATE,
"trip_id"               VARCHAR(16),
"pointorder"            SMALLINT,
"passagesequencenumber" SMALLINT,
"userstopcode"          VARCHAR(10),
"targetarrivaltime"     VARCHAR(8),
"targetdeparturetime"   VARCHAR(8),
"trip_hash"             BIGINT
);
create view tt as select * from tt_20141016;
CREATE TABLE "sys"."kv17_canceled" (
"server"                TIMESTAMP,
"messagetype"           VARCHAR(17),
"dataownercode"         VARCHAR(10),
"operatingday"          DATE,
"lineplanningnumber"    VARCHAR(10),
"journeynumber"         DECIMAL(6),
"reinforcementnumber"   DECIMAL(2),
"reasontype"            DECIMAL(3),
"subreasontype"         VARCHAR(10),
"reasoncontent"         VARCHAR(255),
"subreasoncontent"      VARCHAR(255),
"userstopcode"          VARCHAR(10),
"passagesequencenumber" DECIMAL(4),
"lagtime"               DECIMAL(4),
"targetarrivaltime"     TIME,
"targetdeparturetime"   TIME,
"journeystoptype"       VARCHAR(12),
"destinationcode"       VARCHAR(10),
"destinationname50"     VARCHAR(50),
"destinationname16"     VARCHAR(16),
"destinationdetail16"   VARCHAR(16),
"destinationdisplay16"  VARCHAR(16),
"trip_hash"             BIGINT,
"message"               TIMESTAMP
);
create view kv6 as select * from kv17_canceled; 
select operatingday, coalesce(gepubliceerd, 0) as gepubliceerd, coalesce(gereden, 0) as gereden, coalesce(geannuleerd, 0) as geannuleerd, coalesce(onbekend, 0) as onbekend, coalesce(extra, 0) as extra, coalesce(tochgezien, 0) as tochgezien from (select operatingday, count(trip_hash) as gepubliceerd from tt where pointorder = 1 group by operatingday) as a full outer join (select z.operatingday, count(*) as geannuleerd from (select trip_hash, operatingday from kv17_canceled except select trip_hash, operatingday from kv6 where kv6.messagetype = 'ARRIVAL') as u group by u.operatingday) as e using (operatingday) full outer join (select o.operatingday, count(trip_hash) as onbekend from (select distinct trip_hash, operatingday from tt) as o join (select distinct trip_hash from tt where (epoch(cast(tt.operatingday as timestamp with time zone)) + cast(split_part(tt.targetarrivaltime, ':', 1) as int) * 3600 + (cast(split_part(tt.targetarrivaltime, ':', 2) as int) + 10) * 60) < epoch(now()) except (select trip_hash from kv6 where messagetype = 'ARRIVAL' union all select trip_hash from kv17_canceled)) as p using (trip_hash) group by operatingday) as f using (operatingday) order by operatingday;
