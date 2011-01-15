START TRANSACTION;
create table click_(
		cl_time time,
		url_id integer NOT NULL,
		primary key (cl_time)
		)
;

INSERT INTO click_
values ('00:02:54:458',1)
;


create table statistics_(
		url_id integer ,
		hour1 integer,
		primary key (url_id)
		)
;

INSERT INTO statistics_ values (1,0);


CREATE TABLE temp_table_(
		url_id integer,
		counter1 integer
		)
;


INSERT INTO temp_table_
SELECT DISTINCT url_id,0
FROM click_
;

UPDATE temp_table_
SET counter1 = (SELECT count(*) 
		FROM click_
		WHERE extract(hour from
			cl_time)=0
		GROUP BY
		url_id)
WHERE url_id IN (SELECT url_id FROM click_ 
		WHERE extract(hour from cl_time)=0
		)
;

select * from statistics_;

UPDATE statistics_
SET hour1 = hour1 + (SELECT counter1 FROM temp_table_
		WHERE
		temp_table_.url_id = statistics_.url_id
		)

WHERE url_id IN (SELECT url_id FROM click_
		WHERE extract(hour from
			cl_time)=0
		)
;

select * from statistics_;

drop table click_;
drop table statistics_;
drop table temp_table_;
