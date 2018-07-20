CREATE TABLE system (
        id integer PRIMARY KEY AUTO_INCREMENT,
        name varchar(20) UNIQUE,
        hardware_platform varchar(30),
        RAM varchar(10),
        disk_type varchar(10),
        disk_size varchar(10)
);

CREATE TABLE target (
        id integer PRIMARY KEY AUTO_INCREMENT,
        name varchar(20) UNIQUE,
        measure1 integer,
        measure2 integer,
        measure3 integer
);

CREATE SEQUENCE conf_id_seq AS int
        START WITH 1
        INCREMENT BY 1
        NO CYCLE;

CREATE TABLE configuration (
        id integer PRIMARY KEY DEFAULT NEXT VALUE FOR conf_id_seq,
        name varchar(20),
        system_id integer references system(id),
        target_id integer references target(id)
);

select avg(target_id / case (system_id - 1) WHEN 0 THEN cast(null as int)
ELSE system_id - 1 END) from configuration;

DROP TABLE configuration;
drop sequence conf_id_seq;
DROP TABLE system;
DROP TABLE target;

