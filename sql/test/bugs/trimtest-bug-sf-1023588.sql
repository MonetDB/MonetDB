-- if we don't use auto-commit here, everything goes well
--START TRANSACTION;

CREATE TABLE trimtest (
        number  integer,
        number_sup      char(1),
        trip    integer,
        trip_sup        char(1),
        boatname        varchar(50),
        master  varchar(50),
        tonnage integer,
        type_of_boat    varchar(30),
        built   varchar(15),
        bought  varchar(15),
        hired   varchar(15),
        yard    char(1),
        chamber char(1),
        departure_date  varchar(15),
        departure_harbour       varchar(30),
        cape_arrival    varchar(15),
        cape_departure  varchar(15),
        arrival_date    varchar(15),
        arrival_harbour varchar(30),
        next_voyage     integer,
        particulars     varchar(530)
);

INSERT INTO trimtest VALUES (7948, '', 1, '', 'PATRIOT', ' Volkers', 1150, NULL, '1773', NULL, NULL, 'A', 'A', '1775-10-20', 'Batavia', '1776-01-08', '1776-04-06', '1776-07-03', 'Texel', 4219, ' The PATRIOT transported the cargo of the NIEUW RHOON, laid up at the Cape (see 7955); invoice value: f 293,773, destined for the chamber Amsterdam.');

select master from trimtest;

select trim(master) from trimtest;

update trimtest set master = trim(master);

select master from trimtest;

--ROLLBACK;
DROP TABLE trimtest;
