create table voyage(
	number integer,
	trip   integer,
	boatname varchar(25),
	master varchar(25),
	tonnage integer,
	birth   varchar(10),
	acquired integer,
	yard	char(1),
	chamber	char(1),
	departure	date,
	harbour	varchar(12),
	cape_arrival date,
	cape_departure date,
	destination_arrival date,
	destination_harbour varchar(12)
);

-- example XML record
-- <VOC>
-- <voyage>
-- 	<number>4408</number><trip>3</trip>
-- 	<boatname>BRESLAU</boatname>
-- 	<master>Jan Kornelis Roos</master>
-- 	<tonnage>1150</tonnage>
-- 	<hired>1774</hired>
-- 	<yard>A</yard>
-- 	<chamber>Z</chamber>
-- 	<departure>15-02-1783</departure><harbour>Rammekens</harbour>
-- 	<callatcape><arrival>27-05-1783</arrival><departure>12-06-1783</departure></callatcape>
-- 	<destination><arrival>06-08-1783</arrival><harbour>Batavia</harbour></destination>
-- </voyage>
