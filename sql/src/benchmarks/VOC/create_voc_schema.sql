START TRANSACTION;

--DROP TABLE voyages;
CREATE TABLE voyages (
	number INTEGER,
	number_sup CHAR(1),
	trip INTEGER,
	trip_sup CHAR(1),
	boatname VARCHAR(50),
	master VARCHAR(50),
	tonnage INTEGER,
	type_of_boat VARCHAR(30),
	built VARCHAR(15),
	bought VARCHAR(15),
	hired VARCHAR(15),
	yard CHAR(1),
	chamber CHAR(1),
	departure_date VARCHAR(15),
	departure_harbour VARCHAR(30),
	cape_arrival VARCHAR(15),
	cape_departure VARCHAR(15),
	arrival_date VARCHAR(15),
	arrival_harbour VARCHAR(30),
	next_voyage INTEGER,
	particulars VARCHAR(507)
);

--DROP TABLE seafarers;
CREATE TABLE seafarers (
	number INTEGER,
	number_sup VARCHAR(1),
	trip INTEGER,
	trip_sup VARCHAR(1),
	one INTEGER,
	two INTEGER,
	three INTEGER,
	four INTEGER,
	five INTEGER,
	six INTEGER
);

--DROP TABLE soldiers;
CREATE TABLE soldiers (
	number INTEGER,
	number_sup VARCHAR(1),
	trip INTEGER,
	trip_sup VARCHAR(1),
	one INTEGER,
	two INTEGER,
	three INTEGER,
	four INTEGER,
	five INTEGER,
	six INTEGER
);

--DROP TABLE craftsmen;
CREATE TABLE craftsmen (
	number INTEGER,
	number_sup VARCHAR(1),
	trip INTEGER,
	trip_sup VARCHAR(1),
	one INTEGER,
	two INTEGER,
	three INTEGER,
	four INTEGER,
	five INTEGER,
	six INTEGER
);

--DROP TABLE passengers;
CREATE TABLE passengers (
	number INTEGER,
	number_sup VARCHAR(1),
	trip INTEGER,
	trip_sup VARCHAR(1),
	one INTEGER,
	two INTEGER,
	three INTEGER,
	four INTEGER,
	five INTEGER,
	six INTEGER
);

--DROP TABLE impotenten;
CREATE TABLE impotenten (
	number INTEGER,
	number_sup VARCHAR(1),
	trip INTEGER,
	trip_sup VARCHAR(1),
	one INTEGER,
	two INTEGER,
	three INTEGER,
	four INTEGER,
	five INTEGER,
	six INTEGER
);

--DROP TABLE total;
CREATE TABLE total (
	number INTEGER,
	number_sup VARCHAR(1),
	trip INTEGER,
	trip_sup VARCHAR(1),
	one INTEGER,
	two INTEGER,
	three INTEGER,
	four INTEGER,
	five INTEGER,
	six INTEGER
);

--DROP TABLE invoices;
CREATE TABLE invoices (
	number INTEGER,
	number_sup VARCHAR(1),
	trip INTEGER,
	trip_sup VARCHAR(1),
	invoice INTEGER,
	chamber VARCHAR(3)
);

COMMIT;
