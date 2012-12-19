-- Optimized schema for FITS (meta-)data.
CREATE SCHEMA fits;

CREATE TABLE "fits"."images" (
--	"file_id"  INTEGER,
	"file_location" STRING,
	"hdu"	INTEGER,

-- 	"name"	VARCHAR(80),
-- 	"columns"	INTEGER,
-- 	"date"	VARCHAR(80),
-- 	"origin"	VARCHAR(80),
-- 	"comment"	VARCHAR(80),
-- 	"xtension"	VARCHAR(80),
-- 	"bitpix"	INTEGER,
-- 	"stilvers"	VARCHAR(80),
-- 	"stilclas"	VARCHAR(80),

	"type"	CHAR(1),
	"bitpix"	INTEGER,
	"naxis"	INTEGER,
	"naxis1"	INTEGER,
	"naxis2"	INTEGER,
	"naxis3"	INTEGER,
	"naxis4"	INTEGER,

	"bscale"	DOUBLE,
	"bzero"	DOUBLE,
	"bmaj"	DOUBLE,
	"bmin"	DOUBLE,
	"bpa"	DOUBLE,

	"btype"	STRING,
	"object"	STRING,
	"bunit"	STRING,

	"equinox"	DOUBLE,
	"lonpole"	DOUBLE,
	"latpole"	DOUBLE,
	"pc001001"	DOUBLE,
	"pc002001"	DOUBLE,
	"pc003001"	DOUBLE,
	"pc004001"	DOUBLE,
	"pc001002"	DOUBLE,
	"pc002002"	DOUBLE,
	"pc003002"	DOUBLE,
	"pc004002"	DOUBLE,
	"pc001003"	DOUBLE,
	"pc002003"	DOUBLE,
	"pc003003"	DOUBLE,
	"pc004003"	DOUBLE,
	"pc001004"	DOUBLE,
	"pc002004"	DOUBLE,
	"pc003004"	DOUBLE,
	"pc004004"	DOUBLE,

	"ctype1"	STRING,
	"crval1"	DOUBLE,
	"cdelt1"	DOUBLE,
	"crpix1"	DOUBLE,

	"cunit1"	STRING,
	"ctype2"	STRING,
	"crval2"	DOUBLE,
	"cdelt2"	DOUBLE,
	"crpix2"	DOUBLE,

	"cunit2"	STRING,
	"ctype3"	STRING,
	"crval3"	DOUBLE,
	"cdelt3"	DOUBLE,
	"crpix3"	DOUBLE,

	"cunit3"	STRING,
	"ctype4"	STRING,
	"crval4"	DOUBLE,
	"cdelt4"	DOUBLE,
	"crpix4"	DOUBLE,

	"cunit4"	STRING,
	"pv2_1"	DOUBLE,
	"pv2_2"	DOUBLE,
	"restfrq"	DOUBLE,

	"specsys"	STRING,
	"altrval"	DOUBLE,
	"altrpix"	DOUBLE,
	"velref"	TINYINT,

	"telescop"	STRING,
	"observer"	STRING,
	"date_obs"	TIMESTAMP,
	"timesys"	STRING,

 	"obsra"	DOUBLE,
	"obsdec"	DOUBLE,
	"obsgeo_x"	DOUBLE,
	"obsgeo_y"	DOUBLE,
	"obsgeo_z"	DOUBLE,

	"date"	TIMESTAMP,
	"origin"	STRING,

--	CONSTRAINT "files_pkey_file_id" PRIMARY KEY (file_id)
	CONSTRAINT "tables_pkey_file_loc_hdu" PRIMARY KEY (file_location, hdu)
);

-- CREATE TABLE "fits"."columns" (
-- --	"file_id" INTEGER,	
-- 	"file_location" STRING,
-- 	"hdu"	INTEGER,
-- 	"number"	INTEGER,
-- 	"name"	VARCHAR(80),
-- 	"type"	VARCHAR(80),
-- 	"units"	VARCHAR(10),
-- 	CONSTRAINT "columns_file_loc_hdu_number_pkey" PRIMARY KEY (file_location, hdu, number),
-- 	CONSTRAINT "col_fkey_tables_file_loc_hdu" FOREIGN KEY (file_location, hdu) REFERENCES fits.tables(file_location, hdu)
-- );

CREATE TABLE "fits"."data" (
--	"file_id"      INTEGER,
	"file_location" STRING,
	"hdu"	INTEGER,
	"axis1"	INTEGER,
	"axis2"	INTEGER,
	"value"	FLOAT
);

CREATE VIEW fits.dataview AS
SELECT i.file_location, i.hdu, type, bitpix, naxis, naxis1, naxis2, naxis3, naxis4, bscale, bzero, bmaj, bmin, bpa, btype, object, bunit, equinox, lonpole, latpole, pc001001, pc002001, pc003001, pc004001, pc001002, pc002002, pc003002, pc004002, pc001003, pc002003, pc003003, pc004003, pc001004, pc002004, pc003004, pc004004, ctype1, crval1, cdelt1, crpix1, cunit1, ctype2, crval2, cdelt2, crpix2, cunit2, ctype3, crval3, cdelt3, crpix3, cunit3, ctype4, crval4, cdelt4, crpix4, cunit4, pv2_1, pv2_2, restfrq, specsys, altrval, altrpix, velref, telescop, observer, date_obs, timesys, obsra, obsdec, obsgeo_x, obsgeo_y, obsgeo_z, date, origin, axis1, axis2, value
FROM fits.images AS i 
	JOIN fits.data AS d
		ON i.file_location = d.file_location AND i.hdu = d.hdu;



