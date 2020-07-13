START TRANSACTION;
CREATE TABLE "fluxz" (
	"runcat"           INTEGER       NOT NULL,
	"filter"           CHAR(1)       NOT NULL,
	"f_datapoints"     INTEGER       NOT NULL,
	"active"           BOOLEAN       NOT NULL DEFAULT true,
	"avg_flux"         DOUBLE        NOT NULL,
	"avg_fluxsq"       DOUBLE        NOT NULL,
	"avg_w"            DOUBLE        NOT NULL,
	"avg_wflux"        DOUBLE        NOT NULL,
	"avg_wfluxsq"      DOUBLE        NOT NULL,
	"avg_dec_zone_deg" TINYINT       NOT NULL
);
CREATE TABLE "cm_flux" (
	"runcat"           INTEGER       NOT NULL,
	"xtrsrc"           INTEGER       NOT NULL,
	"filter"           CHAR(1)       NOT NULL,
	"f_datapoints"     INTEGER       NOT NULL,
	"active"           BOOLEAN       NOT NULL DEFAULT true,
	"avg_flux"         DOUBLE        NOT NULL,
	"avg_fluxsq"       DOUBLE        NOT NULL,
	"avg_w"            DOUBLE        NOT NULL,
	"avg_wflux"        DOUBLE        NOT NULL,
	"avg_wfluxsq"      DOUBLE        NOT NULL,
	"avg_dec_zone_deg" TINYINT       NOT NULL
);
COMMIT;
