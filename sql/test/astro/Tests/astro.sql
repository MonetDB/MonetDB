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

plan UPDATE fluxz
   SET (filter
       ,f_datapoints
       ,avg_flux
       ,avg_fluxsq
       ,avg_w
       ,avg_wflux
       ,avg_wfluxsq
       ,avg_dec_zone_deg
       )
       =
       (SELECT filter
              ,f_datapoints
              ,avg_flux
              ,avg_fluxsq
              ,avg_w
              ,avg_wflux
              ,avg_wfluxsq
              ,avg_dec_zone_deg
          FROM cm_flux 
         WHERE cm_flux.runcat = fluxz.runcat 
           AND cm_flux.active = TRUE 
           AND cm_flux.filter = 'g' 
           AND cm_flux.filter = fluxz.filter
       )
 WHERE EXISTS (SELECT runcat
                 FROM cm_flux
                WHERE cm_flux.runcat = fluxz.runcat 
                  AND cm_flux.active = TRUE 
                  AND cm_flux.filter = 'g' 
                  AND cm_flux.filter = fluxz.filter
              );

drop table "fluxz";
drop table "cm_flux";

ROLLBACK;
