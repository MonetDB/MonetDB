CREATE TABLE "sys"."farmap_movimento" (
	"id"                    SERIAL,
	"articolo_id"           BIGINT        NOT NULL,
	"farmacia_id"           INTEGER       NOT NULL,
	"causale_id"            INTEGER       NOT NULL,
	"ts"                    TIMESTAMP     NOT NULL,
	"anno"                  SMALLINT      NOT NULL,
	"mese"                  SMALLINT      NOT NULL,
	"data"                  CHARACTER LARGE OBJECT NOT NULL,
	"ora"                   CHARACTER LARGE OBJECT NOT NULL,
	"tipo_doc"              CHARACTER LARGE OBJECT NOT NULL,
	"num_doc"               CHARACTER LARGE OBJECT NOT NULL,
	"num_riga"              INTEGER       NOT NULL,
	"flag_flusso"           CHARACTER LARGE OBJECT,
	"tessera"               CHARACTER LARGE OBJECT,
	"cassa"                 CHARACTER LARGE OBJECT,
	"operatore"             CHARACTER LARGE OBJECT,
	"tipo_sconto"           CHARACTER LARGE OBJECT,
	"tipo_vend"             CHARACTER LARGE OBJECT,
	"ricetta_numero"        CHARACTER LARGE OBJECT,
	"barcode_reg_asl"       CHARACTER LARGE OBJECT,
	"barcode_ricettario"    CHARACTER LARGE OBJECT,
	"anomalia_dominio"      CHARACTER LARGE OBJECT,
	"anomalia_integrita"    CHARACTER LARGE OBJECT,
	"anomalia_quadratura"   CHARACTER LARGE OBJECT,
	"anomalia_congruenza"   CHARACTER LARGE OBJECT,
	"offerta"               CHARACTER LARGE OBJECT,
	"flag_ricetta"          CHARACTER LARGE OBJECT,
	"flag_costo"            CHARACTER LARGE OBJECT,
	"pagamento"             CHARACTER LARGE OBJECT,
	"pagamento_modalita"    CHARACTER LARGE OBJECT,
	"regola_prezzo"         CHARACTER LARGE OBJECT,
	"regola_costo"          CHARACTER LARGE OBJECT,
	"flag_sconto"           CHARACTER LARGE OBJECT,
	"uscito_qta"            DOUBLE,
	"uscito_val"            DOUBLE,
	"uscito_val_n_iva"      DOUBLE,
	"uscito_listino"        DOUBLE,
	"uscito_listino_n_iva"  DOUBLE,
	"uscito_costo"          DOUBLE,
	"uscito_sconto_riga"    DOUBLE,
	"uscito_sconto_doc"     DOUBLE,
	"uscito_sconto_asl"     DOUBLE,
	"uscito_sconto_listino" DOUBLE,
	"uscito_ricetta"        DOUBLE,
	"uscito_ticket"         DOUBLE,
	"uscito_ricetta_quota"  DOUBLE,
	"periodo"               INTEGER       NOT NULL DEFAULT 0,
	"flag_promo"            BOOLEAN       NOT NULL DEFAULT FALSE,
	"scontrino"             BIGINT        DEFAULT 0,
	"class_interna"         INTEGER       NOT NULL DEFAULT 0,
	"anno_mese"             INTEGER,
	"anno_periodo"          INTEGER
);

CREATE INDEX "annomese_idx" ON "sys"."farmap_movimento" ("anno_mese");
CREATE INDEX "articolo_idx" ON "sys"."farmap_movimento" ("articolo_id");
CREATE INDEX "farmacia_idx" ON "sys"."farmap_movimento" ("farmacia_id");

-- TODO: 3. load data from csv with \copy

SELECT * FROM "sys"."farmap_movimento" ORDER BY "id";

SELECT count(distinct "anno_mese") FROM "sys"."farmap_movimento";

SELECT count(distinct "articolo_id") FROM "sys"."farmap_movimento";

SELECT count(distinct "farmacia_id") FROM "sys"."farmap_movimento";

DROP TABLE "sys"."farmap_movimento";

