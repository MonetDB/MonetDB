START TRANSACTION;
CREATE TABLE "t3097" (
    "id"        INTEGER       NOT NULL AUTO_INCREMENT,
    "name"      VARCHAR(100)  NOT NULL,
    "app_label" VARCHAR(100)  NOT NULL,
    "model"     VARCHAR(100)  NOT NULL,
    CONSTRAINT "t3097_id_pkey" PRIMARY KEY ("id"),
    CONSTRAINT "t3097_app_label_model_unique" 
    UNIQUE ("app_label", "model")
)
;

INSERT INTO "t3097" 
  ("name"
  ,"app_label"
  ,"model"
  ) 
VALUES 
  ('content type'
  ,'contenttypes'
  ,'contenttype'
  )
;

DROP TABLE "t3097";

ROLLBACK;
