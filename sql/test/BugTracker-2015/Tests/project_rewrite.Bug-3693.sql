START TRANSACTION;


CREATE TABLE "naturalis_obj_dict" (
	"idstr" CHARACTER LARGE OBJECT NOT NULL,
	"id"    INTEGER       NOT NULL,
	"type"  CHARACTER LARGE OBJECT NOT NULL,
	"prob"  FLOAT(51)     NOT NULL,
	CONSTRAINT "naturalis_obj_dict_id_pkey" PRIMARY KEY ("id"),
	CONSTRAINT "naturalis_obj_dict_idstr_unique" UNIQUE ("idstr")
);

CREATE TABLE "naturalis_materialized_1" (
	"id"    INTEGER       NOT NULL,
	"pid"   CHARACTER LARGE OBJECT NOT NULL,
	"level" INTEGER,
	"class" CHARACTER LARGE OBJECT NOT NULL,
	"json"  CHARACTER LARGE OBJECT,
	"xml"   CHARACTER LARGE OBJECT,
	CONSTRAINT "naturalis_materialized_1_id_pkey" PRIMARY KEY ("id"),
	CONSTRAINT "naturalis_materialized_1_pid_unique" UNIQUE ("pid")
);

CREATE TABLE "naturalis_all_termdict_snowball" (
	"termid" INTEGER       NOT NULL,
	"term"   CHARACTER LARGE OBJECT NOT NULL,
	"prob"   FLOAT(51)     NOT NULL,
	CONSTRAINT "naturalis_all_termdict_snowball_termid_pkey" PRIMARY KEY ("termid"),
	CONSTRAINT "naturalis_all_termdict_snowball_term_unique" UNIQUE ("term")
);

CREATE TABLE "naturalis_all_termdict_unstemmed" (
	"termid" INTEGER       NOT NULL,
	"term"   CHARACTER LARGE OBJECT NOT NULL,
	"sb"     INTEGER       NOT NULL,
	"prob"   DOUBLE        NOT NULL,
	CONSTRAINT "naturalis_all_termdict_unstemmed_termid_pkey" PRIMARY KEY ("termid"),
	CONSTRAINT "naturalis_all_termdict_unstemmed_term_unique" UNIQUE ("term"),
	CONSTRAINT "naturalis_all_termdict_unstemmed_sb_fkey" FOREIGN KEY ("sb") REFERENCES "naturalis_all_termdict_snowball" ("termid")
);


CREATE TABLE "naturalis_all_tf_bm25_unstemmed" (
	"termid" INTEGER       NOT NULL,
	"objid"  INTEGER       NOT NULL,
	"prob"   DOUBLE        NOT NULL,
	CONSTRAINT "naturalis_all_tf_bm25_unstemmed_objid_fkey" FOREIGN KEY ("objid") REFERENCES "naturalis_obj_dict" ("id"),
	CONSTRAINT "naturalis_all_tf_bm25_unstemmed_termid_fkey" FOREIGN KEY ("termid") REFERENCES "naturalis_all_termdict_unstemmed" ("termid")
);


CREATE VIEW output_1427727864562_8628476569578157143 AS SELECT naturalis_all_tf_bm25_unstemmed.termID AS a1, naturalis_all_tf_bm25_unstemmed.objID AS a2, naturalis_all_termdict_unstemmed.termID AS a3, naturalis_all_termdict_unstemmed.term AS a4, naturalis_all_termdict_unstemmed.sb AS a5, naturalis_all_tf_bm25_unstemmed.prob * naturalis_all_termdict_unstemmed.prob AS prob FROM naturalis_all_tf_bm25_unstemmed,naturalis_all_termdict_unstemmed WHERE naturalis_all_tf_bm25_unstemmed.termID = naturalis_all_termdict_unstemmed.termID;

CREATE VIEW output_1427727864562_5257959729065684465 AS SELECT id AS a1, prob AS prob FROM naturalis_obj_dict;

CREATE VIEW output_1427727864562__3867191803197065991 AS SELECT output_1427727864562_8628476569578157143.a1 AS a1, output_1427727864562_8628476569578157143.a2 AS a2, output_1427727864562_8628476569578157143.a3 AS a3, output_1427727864562_8628476569578157143.a4 AS a4, output_1427727864562_8628476569578157143.a5 AS a5, output_1427727864562_5257959729065684465.a1 AS a6, output_1427727864562_8628476569578157143.prob * output_1427727864562_5257959729065684465.prob AS prob FROM output_1427727864562_8628476569578157143,output_1427727864562_5257959729065684465 WHERE output_1427727864562_8628476569578157143.a2 = output_1427727864562_5257959729065684465.a1;

CREATE VIEW output_1427727864562_7461402036329501989 AS SELECT a2 AS a1, a4 AS a2, prob AS prob FROM output_1427727864562__3867191803197065991;

CREATE VIEW output_1427727864562_664384226664078002 AS SELECT output_1427727864562_7461402036329501989.a1 AS a1, output_1427727864562_7461402036329501989.a2 AS a2, output_1427727864562_7461402036329501989.a1 AS a3, output_1427727864562_7461402036329501989.a2 AS a4, output_1427727864562_7461402036329501989.prob * output_1427727864562_7461402036329501989.prob AS prob FROM output_1427727864562_7461402036329501989,output_1427727864562_7461402036329501989 WHERE output_1427727864562_7461402036329501989.a2 = output_1427727864562_7461402036329501989.a2;

CREATE VIEW output_1427727864562_8055303038742365054 AS SELECT a1 AS a1, a3 AS a2, sum(prob) AS prob FROM output_1427727864562_664384226664078002 GROUP BY a1, a3;

CREATE VIEW output_1427727864562 AS SELECT a1 AS a1, a2 AS a2, prob AS prob FROM (SELECT a1 AS a1, a2 AS a2, prob AS prob FROM (SELECT output_1427727864562_8055303038742365054.a1 AS a1, output_1427727864562_8055303038742365054.a2 AS a2, output_1427727864562_8055303038742365054.prob / tmp_3083417802646904664.prob AS prob FROM output_1427727864562_8055303038742365054,(SELECT max(prob) AS prob FROM output_1427727864562_8055303038742365054) AS tmp_3083417802646904664) AS tmp_6705425488457746005) AS tmp__3265950636919299731 WHERE prob >= 0.5;


--CREATE VIEW source AS SELECT a.*, row_number() OVER(ORDER BY prob DESC, a1) as rownum FROM output_1427727864562 as a;
--CREATE VIEW source AS SELECT a.*, row_number() OVER(ORDER BY prob DESC) as rownum FROM output_1427727864562_7461402036329501989 as a;
--CREATE VIEW source AS SELECT a.*, row_number() OVER() as rownum FROM output_1427727864562_7461402036329501989 as a;
CREATE VIEW source AS SELECT a.*, prob as rownum FROM output_1427727864562_7461402036329501989 as a;

SELECT source.prob
FROM source,
 naturalis_materialized_1 as t1
WHERE t1.id=source.a1 and source.rownum > 0 AND source.rownum <= 10;

