START TRANSACTION;

CREATE TABLE "docdict" (
	"docid"  int           NOT NULL,
	"docoid" bigint,
	"doc"    varchar(10000),
	"type"   varchar(255),
	"prob"   double        DEFAULT 1,
	CONSTRAINT "docdict_docid_pkey" PRIMARY KEY ("docid")
);
CREATE TABLE "secdict" (
	"secid"  int           NOT NULL,
	"secoid" bigint,
	"docid"  int,
	"lang"   varchar(255),
	"type"   varchar(255),
	"prob"   double        DEFAULT 1,
	CONSTRAINT "secdict_secid_pkey" PRIMARY KEY ("secid"),
	CONSTRAINT "secdict_docid_fkey" FOREIGN KEY ("docid") REFERENCES "docdict" ("docid") ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE "nedict" (
	"neid" int           NOT NULL,
	"ne"   varchar(10000),
	"type" varchar(255),
	"prob" double        DEFAULT 1,
	CONSTRAINT "nedict_neid_pkey" PRIMARY KEY ("neid")
);
CREATE TABLE "termdict" (
	"termid" int           NOT NULL,
	"term"   varchar(1000),
	"prob"   double,
	CONSTRAINT "termdict_termid_pkey" PRIMARY KEY ("termid")
);

CREATE TABLE "doc_string" (
	"docid"     int,
	"attribute" varchar(255),
	"value"     varchar(10000),
	"prob"      double        DEFAULT 1,
	CONSTRAINT "doc_string_docid_fkey" FOREIGN KEY ("docid") REFERENCES "docdict" ("docid") ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE "doc_integer" (
	"docid"     int,
	"attribute" varchar(255),
	"value"     int,
	"prob"      double        DEFAULT 1,
	CONSTRAINT "doc_integer_docid_fkey" FOREIGN KEY ("docid") REFERENCES "docdict" ("docid") ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE "doc_date" (
	"docid"     int,
	"attribute" varchar(255),
	"value"     date,
	"prob"      double        DEFAULT 1,
	CONSTRAINT "doc_date_docid_fkey" FOREIGN KEY ("docid") REFERENCES "docdict" ("docid") ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE "sec_string" (
	"secid"     int,
	"attribute" varchar(255),
	"value"     varchar(10000),
	"prob"      double        DEFAULT 1,
	CONSTRAINT "sec_string_secid_fkey" FOREIGN KEY ("secid") REFERENCES "secdict" ("secid") ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE "sec_integer" (
	"secid"     int,
	"attribute" varchar(255),
	"value"     int,
	"prob"      double        DEFAULT 1,
	CONSTRAINT "sec_integer_secid_fkey" FOREIGN KEY ("secid") REFERENCES "secdict" ("secid") ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE "ne_string" (
	"neid"      int,
	"attribute" varchar(255),
	"value"     varchar(10000),
	"prob"      double        DEFAULT 1,
	CONSTRAINT "ne_string_neid_fkey" FOREIGN KEY ("neid") REFERENCES "nedict" ("neid") ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE "ne_integer" (
	"neid"      int,
	"attribute" varchar(255),
	"value"     int,
	"prob"      double        DEFAULT 1,
	CONSTRAINT "ne_integer_neid_fkey" FOREIGN KEY ("neid") REFERENCES "nedict" ("neid") ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE "ne_date" (
	"neid"      int,
	"attribute" varchar(255),
	"value"     date,
	"prob"      double        DEFAULT 1,
	CONSTRAINT "ne_date_neid_fkey" FOREIGN KEY ("neid") REFERENCES "nedict" ("neid") ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE "doc_doc" (
	"docid1"    int,
	"predicate" varchar(255),
	"docid2"    int,
	"prob"      double        DEFAULT 1,
	CONSTRAINT "doc_doc_docid1_fkey" FOREIGN KEY ("docid1") REFERENCES "docdict" ("docid") ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT "doc_doc_docid2_fkey" FOREIGN KEY ("docid2") REFERENCES "docdict" ("docid") ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE "ne_doc" (
	"neid"      int,
	"predicate" varchar(255),
	"docid"     int,
	"prob"      double        DEFAULT 1,
	CONSTRAINT "ne_doc_docid_fkey" FOREIGN KEY ("docid") REFERENCES "docdict" ("docid") ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT "ne_doc_neid_fkey" FOREIGN KEY ("neid") REFERENCES "nedict" ("neid") ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE "ne_sec" (
	"neid"      int,
	"predicate" varchar(255),
	"secid"     int,
	"prob"      double        DEFAULT 1,
	CONSTRAINT "ne_sec_neid_fkey" FOREIGN KEY ("neid") REFERENCES "nedict" ("neid") ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT "ne_sec_secid_fkey" FOREIGN KEY ("secid") REFERENCES "secdict" ("secid") ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE "ne_ne" (
	"neid1"     int,
	"predicate" varchar(255),
	"neid2"     int,
	"prob"      double        DEFAULT 1,
	CONSTRAINT "ne_ne_neid1_fkey" FOREIGN KEY ("neid1") REFERENCES "nedict" ("neid") ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT "ne_ne_neid2_fkey" FOREIGN KEY ("neid2") REFERENCES "nedict" ("neid") ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE "tdf_bm25" (
	"termid" int,
	"docid"  int,
	"prob"   double,
	CONSTRAINT "tdf_bm25_docid_fkey" FOREIGN KEY ("docid") REFERENCES "docdict" ("docid") ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT "tdf_bm25_termid_fkey" FOREIGN KEY ("termid") REFERENCES "termdict" ("termid") ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE "idf_bm25" (
	"termid" int,
	"prob"   float(53,1),
	CONSTRAINT "idf_bm25_termid_fkey" FOREIGN KEY ("termid") REFERENCES "termdict" ("termid") ON DELETE CASCADE ON UPDATE CASCADE
);



-- nyt0_DATA_docDict = docDict
CREATE VIEW nyt0_DATA_docDict AS SELECT ALL docID AS a1, docoid AS a2, doc AS a3, type AS a4, prob FROM docDict;

-- nyt0_DATA_secDict = secDict
CREATE VIEW nyt0_DATA_secDict AS SELECT ALL secID AS a1, secoid AS a2, docID AS a3, lang AS a4, type AS a5, prob FROM secDict;

-- nyt0_DATA_neDict = neDict
CREATE VIEW nyt0_DATA_neDict AS SELECT ALL neID AS a1, ne AS a2, type AS a3, prob FROM neDict;

-- nyt0_DATA_termDict = termDict
CREATE VIEW nyt0_DATA_termDict AS SELECT ALL termID AS a1, term AS a2, prob FROM termDict;

-- nyt0_DATA_doc_string = doc_string
CREATE VIEW nyt0_DATA_doc_string AS SELECT ALL docID AS a1, attribute AS a2, value AS a3, prob FROM doc_string;

-- nyt0_DATA_doc_date = doc_date
CREATE VIEW nyt0_DATA_doc_date AS SELECT ALL docID AS a1, attribute AS a2, value AS a3, prob FROM doc_date;

-- nyt0_DATA_doc_integer = doc_integer
CREATE VIEW nyt0_DATA_doc_integer AS SELECT ALL docID AS a1, attribute AS a2, value AS a3, prob FROM doc_integer;

-- nyt0_DATA_ne_string = ne_string
CREATE VIEW nyt0_DATA_ne_string AS SELECT ALL neID AS a1, attribute AS a2, value AS a3, prob FROM ne_string;

-- nyt0_DATA_ne_integer = ne_integer
CREATE VIEW nyt0_DATA_ne_integer AS SELECT ALL neID AS a1, attribute AS a2, value AS a3, prob FROM ne_integer;

-- nyt0_DATA_ne_date = ne_date
CREATE VIEW nyt0_DATA_ne_date AS SELECT ALL neID AS a1, attribute AS a2, value AS a3, prob FROM ne_date;

-- nyt0_DATA_doc_doc = doc_doc
CREATE VIEW nyt0_DATA_doc_doc AS SELECT ALL docID1 AS a1, predicate AS a2, docID2 AS a3, prob FROM doc_doc;

-- nyt0_DATA_ne_doc = ne_doc
CREATE VIEW nyt0_DATA_ne_doc AS SELECT ALL neID AS a1, predicate AS a2, docID AS a3, prob FROM ne_doc;

-- nyt0_DATA_ne_ne = ne_ne
CREATE VIEW nyt0_DATA_ne_ne AS SELECT ALL neID1 AS a1, predicate AS a2, neID2 AS a3, prob FROM ne_ne;

-- nyt0_DATA_tf_bm25 = tdf_bm25
CREATE VIEW nyt0_DATA_tf_bm25 AS SELECT ALL termID AS a1, docID AS a2, prob FROM tdf_bm25;

-- nyt0_DATA_idf_bm25 = idf_bm25
CREATE VIEW nyt0_DATA_idf_bm25 AS SELECT ALL termID AS a1, prob FROM idf_bm25;

-- nyt0_DATA_result = PROJECT[$1](nyt0_DATA_docDict)
CREATE VIEW nyt0_DATA_result AS SELECT ALL a1, prob FROM nyt0_DATA_docDict;




CREATE TABLE keywords0_tupleData(a1 VARCHAR(1000), prob DOUBLE);
INSERT INTO keywords0_tupleData VALUES ('spare', 1.000000 );
-- keywords0_term_RESULT = keywords0_tupleData
CREATE VIEW keywords0_term_RESULT AS SELECT ALL a1, prob FROM keywords0_tupleData;




-- rank_DOC_BM250_qterm_1 = JOIN[$2=$1](nyt0_DATA_termDict,keywords0_term_result)
CREATE VIEW rank_DOC_BM250_qterm_1 AS SELECT ALL nyt0_DATA_termDict.a1 AS a1, nyt0_DATA_termDict.a2 AS a2, keywords0_term_result.a1 AS a3, nyt0_DATA_termDict.prob * keywords0_term_result.prob AS prob FROM nyt0_DATA_termDict, keywords0_term_result WHERE nyt0_DATA_termDict.a2=keywords0_term_result.a1;

-- rank_DOC_BM250_qterm = PROJECT[$1](rank_DOC_BM250_qterm_1)
CREATE VIEW rank_DOC_BM250_qterm AS SELECT ALL a1, prob FROM rank_DOC_BM250_qterm_1;

-- rank_DOC_BM250_tf = nyt0_DATA_tf_bm25
CREATE VIEW rank_DOC_BM250_tf AS SELECT ALL a1, a2, prob FROM nyt0_DATA_tf_bm25;

-- rank_DOC_BM250_idf = nyt0_DATA_idf_bm25
CREATE VIEW rank_DOC_BM250_idf AS SELECT ALL a1, prob FROM nyt0_DATA_idf_bm25;

-- rank_DOC_BM250_weighted_qterm_1 = JOIN[$1=$1](rank_DOC_BM250_qterm,rank_DOC_BM250_idf)
CREATE VIEW rank_DOC_BM250_weighted_qterm_1 AS SELECT ALL rank_DOC_BM250_qterm.a1 AS a1, rank_DOC_BM250_idf.a1 AS a2, rank_DOC_BM250_qterm.prob * rank_DOC_BM250_idf.prob AS prob FROM rank_DOC_BM250_qterm, rank_DOC_BM250_idf WHERE rank_DOC_BM250_qterm.a1=rank_DOC_BM250_idf.a1;

-- rank_DOC_BM250_weighted_qterm = PROJECT[$1](rank_DOC_BM250_weighted_qterm_1)
CREATE VIEW rank_DOC_BM250_weighted_qterm AS SELECT ALL a1, prob FROM rank_DOC_BM250_weighted_qterm_1;

-- rank_DOC_BM250_norm_weighted_qterm_L = PROJECT[$1](rank_DOC_BM250_weighted_qterm)
CREATE VIEW rank_DOC_BM250_norm_weighted_qterm_L AS SELECT ALL a1, prob FROM rank_DOC_BM250_weighted_qterm;

-- rank_DOC_BM250_norm_weighted_qterm_R_2 = PROJECT[$1](rank_DOC_BM250_weighted_qterm)
CREATE VIEW rank_DOC_BM250_norm_weighted_qterm_R_2 AS SELECT ALL a1, prob FROM rank_DOC_BM250_weighted_qterm;

-- rank_DOC_BM250_norm_weighted_qterm_R_1 = PROJECT DISJOINT[](rank_DOC_BM250_norm_weighted_qterm_R_2)
CREATE VIEW rank_DOC_BM250_norm_weighted_qterm_R_1 AS SELECT ALL sum(prob) AS prob FROM rank_DOC_BM250_norm_weighted_qterm_R_2;

-- rank_DOC_BM250_norm_weighted_qterm_R = PROJECT INVERSE(rank_DOC_BM250_norm_weighted_qterm_R_1)
CREATE VIEW rank_DOC_BM250_norm_weighted_qterm_R AS SELECT ALL 1/prob AS prob FROM rank_DOC_BM250_norm_weighted_qterm_R_1;

-- rank_DOC_BM250_norm_weighted_qterm = JOIN[](rank_DOC_BM250_norm_weighted_qterm_L,rank_DOC_BM250_norm_weighted_qterm_R)
CREATE VIEW rank_DOC_BM250_norm_weighted_qterm AS SELECT ALL rank_DOC_BM250_norm_weighted_qterm_L.a1 AS a1, rank_DOC_BM250_norm_weighted_qterm_L.prob * rank_DOC_BM250_norm_weighted_qterm_R.prob AS prob FROM rank_DOC_BM250_norm_weighted_qterm_L, rank_DOC_BM250_norm_weighted_qterm_R;

-- rank_DOC_BM250_scores_1 = JOIN[$1=$1](rank_DOC_BM250_norm_weighted_qterm,rank_DOC_BM250_tf)
CREATE VIEW rank_DOC_BM250_scores_1 AS SELECT ALL rank_DOC_BM250_norm_weighted_qterm.a1 AS a1, rank_DOC_BM250_tf.a1 AS a2, rank_DOC_BM250_tf.a2 AS a3, rank_DOC_BM250_norm_weighted_qterm.prob * rank_DOC_BM250_tf.prob AS prob FROM rank_DOC_BM250_norm_weighted_qterm, rank_DOC_BM250_tf WHERE rank_DOC_BM250_norm_weighted_qterm.a1=rank_DOC_BM250_tf.a1;

-- rank_DOC_BM250_scores = PROJECT SUM[$3](rank_DOC_BM250_scores_1)
CREATE VIEW rank_DOC_BM250_scores AS SELECT ALL a3 AS a1, sum(prob) AS prob FROM rank_DOC_BM250_scores_1 GROUP BY a3;

-- rank_DOC_BM250_filtered_scores_1 = JOIN[$1=$1](rank_DOC_BM250_scores,nyt0_DATA_result)
CREATE VIEW rank_DOC_BM250_filtered_scores_1 AS SELECT ALL rank_DOC_BM250_scores.a1 AS a1, nyt0_DATA_result.a1 AS a2, rank_DOC_BM250_scores.prob * nyt0_DATA_result.prob AS prob FROM rank_DOC_BM250_scores, nyt0_DATA_result WHERE rank_DOC_BM250_scores.a1=nyt0_DATA_result.a1;

-- rank_DOC_BM250_filtered_scores = PROJECT[$1](rank_DOC_BM250_filtered_scores_1)
CREATE VIEW rank_DOC_BM250_filtered_scores AS SELECT ALL a1, prob FROM rank_DOC_BM250_filtered_scores_1;

-- rank_DOC_BM250_RETRIEVE_result_R_1 = PROJECT DISJOINT[](rank_DOC_BM250_filtered_scores)
CREATE VIEW rank_DOC_BM250_RETRIEVE_result_R_1 AS SELECT ALL sum(prob) AS prob FROM rank_DOC_BM250_filtered_scores;

-- rank_DOC_BM250_RETRIEVE_result_R = PROJECT INVERSE(rank_DOC_BM250_RETRIEVE_result_R_1)
CREATE VIEW rank_DOC_BM250_RETRIEVE_result_R AS SELECT ALL 1/prob AS prob FROM rank_DOC_BM250_RETRIEVE_result_R_1;

-- rank_DOC_BM250_RETRIEVE_result = JOIN[](rank_DOC_BM250_filtered_scores,rank_DOC_BM250_RETRIEVE_result_R)
CREATE VIEW rank_DOC_BM250_RETRIEVE_result AS SELECT ALL rank_DOC_BM250_filtered_scores.a1 AS a1, rank_DOC_BM250_filtered_scores.prob * rank_DOC_BM250_RETRIEVE_result_R.prob AS prob FROM rank_DOC_BM250_filtered_scores, rank_DOC_BM250_RETRIEVE_result_R;






-- The following crahes:
SELECT * FROM rank_DOC_BM250_RETRIEVE_result LIMIT 10;

-- The following works:
-- EXPLAIN SELECT * FROM rank_DOC_BM250_RETRIEVE_result;

