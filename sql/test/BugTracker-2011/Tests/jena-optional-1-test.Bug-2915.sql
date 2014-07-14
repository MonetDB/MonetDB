START TRANSACTION;
CREATE TABLE "nodes" (
	"hash"     BIGINT        NOT NULL,
	"lex"      CHARACTER LARGE OBJECT NOT NULL,
	"lang"     VARCHAR(10)   NOT NULL,
	"datatype" VARCHAR(200),
	"type"     INTEGER       NOT NULL,
	CONSTRAINT "nodes_hash_pkey" PRIMARY KEY ("hash")
);
COPY 13 RECORDS INTO "nodes" FROM stdin USING DELIMITERS '\t','\n','"';
-8395209716130787220	"http://example/x"	""	""	2
2004134117598721274	"http://example/p"	""	""	2
435905340492217258	"1"	""	"http://www.w3.org/2001/XMLSchema#integer"	5
745852752491398227	"2"	""	"http://www.w3.org/2001/XMLSchema#integer"	5
-5334307821581591471	"3"	""	"http://www.w3.org/2001/XMLSchema#integer"	5
4788264553748351656	"http://example/a"	""	""	2
8936870869765386580	"http://example/b"	""	""	2
3816485599920428794	"http://example/q1"	""	""	2
-5216419694041718175	"http://example/z1"	""	""	2
-8287201118401564753	"http://example/q2"	""	""	2
7121703792433320712	"http://example/z2"	""	""	2
-4064636373028764940	"http://example/z"	""	""	2
-3401798235167296541	"abc"	""	""	3
CREATE TABLE "triples" (
	"s" BIGINT        NOT NULL,
	"p" BIGINT        NOT NULL,
	"o" BIGINT        NOT NULL,
	CONSTRAINT "triples_s_p_o_pkey" PRIMARY KEY ("s", "p", "o")
);
CREATE INDEX "objsubj" ON "triples" ("o", "s");
CREATE INDEX "predobj" ON "triples" ("p", "o");
COPY 9 RECORDS INTO "triples" FROM stdin USING DELIMITERS '\t','\n','"';
-8395209716130787220	2004134117598721274	435905340492217258
-8395209716130787220	2004134117598721274	745852752491398227
-8395209716130787220	2004134117598721274	-5334307821581591471
-8395209716130787220	2004134117598721274	4788264553748351656
-8395209716130787220	2004134117598721274	8936870869765386580
4788264553748351656	3816485599920428794	-5216419694041718175
4788264553748351656	-8287201118401564753	7121703792433320712
8936870869765386580	3816485599920428794	-5216419694041718175
-4064636373028764940	2004134117598721274	-3401798235167296541
CREATE TABLE "quads" (
	"g" BIGINT        NOT NULL,
	"s" BIGINT        NOT NULL,
	"p" BIGINT        NOT NULL,
	"o" BIGINT        NOT NULL,
	CONSTRAINT "quads_g_s_p_o_pkey" PRIMARY KEY ("g", "s", "p", "o")
);
CREATE INDEX "graobjsubj" ON "quads" ("g", "o", "s");
CREATE INDEX "grapredobj" ON "quads" ("g", "p", "o");
CREATE INDEX "objsubjpred" ON "quads" ("o", "s", "p");
CREATE INDEX "predobjsubj" ON "quads" ("p", "o", "s");
CREATE INDEX "subjpredobj" ON "quads" ("s", "p", "o");

SELECT R_1.lex AS V_1_lex, R_1.datatype AS V_1_datatype,
R_1.lang AS V_1_lang, R_1.type AS V_1_type, 
  R_2.lex AS V_2_lex, R_2.datatype AS V_2_datatype, R_2.lang AS V_2_lang,
R_2.type AS V_2_type
FROM
    ( SELECT T_1.s AS X_1
      FROM Triples AS T_1
      WHERE ( T_1.p = 2004134117598721274
         AND T_1.o = 435905340492217258 )
    ) AS T_1
  LEFT OUTER JOIN
    Triples AS T_2
  ON ( T_2.p = 3816485599920428794
   AND T_1.X_1 = T_2.s )
  LEFT OUTER JOIN
    Nodes AS R_1
  ON ( T_1.X_1 = R_1.hash )
  LEFT OUTER JOIN
    Nodes AS R_2
  ON ( T_2.o = R_2.hash )
;

ROLLBACK;
