start transaction;

CREATE TABLE "sys"."params_str" (
	"paramname" CHARACTER LARGE OBJECT,
	"value"     CHARACTER LARGE OBJECT,
	"prob"      DOUBLE
);
CREATE TABLE "sys"."bm_0_obj_dict" (
	"id"    INTEGER       NOT NULL,
	"idstr" CHARACTER LARGE OBJECT NOT NULL,
	"prob"  FLOAT(51)     NOT NULL,
	CONSTRAINT "bm_0_obj_dict_id_pkey" PRIMARY KEY ("id"),
	CONSTRAINT "bm_0_obj_dict_idstr_unique" UNIQUE ("idstr")
);
CREATE TABLE "sys"."bm_0_obj_type" (
	"id"      INTEGER       NOT NULL,
	"type"    INTEGER       NOT NULL,
	"typestr" CHARACTER LARGE OBJECT NOT NULL,
	"prob"    DOUBLE        NOT NULL,
	CONSTRAINT "bm_0_obj_type_id_fkey" FOREIGN KEY ("id") REFERENCES "sys"."bm_0_obj_dict" ("id"),
	CONSTRAINT "bm_0_obj_type_type_fkey" FOREIGN KEY ("type") REFERENCES "sys"."bm_0_obj_dict" ("id")
);
CREATE TABLE "sys"."tr_0_obj_dict" (
	"id"    INTEGER       NOT NULL,
	"idstr" CHARACTER LARGE OBJECT NOT NULL,
	"prob"  FLOAT(51)     NOT NULL,
	CONSTRAINT "tr_0_obj_dict_id_pkey" PRIMARY KEY ("id"),
	CONSTRAINT "tr_0_obj_dict_idstr_unique" UNIQUE ("idstr")
);
CREATE TABLE "sys"."tr_0_obj_type" (
	"id"      INTEGER       NOT NULL,
	"type"    INTEGER       NOT NULL,
	"typestr" CHARACTER LARGE OBJECT NOT NULL,
	"prob"    DOUBLE        NOT NULL,
	CONSTRAINT "tr_0_obj_type_id_fkey" FOREIGN KEY ("id") REFERENCES "sys"."tr_0_obj_dict" ("id"),
	CONSTRAINT "tr_0_obj_type_type_fkey" FOREIGN KEY ("type") REFERENCES "sys"."tr_0_obj_dict" ("id")
);

CREATE TABLE "sys"."_cachedrel_4" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_5" (
	"a1"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_6" (
	"a1"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_7" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_8" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_9" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_10" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"a3"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_11" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"a3"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_12" (
	"a1"   CHARACTER LARGE OBJECT,
	"a2"   CHARACTER LARGE OBJECT,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_13" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_14" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_15" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_16" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_17" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_18" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_19" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"a3"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_20" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"a3"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_21" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"a3"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_22" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"a3"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_23" (
	"a1"   CHARACTER LARGE OBJECT,
	"a2"   CHARACTER LARGE OBJECT,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_24" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_25" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_26" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_27" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_28" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_29" (
	"a1"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_30" (
	"a1"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_31" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_32" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_34" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_35" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   BIGINT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_36" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   BIGINT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_37" (
	"a1"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_38" (
	"a1"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_39" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_40" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_41" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_42" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_43" (
	"a1"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_44" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_46" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_47" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_48" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_49" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_50" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_51" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_52" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_54" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_56" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_57" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_58" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_59" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_60" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_61" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_62" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_63" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_64" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_65" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_66" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_67" (
	"a1"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_68" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" FLOAT(53,2)
);
CREATE TABLE "sys"."_cachedrel_69" (
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_70" (
	"a1"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_71" (
	"a1"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_72" (
	"a1"   INTEGER,
	"prob" TINYINT
);
CREATE TABLE "sys"."_cachedrel_73" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" FLOAT(53,2)
);
CREATE TABLE "sys"."_cachedrel_74" (
	"a1"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_75" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_76" (
	"a1"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_77" (
	"a1"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_78" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_79" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_80" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_81" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_82" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_86" (
	"a1"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_87" (
	"a1"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_88" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_89" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_90" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_91" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_92" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_93" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_94" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_95" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_97" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_98" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_99" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_100" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_101" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_102" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_103" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_104" (
	"a1"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_105" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" FLOAT(53,2)
);
CREATE TABLE "sys"."_cachedrel_106" (
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_107" (
	"a1"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_108" (
	"a1"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_109" (
	"a1"   INTEGER,
	"prob" TINYINT
);
CREATE TABLE "sys"."_cachedrel_110" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" FLOAT(53,2)
);
CREATE TABLE "sys"."_cachedrel_111" (
	"a1"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_112" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_113" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_115" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_116" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_117" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_118" (
	"a1"   INTEGER,
	"a2"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_119" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_120" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_121" (
	"a1"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_122" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" FLOAT(53,2)
);
CREATE TABLE "sys"."_cachedrel_123" (
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_124" (
	"a1"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_125" (
	"a1"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_126" (
	"a1"   INTEGER,
	"prob" TINYINT
);
CREATE TABLE "sys"."_cachedrel_127" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" FLOAT(53,2)
);
CREATE TABLE "sys"."_cachedrel_128" (
	"a1"   INTEGER,
	"prob" FLOAT(53,1)
);
CREATE TABLE "sys"."_cachedrel_130" (
	"a1"   CHAR(1),
	"prob" DECIMAL(18,3)
);
CREATE TABLE "sys"."_cachedrel_134" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_135" (
	"a1"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_136" (
	"a1"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_137" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_138" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_139" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_140" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_143" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_144" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_145" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_146" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_149" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_150" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_151" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_152" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_153" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_156" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_157" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_158" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_159" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_161" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_162" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_163" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_165" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_166" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_167" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_168" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_169" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_170" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_172" (
	"a1"   CHAR(1),
	"prob" DECIMAL(18,3)
);
CREATE TABLE "sys"."_cachedrel_173" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_174" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_175" (
	"a1"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_176" (
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_177" (
	"a1"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_178" (
	"a1"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_180" (
	"a1"   CHAR(1),
	"prob" DECIMAL(18,3)
);
CREATE TABLE "sys"."_cachedrel_181" (
	"a1"   INTEGER,
	"prob" FLOAT(51)
);
CREATE TABLE "sys"."_cachedrel_182" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
CREATE TABLE "sys"."_cachedrel_183" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);

create or replace function pcre_replace(s string, pattern string, repl string, flags string) returns string external name pcre."replace";

create view _cachedrel_1 as select idstr as a1, id as a2, prob from bm_0_obj_dict;
create view _cachedrel_2 as select a2 as a1, prob from _cachedrel_1;
create view _cachedrel_3 as select id as a1, type as a2, typestr as a3, prob from bm_0_obj_type;
create view _cachedrel_33 as select a1, a2, prob from _cachedrel_32;
create view _cachedrel_45 as select a1, a1 as a2, prob from _cachedrel_43;
create view _cachedrel_53 as select a1, prob from (values ('1',1.0)) as t__x41(a1,prob);
create view _cachedrel_55 as select a1, prob from (values ('2',1.0)) as t__x44(a1,prob);
create view _cachedrel_83 as select a1, a2, prob from _cachedrel_82;
create view _cachedrel_84 as select a1, prob from (values ('Functionele naam',1.0)) as t__x78(a1,prob);
create view _cachedrel_85 as select a1, a3 as a2, prob from _cachedrel_15;
create view _cachedrel_96 as select a1, a2, prob from _cachedrel_95;
create view _cachedrel_114 as select a1, a2, prob from _cachedrel_113;
create view _cachedrel_129 as select a1, prob from (values ('1',300.0),('2',100.0),('3',400.0),('4',100.0),('5',200.0),('6',10.0),('7',100.0)) as t__x132(a1,prob);
create view _cachedrel_131 as select idstr as a1, id as a2, prob from tr_0_obj_dict;
create view _cachedrel_132 as select a2 as a1, prob from _cachedrel_131;
create view _cachedrel_133 as select id as a1, type as a2, typestr as a3, prob from tr_0_obj_type;
create view _cachedrel_141 as select a2 as a1, prob from _cachedrel_140;
create view _cachedrel_142 as select a1, a2, prob from _cachedrel_140;
create view _cachedrel_147 as select a2 as a1, prob from _cachedrel_146;
create view _cachedrel_148 as select a1, a2, prob from _cachedrel_146;
create view _cachedrel_154 as select a2 as a1, prob from _cachedrel_153;
create view _cachedrel_155 as select a1, a2, prob from _cachedrel_153;
create view _cachedrel_160 as select a2 as a1, a1 as a2, prob from _cachedrel_159;
create view _cachedrel_164 as select a1, a3 as a2, prob from _cachedrel_163;
create view _cachedrel_171 as select a1, prob from (values ('1',5.0),('2',1.0)) as t__x171(a1,prob);
create view _cachedrel_179 as select a1, prob from (values ('1',5.0),('2',2.0),('3',1.0)) as t__x179(a1,prob);
create view _cachedrel_184 as select a1, a3 as a2, prob from _cachedrel_183;
create view _cachedrel_185 as select a1, a3 as a2, prob from _cachedrel_26;


CREATE VIEW s0_ifThenElse_1_RESULT_result AS 
WITH 
q0_x0 AS (SELECT 0 AS a1, a2, prob FROM (SELECT paramName AS a1, value AS a2, prob FROM params_str WHERE paramName = 's0_keyword') AS t__x7),
q0_x1 AS (SELECT a1, a2, prob FROM q0_x0),
q0_x2 AS (SELECT a2 AS a1, prob FROM q0_x1),
q0_x3 AS (SELECT a1, prob FROM (SELECT q0_x2.a1 AS a1, q0_x2.prob / t__x10.prob AS prob FROM q0_x2,(SELECT max(prob) AS prob FROM q0_x2) AS t__x10) AS t__x11),
q0_x4 AS (SELECT a1||a2 AS a1, prob FROM (SELECT t__x12_1.a1 AS a1, t__x12_2.a1 AS a2, t__x12_1.prob * t__x12_2.prob AS prob FROM q0_x3 AS t__x12_1,
q0_x3 AS t__x12_2 WHERE t__x12_1.a1 <> t__x12_2.a1) AS t__x13),
q0_x5 AS (SELECT a1, prob FROM q0_x3 UNION ALL SELECT a1, prob FROM q0_x4),
q0_x6 AS (SELECT lcase(a1) AS a1, prob FROM q0_x5),
q0_x7 AS (SELECT a1, prob FROM q0_x6),
q0_x8 AS (SELECT a2 AS a1, a4 AS a2, max(prob) AS prob FROM (SELECT _cachedrel_12.a1 AS a1, _cachedrel_12.a2 AS a2, _cachedrel_12.a3 AS a3, _cachedrel_12.a4 AS a4, q0_x7.a1 AS a5, _cachedrel_12.prob * q0_x7.prob AS prob FROM _cachedrel_12,q0_x7 WHERE _cachedrel_12.a1 = q0_x7.a1) AS t__x15 GROUP BY a2, a4),
q0_x9 AS (SELECT a1, max(prob) AS prob FROM (SELECT a2 AS a1, prob FROM q0_x8 UNION ALL SELECT a1, prob FROM q0_x5) AS t__x16 GROUP BY a1),
q0_x10 AS (SELECT a1, prob FROM q0_x6),
q0_x11 AS (SELECT a2 AS a1, a4 AS a2, max(prob) AS prob FROM (SELECT _cachedrel_23.a1 AS a1, _cachedrel_23.a2 AS a2, _cachedrel_23.a3 AS a3, _cachedrel_23.a4 AS a4, q0_x10.a1 AS a5, _cachedrel_23.prob * q0_x10.prob AS prob FROM _cachedrel_23,q0_x10 WHERE _cachedrel_23.a1 = q0_x10.a1) AS t__x18 GROUP BY a2, a4),
q0_x12 AS (SELECT a1, max(prob) AS prob FROM (SELECT a2 AS a1, prob FROM q0_x11 UNION ALL SELECT a1, prob FROM q0_x5) AS t__x19 GROUP BY a1),
q0_x13 AS (SELECT a1, max(prob) AS prob FROM (SELECT a1, prob FROM q0_x9 UNION ALL SELECT a1, prob FROM q0_x12) AS t__x20 GROUP BY a1),
q0_x14 AS (SELECT lcase(a1) AS a1, prob FROM q0_x13),
q0_x15 AS (SELECT a1, prob FROM (SELECT _cachedrel_27.a1 AS a1, _cachedrel_27.a2 AS a2, q0_x14.a1 AS a3, _cachedrel_27.prob * q0_x14.prob AS prob FROM _cachedrel_27,q0_x14 WHERE _cachedrel_27.a2 = q0_x14.a1) AS t__x22),
q0_x16 AS (SELECT a1, prob FROM (SELECT _cachedrel_6.a1 AS a1, q0_x15.a1 AS a2, _cachedrel_6.prob * q0_x15.prob AS prob FROM _cachedrel_6,q0_x15 WHERE _cachedrel_6.a1 = q0_x15.a1) AS t__x24),
q0_x115 AS (SELECT CAST(count(prob) AS DOUBLE) AS prob FROM q0_x16),
q0_x104 AS (SELECT a1, prob FROM (SELECT _cachedrel_184.a1 AS a1, _cachedrel_184.a2 AS a2, q0_x13.a1 AS a3, _cachedrel_184.prob * q0_x13.prob AS prob FROM _cachedrel_184,q0_x13 WHERE _cachedrel_184.a2 = q0_x13.a1) AS t__x26),
q0_x105 AS (SELECT a1, prob FROM (SELECT _cachedrel_77.a1 AS a1, q0_x104.a1 AS a2, _cachedrel_77.prob * q0_x104.prob AS prob FROM _cachedrel_77,q0_x104 WHERE _cachedrel_77.a1 = q0_x104.a1) AS t__x28),
q0_x108 AS (SELECT a1, prob FROM (SELECT q0_x105.a1 AS a1, _cachedrel_53.a1 AS a2, q0_x105.prob * _cachedrel_53.prob AS prob FROM q0_x105,_cachedrel_53) AS t__x30),
q0_x106 AS (SELECT a1, prob FROM (SELECT _cachedrel_185.a1 AS a1, _cachedrel_185.a2 AS a2, q0_x13.a1 AS a3, _cachedrel_185.prob * q0_x13.prob AS prob FROM _cachedrel_185,q0_x13 WHERE _cachedrel_185.a2 = q0_x13.a1) AS t__x32),
q0_x107 AS (SELECT a1, prob FROM (SELECT _cachedrel_77.a1 AS a1, q0_x106.a1 AS a2, _cachedrel_77.prob * q0_x106.prob AS prob FROM _cachedrel_77,q0_x106 WHERE _cachedrel_77.a1 = q0_x106.a1) AS t__x33),
q0_x109 AS (SELECT a1, prob FROM (SELECT q0_x107.a1 AS a1, _cachedrel_55.a1 AS a2, q0_x107.prob * _cachedrel_55.prob AS prob FROM q0_x107,_cachedrel_55) AS t__x35),
q0_x110 AS (SELECT a1, prob FROM q0_x108 UNION ALL SELECT a1, prob FROM q0_x109),
q0_x111 AS (SELECT a1, prob FROM q0_x110),
q0_x112 AS (SELECT CAST(count(prob) AS DOUBLE) AS prob FROM q0_x111),
q0_x17 AS (SELECT 0 AS a1, a1 AS a2, prob FROM q0_x13),
q0_x18 AS (SELECT a1, a2, prob FROM q0_x17),
q0_x19 AS (SELECT a1, a2, prob FROM q0_x18),
q0_x20 AS (SELECT a1, lcase(a2) AS a2, prob FROM q0_x19),
q0_x21 AS (SELECT a1, a3 AS a2, prob FROM (SELECT q0_x20.a1 AS a1, q0_x20.a2 AS a2, _cachedrel_64.a1 AS a3, _cachedrel_64.a2 AS a4, q0_x20.prob * _cachedrel_64.prob AS prob FROM q0_x20,_cachedrel_64 WHERE q0_x20.a2 = _cachedrel_64.a2) AS t__x38),
q0_x22 AS (SELECT a1, a2, sum(prob) AS prob FROM q0_x21 GROUP BY a1, a2),
q0_x23 AS (SELECT a1, a2, 1 AS prob FROM q0_x22),
q0_x24 AS (SELECT a1, 1 AS prob FROM (SELECT a2 AS a1, max(prob) AS prob FROM q0_x22 GROUP BY a2) AS t__x39),
q0_x25 AS (SELECT a1, a2, prob FROM (SELECT q0_x23.a1 AS a1, q0_x23.a2 AS a2, q0_x24.a1 AS a3, q0_x23.prob * q0_x24.prob AS prob FROM q0_x23,q0_x24 WHERE q0_x23.a2 = q0_x24.a1) AS t__x41),
q0_x26 AS (SELECT a1, a3 AS a2, sum(prob) AS prob FROM (SELECT _cachedrel_73.a1 AS a1, _cachedrel_73.a2 AS a2, q0_x25.a1 AS a3, q0_x25.a2 AS a4, _cachedrel_73.prob * q0_x25.prob AS prob FROM _cachedrel_73,q0_x25 WHERE _cachedrel_73.a2 = q0_x25.a2) AS t__x43 GROUP BY a1, a3),
q0_x27 AS (SELECT a1, prob FROM (SELECT t__x44.a1 AS a1, t__x44.a2 AS a2, _cachedrel_74.a1 AS a3, t__x44.prob * _cachedrel_74.prob AS prob FROM (SELECT a1, a2, prob FROM q0_x26 WHERE a2 = 0) AS t__x44,_cachedrel_74 WHERE t__x44.a1 = _cachedrel_74.a1) AS t__x45),
q0_x28 AS (SELECT a1, prob FROM (SELECT q0_x27.a1 AS a1, q0_x27.prob / t__x47.prob AS prob FROM q0_x27,(SELECT max(prob) AS prob FROM q0_x27) AS t__x47) AS t__x48),
q0_x61 AS (SELECT a1, prob FROM (SELECT q0_x28.a1 AS a1, t__x50.a1 AS a2, q0_x28.prob * t__x50.prob AS prob FROM q0_x28,(SELECT a1, prob FROM _cachedrel_130 WHERE a1 = '1') AS t__x50) AS t__x51),
q0_x29 AS (SELECT _cachedrel_83.a1 AS a1, _cachedrel_83.a2 AS a2, q0_x13.a1 AS a3, _cachedrel_83.prob * q0_x13.prob AS prob FROM _cachedrel_83,q0_x13 WHERE [_cachedrel_83.a2] contains [q0_x13.a1,false]),
q0_x30 AS (SELECT a1, a2, '^.*(?<![\\pL\\d])'||a3||'(?![\\pL\\d]).*$' AS a3, prob FROM (SELECT a1, a2, pcre_replace(a3,'(\\$|\\^|\\+|\\.|\\?|\\*|\\(|\\)|\\[|\\]|\\{|\\}|\\\\)','\\\\\\1','') AS a3, prob FROM q0_x29) AS t__x53),
q0_x31 AS (SELECT a1, a2, a3, prob FROM q0_x30 WHERE pcre_imatch(a2,a3)),
q0_x32 AS (SELECT a1, a2, max(prob) AS prob FROM q0_x31 GROUP BY a1, a2),
q0_x33 AS (SELECT a1, prob FROM (SELECT a1, prob FROM q0_x32) AS t__x54),
q0_x62 AS (SELECT a1, prob FROM (SELECT q0_x33.a1 AS a1, t__x56.a1 AS a2, q0_x33.prob * t__x56.prob AS prob FROM q0_x33,(SELECT a1, prob FROM _cachedrel_130 WHERE a1 = '2') AS t__x56) AS t__x57),
q0_x34 AS (SELECT a1, a2, prob FROM q0_x17),
q0_x35 AS (SELECT a1, a2, prob FROM q0_x34),
q0_x36 AS (SELECT a1, lcase(a2) AS a2, prob FROM q0_x35),
q0_x37 AS (SELECT a1, a3 AS a2, prob FROM (SELECT q0_x36.a1 AS a1, q0_x36.a2 AS a2, _cachedrel_101.a1 AS a3, _cachedrel_101.a2 AS a4, q0_x36.prob * _cachedrel_101.prob AS prob FROM q0_x36,_cachedrel_101 WHERE q0_x36.a2 = _cachedrel_101.a2) AS t__x60),
q0_x38 AS (SELECT a1, a2, sum(prob) AS prob FROM q0_x37 GROUP BY a1, a2),
q0_x39 AS (SELECT a1, a2, 1 AS prob FROM q0_x38),
q0_x40 AS (SELECT a1, 1 AS prob FROM (SELECT a2 AS a1, max(prob) AS prob FROM q0_x38 GROUP BY a2) AS t__x61),
q0_x41 AS (SELECT a1, a2, prob FROM (SELECT q0_x39.a1 AS a1, q0_x39.a2 AS a2, q0_x40.a1 AS a3, q0_x39.prob * q0_x40.prob AS prob FROM q0_x39,q0_x40 WHERE q0_x39.a2 = q0_x40.a1) AS t__x63),
q0_x42 AS (SELECT a1, a3 AS a2, sum(prob) AS prob FROM (SELECT _cachedrel_110.a1 AS a1, _cachedrel_110.a2 AS a2, q0_x41.a1 AS a3, q0_x41.a2 AS a4, _cachedrel_110.prob * q0_x41.prob AS prob FROM _cachedrel_110,q0_x41 WHERE _cachedrel_110.a2 = q0_x41.a2) AS t__x65 GROUP BY a1, a3),
q0_x43 AS (SELECT a1, prob FROM (SELECT t__x66.a1 AS a1, t__x66.a2 AS a2, _cachedrel_111.a1 AS a3, t__x66.prob * _cachedrel_111.prob AS prob FROM (SELECT a1, a2, prob FROM q0_x42 WHERE a2 = 0) AS t__x66,_cachedrel_111 WHERE t__x66.a1 = _cachedrel_111.a1) AS t__x67),
q0_x44 AS (SELECT a1, prob FROM (SELECT q0_x43.a1 AS a1, q0_x43.prob / t__x69.prob AS prob FROM q0_x43,(SELECT max(prob) AS prob FROM q0_x43) AS t__x69) AS t__x70),
q0_x63 AS (SELECT a1, prob FROM (SELECT q0_x44.a1 AS a1, t__x72.a1 AS a2, q0_x44.prob * t__x72.prob AS prob FROM q0_x44,(SELECT a1, prob FROM _cachedrel_130 WHERE a1 = '3') AS t__x72) AS t__x73),
q0_x45 AS (SELECT a1, a2, prob FROM q0_x18),
q0_x46 AS (SELECT a1, lcase(a2) AS a2, prob FROM q0_x45),
q0_x47 AS (SELECT a1, a3 AS a2, prob FROM (SELECT q0_x46.a1 AS a1, q0_x46.a2 AS a2, _cachedrel_118.a1 AS a3, _cachedrel_118.a2 AS a4, q0_x46.prob * _cachedrel_118.prob AS prob FROM q0_x46,_cachedrel_118 WHERE q0_x46.a2 = _cachedrel_118.a2) AS t__x75),
q0_x48 AS (SELECT a1, a2, sum(prob) AS prob FROM q0_x47 GROUP BY a1, a2),
q0_x49 AS (SELECT a1, a2, 1 AS prob FROM q0_x48),
q0_x50 AS (SELECT a1, 1 AS prob FROM (SELECT a2 AS a1, max(prob) AS prob FROM q0_x48 GROUP BY a2) AS t__x76),
q0_x51 AS (SELECT a1, a2, prob FROM (SELECT q0_x49.a1 AS a1, q0_x49.a2 AS a2, q0_x50.a1 AS a3, q0_x49.prob * q0_x50.prob AS prob FROM q0_x49,q0_x50 WHERE q0_x49.a2 = q0_x50.a1) AS t__x78),
q0_x52 AS (SELECT a1, a3 AS a2, sum(prob) AS prob FROM (SELECT _cachedrel_127.a1 AS a1, _cachedrel_127.a2 AS a2, q0_x51.a1 AS a3, q0_x51.a2 AS a4, _cachedrel_127.prob * q0_x51.prob AS prob FROM _cachedrel_127,q0_x51 WHERE _cachedrel_127.a2 = q0_x51.a2) AS t__x80 GROUP BY a1, a3),
q0_x53 AS (SELECT a1, prob FROM (SELECT t__x81.a1 AS a1, t__x81.a2 AS a2, _cachedrel_128.a1 AS a3, t__x81.prob * _cachedrel_128.prob AS prob FROM (SELECT a1, a2, prob FROM q0_x52 WHERE a2 = 0) AS t__x81,_cachedrel_128 WHERE t__x81.a1 = _cachedrel_128.a1) AS t__x82),
q0_x54 AS (SELECT a1, prob FROM (SELECT q0_x53.a1 AS a1, q0_x53.prob / t__x84.prob AS prob FROM q0_x53,(SELECT max(prob) AS prob FROM q0_x53) AS t__x84) AS t__x85),
q0_x64 AS (SELECT a1, prob FROM (SELECT q0_x54.a1 AS a1, t__x87.a1 AS a2, q0_x54.prob * t__x87.prob AS prob FROM q0_x54,(SELECT a1, prob FROM _cachedrel_130 WHERE a1 = '4') AS t__x87) AS t__x88),
q0_x55 AS (SELECT _cachedrel_114.a1 AS a1, _cachedrel_114.a2 AS a2, q0_x13.a1 AS a3, _cachedrel_114.prob * q0_x13.prob AS prob FROM _cachedrel_114,q0_x13 WHERE [_cachedrel_114.a2] contains [q0_x13.a1,false]),
q0_x56 AS (SELECT a1, a2, max(prob) AS prob FROM q0_x55 GROUP BY a1, a2),
q0_x57 AS (SELECT a1, prob FROM (SELECT a1, prob FROM q0_x56) AS t__x90),
q0_x65 AS (SELECT a1, prob FROM (SELECT q0_x57.a1 AS a1, t__x92.a1 AS a2, q0_x57.prob * t__x92.prob AS prob FROM q0_x57,(SELECT a1, prob FROM _cachedrel_130 WHERE a1 = '5') AS t__x92) AS t__x93),
q0_x58 AS (SELECT _cachedrel_60.a1 AS a1, _cachedrel_60.a2 AS a2, q0_x13.a1 AS a3, _cachedrel_60.prob * q0_x13.prob AS prob FROM _cachedrel_60,q0_x13 WHERE [_cachedrel_60.a2] contains [q0_x13.a1,false]),
q0_x59 AS (SELECT a1, a2, max(prob) AS prob FROM q0_x58 GROUP BY a1, a2),
q0_x60 AS (SELECT a1, prob FROM (SELECT a1, prob FROM q0_x59) AS t__x95),
q0_x66 AS (SELECT a1, prob FROM (SELECT q0_x60.a1 AS a1, t__x97.a1 AS a2, q0_x60.prob * t__x97.prob AS prob FROM q0_x60,(SELECT a1, prob FROM _cachedrel_130 WHERE a1 = '6') AS t__x97) AS t__x98),
q0_x67 AS (SELECT a1, prob FROM q0_x61 UNION ALL (SELECT a1, prob FROM q0_x62 UNION ALL (SELECT a1, prob FROM q0_x63 UNION ALL (SELECT a1, prob FROM q0_x64 UNION ALL (SELECT a1, prob FROM q0_x65 UNION ALL SELECT a1, prob FROM q0_x66))))),
q0_x68 AS (SELECT a1, sum(prob) AS prob FROM q0_x67 GROUP BY a1),
q0_x69 AS (SELECT a1, prob FROM (SELECT q0_x68.a1 AS a1, _cachedrel_53.a1 AS a2, q0_x68.prob * _cachedrel_53.prob AS prob FROM q0_x68,_cachedrel_53) AS t__x100),
q0_x70 AS (SELECT a1, prob FROM q0_x69),
q0_x96 AS (SELECT a1, prob FROM (SELECT q0_x70.a1 AS a1, q0_x70.prob / t__x102.prob AS prob FROM q0_x70,(SELECT max(prob) AS prob FROM q0_x70) AS t__x102) AS t__x103),
q0_x99 AS (SELECT a1, prob FROM (SELECT q0_x96.a1 AS a1, t__x105.a1 AS a2, q0_x96.prob * t__x105.prob AS prob FROM q0_x96,(SELECT a1, prob FROM _cachedrel_180 WHERE a1 = '1') AS t__x105) AS t__x106),
q0_x71 AS (SELECT a2 AS a1, prob FROM (SELECT paramName AS a1, value AS a2, prob FROM params_str WHERE paramName = 's0_userid') AS t__x107),
q0_x72 AS (SELECT a1, prob FROM (SELECT _cachedrel_164.a1 AS a1, _cachedrel_164.a2 AS a2, q0_x71.a1 AS a3, _cachedrel_164.prob * q0_x71.prob AS prob FROM _cachedrel_164,q0_x71 WHERE _cachedrel_164.a2 = q0_x71.a1) AS t__x109),
q0_x73 AS (SELECT a1, prob FROM (SELECT _cachedrel_136.a1 AS a1, q0_x72.a1 AS a2, _cachedrel_136.prob * q0_x72.prob AS prob FROM _cachedrel_136,q0_x72 WHERE _cachedrel_136.a1 = q0_x72.a1) AS t__x111),
q0_x74 AS (SELECT a1, prob FROM (SELECT _cachedrel_160.a1 AS a1, _cachedrel_160.a2 AS a2, q0_x73.a1 AS a3, _cachedrel_160.prob * q0_x73.prob AS prob FROM _cachedrel_160,q0_x73 WHERE _cachedrel_160.a2 = q0_x73.a1) AS t__x113),
q0_x88 AS (SELECT a1, prob FROM (SELECT q0_x74.a1 AS a1, q0_x74.prob / t__x115.prob AS prob FROM q0_x74,(SELECT max(prob) AS prob FROM q0_x74) AS t__x115) AS t__x116),
q0_x90 AS (SELECT a1, prob FROM (SELECT q0_x88.a1 AS a1, t__x118.a1 AS a2, q0_x88.prob * t__x118.prob AS prob FROM q0_x88,(SELECT a1, prob FROM _cachedrel_172 WHERE a1 = '1') AS t__x118) AS t__x119),
q0_x75 AS (SELECT a1, 1 AS prob FROM q0_x74),
q0_x76 AS (SELECT a1, a4 AS a2, prob FROM (SELECT q0_x75.a1 AS a1, _cachedrel_167.a1 AS a2, _cachedrel_167.a2 AS a3, _cachedrel_167.a3 AS a4, q0_x75.prob * _cachedrel_167.prob AS prob FROM q0_x75,_cachedrel_167 WHERE q0_x75.a1 = _cachedrel_167.a1) AS t__x121),
q0_x77 AS (SELECT a2 AS a1, max(prob) AS prob FROM q0_x76 GROUP BY a2),
q0_x79 AS (SELECT a1, a4 AS a2, prob FROM (SELECT q0_x77.a1 AS a1, t__x123.a1 AS a2, t__x123.a2 AS a3, t__x123.a3 AS a4, q0_x77.prob * t__x123.prob AS prob FROM q0_x77,(SELECT a3 AS a1, a2, a1 AS a3, prob FROM _cachedrel_167) AS t__x123 WHERE q0_x77.a1 = t__x123.a1) AS t__x124),
q0_x80 AS (SELECT a2 AS a1, prob FROM q0_x79),
q0_x81 AS (SELECT a1, a3 AS a2, max(prob) AS prob FROM (SELECT _cachedrel_170.a1 AS a1, _cachedrel_170.a2 AS a2, _cachedrel_170.a3 AS a3, _cachedrel_170.a4 AS a4, q0_x80.a1 AS a5, _cachedrel_170.prob * q0_x80.prob AS prob FROM _cachedrel_170,q0_x80 WHERE _cachedrel_170.a1 = q0_x80.a1) AS t__x126 GROUP BY a1, a3),
q0_x78 AS (SELECT a1, a2, max(prob) AS prob FROM q0_x76 GROUP BY a1, a2),
q0_x82 AS (SELECT a2 AS a1, a1 AS a2, prob FROM q0_x78),
q0_x83 AS (SELECT a1, a3 AS a2, prob FROM (SELECT _cachedrel_170.a1 AS a1, _cachedrel_170.a2 AS a2, _cachedrel_170.a3 AS a3, _cachedrel_170.a4 AS a4, q0_x74.a1 AS a5, _cachedrel_170.prob * q0_x74.prob AS prob FROM _cachedrel_170,q0_x74 WHERE _cachedrel_170.a1 = q0_x74.a1) AS t__x127),
q0_x84 AS (SELECT a1, a4 AS a2, sum(prob) AS prob FROM (SELECT q0_x82.a1 AS a1, q0_x82.a2 AS a2, q0_x83.a1 AS a3, q0_x83.a2 AS a4, q0_x82.prob * q0_x83.prob AS prob FROM q0_x82,q0_x83 WHERE q0_x82.a2 = q0_x83.a1) AS t__x129 GROUP BY a1, a4),
q0_x85 AS (SELECT q0_x81.a1 AS a1, q0_x81.a2 AS a2, q0_x84.a1 AS a3, q0_x84.a2 AS a4, q0_x81.prob * q0_x84.prob AS prob FROM q0_x81,q0_x84 WHERE q0_x81.a2 = q0_x84.a2),
q0_x86 AS (SELECT a1, a3 AS a2, max(prob) AS prob FROM q0_x85 GROUP BY a1, a3),
q0_x87 AS (SELECT a1, prob FROM (SELECT a1, prob FROM q0_x86) AS t__x131),
q0_x89 AS (SELECT a1, prob FROM (SELECT q0_x87.a1 AS a1, q0_x87.prob / t__x133.prob AS prob FROM q0_x87,(SELECT max(prob) AS prob FROM q0_x87) AS t__x133) AS t__x134),
q0_x91 AS (SELECT a1, prob FROM (SELECT q0_x89.a1 AS a1, t__x136.a1 AS a2, q0_x89.prob * t__x136.prob AS prob FROM q0_x89,(SELECT a1, prob FROM _cachedrel_172 WHERE a1 = '2') AS t__x136) AS t__x137),
q0_x92 AS (SELECT a1, prob FROM q0_x90 UNION ALL SELECT a1, prob FROM q0_x91),
q0_x93 AS (SELECT a1, sum(prob) AS prob FROM q0_x92 GROUP BY a1),
q0_x94 AS (SELECT a1, prob FROM (SELECT q0_x70.a1 AS a1, q0_x93.a1 AS a2, q0_x70.prob * q0_x93.prob AS prob FROM q0_x70,q0_x93 WHERE q0_x70.a1 = q0_x93.a1) AS t__x138),
q0_x97 AS (SELECT a1, prob FROM (SELECT q0_x94.a1 AS a1, q0_x94.prob / t__x140.prob AS prob FROM q0_x94,(SELECT max(prob) AS prob FROM q0_x94) AS t__x140) AS t__x141),
q0_x100 AS (SELECT a1, prob FROM (SELECT q0_x97.a1 AS a1, t__x143.a1 AS a2, q0_x97.prob * t__x143.prob AS prob FROM q0_x97,(SELECT a1, prob FROM _cachedrel_180 WHERE a1 = '2') AS t__x143) AS t__x144),
q0_x95 AS (SELECT a1, prob FROM (SELECT q0_x70.a1 AS a1, _cachedrel_178.a1 AS a2, q0_x70.prob * _cachedrel_178.prob AS prob FROM q0_x70,_cachedrel_178 WHERE q0_x70.a1 = _cachedrel_178.a1) AS t__x145),
q0_x98 AS (SELECT a1, prob FROM (SELECT q0_x95.a1 AS a1, q0_x95.prob / t__x147.prob AS prob FROM q0_x95,(SELECT max(prob) AS prob FROM q0_x95) AS t__x147) AS t__x148),
q0_x101 AS (SELECT a1, prob FROM (SELECT q0_x98.a1 AS a1, t__x150.a1 AS a2, q0_x98.prob * t__x150.prob AS prob FROM q0_x98,(SELECT a1, prob FROM _cachedrel_180 WHERE a1 = '3') AS t__x150) AS t__x151),
q0_x102 AS (SELECT a1, prob FROM q0_x99 UNION ALL (SELECT a1, prob FROM q0_x100 UNION ALL SELECT a1, prob FROM q0_x101)),
q0_x103 AS (SELECT a1, sum(prob) AS prob FROM q0_x102 GROUP BY a1),
q0_x113 AS (SELECT a1, prob FROM (SELECT q0_x111.a1 AS a1, q0_x111.prob * t__x154.prob AS prob FROM q0_x111,(SELECT prob FROM (SELECT CASE WHEN prob > CAST(0 AS DOUBLE) THEN CAST(1 AS DOUBLE) ELSE CAST(0 AS DOUBLE) END AS prob FROM q0_x112) AS t__x153 WHERE prob > 0.0) AS t__x154 UNION ALL SELECT q0_x103.a1 AS a1, q0_x103.prob * t__x157.prob AS prob FROM q0_x103,(SELECT prob FROM (SELECT CASE WHEN prob > CAST(0 AS DOUBLE) THEN CAST(0 AS DOUBLE) ELSE CAST(1 AS DOUBLE) END AS prob FROM q0_x112) AS t__x156 WHERE prob > 0.0) AS t__x157) AS t__x158),
q0_x114 AS (SELECT a1, prob FROM (SELECT q0_x113.a1 AS a1, _cachedrel_77.a1 AS a2, q0_x113.prob * _cachedrel_77.prob AS prob FROM q0_x113,_cachedrel_77 WHERE q0_x113.a1 = _cachedrel_77.a1) AS t__x160) SELECT a1, prob FROM (SELECT q0_x16.a1 AS a1, q0_x16.prob * t__x2.prob AS prob FROM q0_x16,(SELECT prob FROM (SELECT CASE WHEN prob > CAST(0 AS DOUBLE) THEN CAST(1 AS DOUBLE) ELSE CAST(0 AS DOUBLE) END AS prob FROM q0_x115) AS t__x1 WHERE prob > 0.0) AS t__x2 UNION ALL SELECT q0_x114.a1 AS a1, q0_x114.prob * t__x5.prob AS prob FROM q0_x114,(SELECT prob FROM (SELECT CASE WHEN prob > CAST(0 AS DOUBLE) THEN CAST(0 AS DOUBLE) ELSE CAST(1 AS DOUBLE) END AS prob FROM q0_x115) AS t__x4 WHERE prob > 0.0) AS t__x5) AS t__x6;

rollback;
