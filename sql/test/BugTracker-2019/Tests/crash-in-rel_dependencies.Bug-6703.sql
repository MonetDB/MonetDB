
START TRANSACTION;

CREATE TABLE resultbuffer_int_str(q int, i1 int ,s1 string, prob double);

CREATE TABLE "paramsint" (
	"paramname" CHARACTER LARGE OBJECT,
	"value"     BIGINT,
	"prob"      DOUBLE
);
CREATE TABLE "cachedrel_6" (
	"a1"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "cachedrel_33" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "cachedrel_44" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   BIGINT,
	"prob" DOUBLE
);
CREATE TABLE "cachedrel_84" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   INTEGER,
	"prob" DOUBLE
);
CREATE TABLE "cachedrel_91" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"a3"   CHARACTER LARGE OBJECT,
	"a4"   CHARACTER LARGE OBJECT,
	"prob" DOUBLE
);
create view cachedrel_104 as select a3 as a1, a2, a1 as a3, prob from cachedrel_84;
create view cachedrel_421 as select a3 as a1, a2, a1 as a3, prob from cachedrel_33;
create view cachedrel_606 as select a1, prob from (values ('1',1.0)) as t__x0(a1,prob);
CREATE TABLE "cachedrel_646" (
	"a1"   INTEGER,
	"a2"   INTEGER,
	"prob" DOUBLE
);
create view cachedrel_647 as select a2 as a1, a1 as a2, prob from cachedrel_646;


create view s0_mix_source_string_result_result as with q0_x0 as (select a1, prob from (select cachedrel_44.a1 as a1, cachedrel_44.a2 as a2, cachedrel_44.a3 as a3, t__x2.a1 as a4, cachedrel_44.prob * t__x2.prob as prob from cachedrel_44,(select a2 as a1, prob from (select paramname as a1, value as a2, prob from paramsint where paramname = 's0_userid') as t__x1) as t__x2 where cachedrel_44.a3 = t__x2.a1) as t__x3),q0_x1 as (select a1, prob from (select cachedrel_6.a1 as a1, q0_x0.a1 as a2, cachedrel_6.prob * q0_x0.prob as prob from cachedrel_6,q0_x0 where cachedrel_6.a1 = q0_x0.a1) as t__x5),q0_x2 as (select a1, prob from (select cachedrel_647.a1 as a1, cachedrel_647.a2 as a2, q0_x1.a1 as a3, cachedrel_647.prob * q0_x1.prob as prob from cachedrel_647,q0_x1 where cachedrel_647.a2 = q0_x1.a1) as t__x7),q0_x3 as (select a1, 1 as prob from q0_x2),q0_x4 as (select a1, a4 as a2, prob from (select q0_x3.a1 as a1, cachedrel_33.a1 as a2, cachedrel_33.a2 as a3, cachedrel_33.a3 as a4, q0_x3.prob * cachedrel_33.prob as prob from q0_x3,cachedrel_33 where q0_x3.a1 = cachedrel_33.a1) as t__x9),q0_x5 as (select a2 as a1, max(prob) as prob from q0_x4 group by a2),q0_x8 as (select a1, prob from (select q0_x5.a1 as a1, q0_x5.prob / t__x11.prob as prob from q0_x5,(select max(prob) as prob from q0_x5) as t__x11) as t__x12),q0_x9 as (select a1, a4 as a2, prob from (select q0_x8.a1 as a1, cachedrel_84.a1 as a2, cachedrel_84.a2 as a3, cachedrel_84.a3 as a4, q0_x8.prob * cachedrel_84.prob as prob from q0_x8,cachedrel_84 where q0_x8.a1 = cachedrel_84.a1) as t__x14),q0_x10 as (select a2 as a1, max(prob) as prob from q0_x9 group by a2),q0_x12 as (select a1, a4 as a2, prob from (select q0_x10.a1 as a1, cachedrel_84.a1 as a2, cachedrel_84.a2 as a3, cachedrel_84.a3 as a4, q0_x10.prob * cachedrel_84.prob as prob from q0_x10,cachedrel_84 where q0_x10.a1 = cachedrel_84.a1) as t__x16),q0_x13 as (select a2 as a1, max(prob) as prob from q0_x12 group by a2),q0_x15 as (select a1, a4 as a2, prob from (select q0_x13.a1 as a1, cachedrel_84.a1 as a2, cachedrel_84.a2 as a3, cachedrel_84.a3 as a4, q0_x13.prob * cachedrel_84.prob as prob from q0_x13,cachedrel_84 where q0_x13.a1 = cachedrel_84.a1) as t__x18),q0_x16 as (select a2 as a1, max(prob) as prob from q0_x15 group by a2),q0_x18 as (select a1, a4 as a2, prob from (select q0_x16.a1 as a1, cachedrel_104.a1 as a2, cachedrel_104.a2 as a3, cachedrel_104.a3 as a4, q0_x16.prob * cachedrel_104.prob as prob from q0_x16,cachedrel_104 where q0_x16.a1 = cachedrel_104.a1) as t__x20),q0_x19 as (select a2 as a1, max(prob) as prob from q0_x18 group by a2),q0_x20 as (select a1, a4 as a2, prob from (select q0_x19.a1 as a1, cachedrel_104.a1 as a2, cachedrel_104.a2 as a3, cachedrel_104.a3 as a4, q0_x19.prob * cachedrel_104.prob as prob from q0_x19,cachedrel_104 where q0_x19.a1 = cachedrel_104.a1) as t__x22),q0_x21 as (select a2 as a1, max(prob) as prob from q0_x20 group by a2),q0_x22 as (select a1, a4 as a2, prob from (select q0_x21.a1 as a1, cachedrel_104.a1 as a2, cachedrel_104.a2 as a3, cachedrel_104.a3 as a4, q0_x21.prob * cachedrel_104.prob as prob from q0_x21,cachedrel_104 where q0_x21.a1 = cachedrel_104.a1) as t__x24),q0_x23 as (select a2 as a1, max(prob) as prob from q0_x22 group by a2),q0_x24 as (select a1, a4 as a2, prob from (select q0_x23.a1 as a1, cachedrel_421.a1 as a2, cachedrel_421.a2 as a3, cachedrel_421.a3 as a4, q0_x23.prob * cachedrel_421.prob as prob from q0_x23,cachedrel_421 where q0_x23.a1 = cachedrel_421.a1) as t__x26),q0_x25 as (select a2 as a1, prob from q0_x24),q0_x26 as (select a1, a3 as a2, max(prob) as prob from (select cachedrel_91.a1 as a1, cachedrel_91.a2 as a2, cachedrel_91.a3 as a3, cachedrel_91.a4 as a4, q0_x25.a1 as a5, cachedrel_91.prob * q0_x25.prob as prob from cachedrel_91,q0_x25 where cachedrel_91.a1 = q0_x25.a1) as t__x28 group by a1, a3),q0_x33 as (select a1, a2, prob from (select q0_x26.a1 as a1, q0_x26.a2 as a2, cachedrel_606.a1 as a3, q0_x26.prob * cachedrel_606.prob as prob from q0_x26,cachedrel_606) as t__x30),q0_x6 as (select a1, a2, max(prob) as prob from q0_x4 group by a1, a2),q0_x7 as (select a1, a2, prob from (select q0_x6.a1 as a1, q0_x6.a2 as a2, t__x32.a1 as a3, q0_x6.prob / t__x32.prob as prob from q0_x6,(select a1, max(prob) as prob from q0_x6 group by a1) as t__x32 where q0_x6.a1 = t__x32.a1) as t__x33),q0_x11 as (select a1, a2, max(prob) as prob from q0_x9 group by a1, a2),q0_x27 as (select a1, a4 as a2, prob from (select q0_x7.a1 as a1, q0_x7.a2 as a2, q0_x11.a1 as a3, q0_x11.a2 as a4, q0_x7.prob * q0_x11.prob as prob from q0_x7,q0_x11 where q0_x7.a2 = q0_x11.a1) as t__x35),q0_x14 as (select a1, a2, max(prob) as prob from q0_x12 group by a1, a2),q0_x28 as (select a1, a4 as a2, prob from (select q0_x27.a1 as a1, q0_x27.a2 as a2, q0_x14.a1 as a3, q0_x14.a2 as a4, q0_x27.prob * q0_x14.prob as prob from q0_x27,q0_x14 where q0_x27.a2 = q0_x14.a1) as t__x37),q0_x17 as (select a1, a2, max(prob) as prob from q0_x15 group by a1, a2),q0_x29 as (select a1, a4 as a2, prob from (select q0_x28.a1 as a1, q0_x28.a2 as a2, q0_x17.a1 as a3, q0_x17.a2 as a4, q0_x28.prob * q0_x17.prob as prob from q0_x28,q0_x17 where q0_x28.a2 = q0_x17.a1) as t__x39),q0_x30 as (select a2 as a1, a1 as a2, prob from q0_x29),q0_x31 as (select a1, a3 as a2, cast(count(prob) as double) as prob from (select cachedrel_91.a1 as a1, cachedrel_91.a2 as a2, cachedrel_91.a3 as a3, cachedrel_91.a4 as a4, q0_x2.a1 as a5, cachedrel_91.prob * q0_x2.prob as prob from cachedrel_91,q0_x2 where cachedrel_91.a1 = q0_x2.a1) as t__x40 group by a1, a3),q0_x32 as (select a1, a4 as a2, sum(prob) as prob from (select q0_x30.a1 as a1, q0_x30.a2 as a2, q0_x31.a1 as a3, q0_x31.a2 as a4, q0_x30.prob * q0_x31.prob as prob from q0_x30,q0_x31 where q0_x30.a2 = q0_x31.a1) as t__x42 group by a1, a4),q0_x34 as (select a1, a2, prob from (select q0_x32.a1 as a1, q0_x32.a2 as a2, t__x44.a1 as a3, q0_x32.prob * t__x44.prob as prob from q0_x32,(values ('2',1.0)) as t__x44(a1,prob)) as t__x45),q0_x35 as (select a1, a2, prob from q0_x33 union all select a1, a2, prob from q0_x34) select a1, a2, prob from q0_x35;

-- Just the SELECT does not fail.
-- select cast(0 as int) as q,a1, a2, prob from s0_mix_source_string_result_result limit 1;

-- The INSERT INTO fails
insert into resultbuffer_int_str (q, i1,s1, prob) select cast(0 as int) as q,a1, a2, prob from s0_mix_source_string_result_result as r;

ROLLBACK;

