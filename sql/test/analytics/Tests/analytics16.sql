start transaction;
create table analytics (aa int, bb int);
insert into analytics values (15, 3), (3, 1), (2, 1), (5, 3), (NULL, 2), (3, 2), (4, 1), (6, 3), (8, 2), (NULL, 4);

select covar_samp(aa, aa),
       covar_samp(bb, bb),
       covar_samp(aa, bb),
       covar_samp(bb, aa) from analytics;

select covar_pop(aa, aa),
       covar_pop(bb, bb),
       covar_pop(aa, bb),
       covar_pop(bb, aa) from analytics;

select corr(aa, aa),
       corr(bb, bb),
       corr(aa, bb),
       corr(bb, aa) from analytics;


select covar_samp(aa, aa),
       covar_samp(bb, bb),
       covar_samp(aa, bb),
       covar_samp(bb, aa) from analytics group by aa;

select covar_pop(aa, aa),
       covar_pop(bb, bb),
       covar_pop(aa, bb),
       covar_pop(bb, aa) from analytics group by aa;

select corr(aa, aa),
       corr(bb, bb),
       corr(aa, bb),
       corr(bb, aa) from analytics group by aa;


select covar_samp(aa, aa),
       covar_samp(bb, bb),
       covar_samp(aa, bb),
       covar_samp(bb, aa) from analytics group by bb;

select covar_pop(aa, aa),
       covar_pop(bb, bb),
       covar_pop(aa, bb),
       covar_pop(bb, aa) from analytics group by bb;

select corr(aa, aa),
       corr(bb, bb),
       corr(aa, bb),
       corr(bb, aa) from analytics group by bb;


select covar_samp(aa, aa),
       covar_samp(bb, bb),
       covar_samp(aa, bb),
       covar_samp(bb, aa) from analytics group by aa, bb;

select covar_pop(aa, aa),
       covar_pop(bb, bb),
       covar_pop(aa, bb),
       covar_pop(bb, aa) from analytics group by aa, bb;

select corr(aa, aa),
       corr(bb, bb),
       corr(aa, bb),
       corr(bb, aa) from analytics group by aa, bb;


select covar_samp(aa, 1),
       covar_pop(aa, 1),
       corr(aa, 1) from analytics group by aa;
select covar_samp(aa, 1),
       covar_pop(aa, 1),
       corr(aa, 1) from analytics group by bb;
select covar_samp(aa, 1),
       covar_pop(bb, 1),
       corr(bb, 1) from analytics group by aa, bb;

select covar_samp(1, aa),
       covar_pop(1, aa),
       corr(1, aa) from analytics group by aa;
select covar_samp(1, aa),
       covar_pop(1, aa),
       corr(1, aa) from analytics group by bb;
select covar_samp(1, aa),
       covar_pop(1, aa),
       corr(1, aa) from analytics group by aa, bb;

select covar_samp(1, 1),
       covar_pop(1, 1),
       corr(1, 1) from analytics group by aa;
select covar_samp(1, 1),
       covar_pop(1, 1),
       corr(1, 1) from analytics group by bb;
select covar_samp(1, 1),
       covar_pop(1, 1),
       corr(1, 1) from analytics group by aa, bb;


select covar_samp(NULL, aa),
       covar_pop(NULL, aa),
       corr(NULL, aa) from analytics group by aa;
select covar_samp(NULL, aa),
       covar_pop(NULL, aa),
       corr(NULL, aa) from analytics group by bb;
select covar_samp(NULL, aa),
       covar_pop(NULL, aa),
       corr(NULL, aa) from analytics group by aa, bb;

select covar_samp(aa, NULL),
       covar_pop(aa, NULL),
       corr(aa, NULL) from analytics group by aa;
select covar_samp(aa, NULL),
       covar_pop(aa, NULL),
       corr(aa, NULL) from analytics group by bb;
select covar_samp(aa, NULL),
       covar_pop(aa, NULL),
       corr(aa, NULL) from analytics group by aa, bb;

select covar_samp(NULL, NULL),
       covar_pop(NULL, NULL),
       corr(NULL, NULL) from analytics group by aa;
select covar_samp(NULL, NULL),
       covar_pop(NULL, NULL),
       corr(NULL, NULL) from analytics group by bb;
select covar_samp(NULL, NULL),
       covar_pop(NULL, NULL),
       corr(NULL, NULL) from analytics group by aa, bb;


select (select corr(a1.aa, a2.aa) + corr(a2.aa, a1.aa) from analytics a2) from analytics a1;
select (select corr(a1.aa + a2.aa, a1.aa + a2.aa) from analytics a2) from analytics a1;
select corr(a1.aa, a1.bb) from analytics a1 where a1.bb > (select corr(a1.aa, a2.aa) + corr(a2.aa, a1.aa) from analytics a2);
select corr(a1.aa, a1.bb) from analytics a1 where a1.bb > (select corr(a1.aa + a2.aa, a1.aa + a2.aa) from analytics a2);
select corr(a1.aa, a1.bb) from analytics a1 where a1.bb > (select corr(a1.aa, a2.aa) + corr(a2.aa, a1.aa) from analytics a2) group by bb;
select corr(a1.aa, a1.bb) from analytics a1 where a1.bb > (select corr(a1.aa + a2.aa, a1.aa + a2.aa) from analytics a2) group by bb;
select corr(a1.aa, a1.bb) from analytics a1 group by bb having a1.bb > (select corr(MAX(a1.aa) + a2.aa, MIN(a1.aa) + a2.aa) from analytics a2);

rollback;

CREATE TABLE t0(c0 DOUBLE, c1 INT);
INSERT INTO t0(c0,c1) VALUES(1E200, 1), (0, 1);

SELECT VAR_POP(c0) FROM t0; --error, overflow
SELECT STDDEV_POP(c0) FROM t0; --error, overflow
SELECT COVAR_POP(c0,c0) FROM t0; --error, overflow
SELECT CORR(c0,c0) FROM t0; --error, overflow

SELECT VAR_POP(c0) FROM t0 GROUP BY c0;
SELECT STDDEV_POP(c0) FROM t0 GROUP BY c0;
SELECT CORR(c0,c0) FROM t0 GROUP BY c0;

SELECT VAR_POP(c0) FROM t0 GROUP BY c1;--error, overflow
SELECT STDDEV_POP(c0) FROM t0 GROUP BY c1; --error, overflow
SELECT CORR(c0,c0) FROM t0 GROUP BY c1; --error, overflow

SELECT VAR_SAMP(c0) OVER () FROM t0; --error, overflow
SELECT STDDEV_SAMP(c0) OVER () FROM t0; --error, overflow
SELECT COVAR_SAMP(c0,c0) OVER () FROM t0; --error, overflow
SELECT CORR(c0,c0) OVER () FROM t0; --error, overflow

DROP TABLE T0;
