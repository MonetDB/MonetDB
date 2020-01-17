start transaction;
create table analytics (aa int, bb int);
insert into analytics values (15, 3), (3, 1), (2, 1), (5, 3), (NULL, 2), (3, 2), (4, 1), (6, 3), (8, 2), (NULL, 4);

select covar_samp(aa, aa) from analytics;
select covar_samp(bb, bb) from analytics;
select covar_samp(aa, bb) from analytics;
select covar_samp(bb, aa) from analytics;

select covar_pop(aa, aa) from analytics;
select covar_pop(bb, bb) from analytics;
select covar_pop(aa, bb) from analytics;
select covar_pop(bb, aa) from analytics;

select corr(aa, aa) from analytics;
select corr(bb, bb) from analytics;
select corr(aa, bb) from analytics;
select corr(bb, aa) from analytics;


select covar_samp(aa, aa) from analytics group by aa;
select covar_samp(bb, bb) from analytics group by aa;
select covar_samp(aa, bb) from analytics group by aa;
select covar_samp(bb, aa) from analytics group by aa;

select covar_pop(aa, aa) from analytics group by aa;
select covar_pop(bb, bb) from analytics group by aa;
select covar_pop(aa, bb) from analytics group by aa;
select covar_pop(bb, aa) from analytics group by aa;

select corr(aa, aa) from analytics group by aa;
select corr(bb, bb) from analytics group by aa;
select corr(aa, bb) from analytics group by aa;
select corr(bb, aa) from analytics group by aa;


select covar_samp(aa, aa) from analytics group by bb;
select covar_samp(bb, bb) from analytics group by bb;
select covar_samp(aa, bb) from analytics group by bb;
select covar_samp(bb, aa) from analytics group by bb;

select covar_pop(aa, aa) from analytics group by bb;
select covar_pop(bb, bb) from analytics group by bb;
select covar_pop(aa, bb) from analytics group by bb;
select covar_pop(bb, aa) from analytics group by bb;

select corr(aa, aa) from analytics group by bb;
select corr(bb, bb) from analytics group by bb;
select corr(aa, bb) from analytics group by bb;
select corr(bb, aa) from analytics group by bb;


select covar_samp(aa, aa) from analytics group by aa, bb;
select covar_samp(bb, bb) from analytics group by aa, bb;
select covar_samp(aa, bb) from analytics group by aa, bb;
select covar_samp(bb, aa) from analytics group by aa, bb;

select covar_pop(aa, aa) from analytics group by aa, bb;
select covar_pop(bb, bb) from analytics group by aa, bb;
select covar_pop(aa, bb) from analytics group by aa, bb;
select covar_pop(bb, aa) from analytics group by aa, bb;

select corr(aa, aa) from analytics group by aa, bb;
select corr(bb, bb) from analytics group by aa, bb;
select corr(aa, bb) from analytics group by aa, bb;
select corr(bb, aa) from analytics group by aa, bb;


select covar_samp(aa, 1) from analytics group by aa;
select covar_pop(aa, 1) from analytics group by aa;
select corr(aa, 1) from analytics group by aa;
select covar_samp(aa, 1) from analytics group by bb;
select covar_pop(aa, 1) from analytics group by bb;
select corr(aa, 1) from analytics group by bb;
select covar_samp(aa, 1) from analytics group by aa, bb;
select covar_pop(bb, 1) from analytics group by aa, bb;
select corr(bb, 1) from analytics group by aa, bb;

select covar_samp(1, aa) from analytics group by aa;
select covar_pop(1, aa) from analytics group by aa;
select corr(1, aa) from analytics group by aa;
select covar_samp(1, aa) from analytics group by bb;
select covar_pop(1, aa) from analytics group by bb;
select corr(1, aa) from analytics group by bb;
select covar_samp(1, aa) from analytics group by aa, bb;
select covar_pop(1, aa) from analytics group by aa, bb;
select corr(1, aa) from analytics group by aa, bb;

select covar_samp(1, 1) from analytics group by aa;
select covar_pop(1, 1) from analytics group by aa;
select corr(1, 1) from analytics group by aa;
select covar_samp(1, 1) from analytics group by bb;
select covar_pop(1, 1) from analytics group by bb;
select corr(1, 1) from analytics group by bb;
select covar_samp(1, 1) from analytics group by aa, bb;
select covar_pop(1, 1) from analytics group by aa, bb;
select corr(1, 1) from analytics group by aa, bb;


select covar_samp(NULL, aa) from analytics group by aa;
select covar_pop(NULL, aa) from analytics group by aa;
select corr(NULL, aa) from analytics group by aa;
select covar_samp(NULL, aa) from analytics group by bb;
select covar_pop(NULL, aa) from analytics group by bb;
select corr(NULL, aa) from analytics group by bb;
select covar_samp(NULL, aa) from analytics group by aa, bb;
select covar_pop(NULL, aa) from analytics group by aa, bb;
select corr(NULL, aa) from analytics group by aa, bb;

select covar_samp(aa, NULL) from analytics group by aa;
select covar_pop(aa, NULL) from analytics group by aa;
select corr(aa, NULL) from analytics group by aa;
select covar_samp(aa, NULL) from analytics group by bb;
select covar_pop(aa, NULL) from analytics group by bb;
select corr(aa, NULL) from analytics group by bb;
select covar_samp(aa, NULL) from analytics group by aa, bb;
select covar_pop(aa, NULL) from analytics group by aa, bb;
select corr(aa, NULL) from analytics group by aa, bb;

select covar_samp(NULL, NULL) from analytics group by aa;
select covar_pop(NULL, NULL) from analytics group by aa;
select corr(NULL, NULL) from analytics group by aa;
select covar_samp(NULL, NULL) from analytics group by bb;
select covar_pop(NULL, NULL) from analytics group by bb;
select corr(NULL, NULL) from analytics group by bb;
select covar_samp(NULL, NULL) from analytics group by aa, bb;
select covar_pop(NULL, NULL) from analytics group by aa, bb;
select corr(NULL, NULL) from analytics group by aa, bb;

rollback;
