CREATE TABLE cabact
(
  cabactcsu integer NOT NULL,
  cabact___rforefide character varying(32) NOT NULL,
  cabact___rteprcide character varying(32) NOT NULL,
  cabact___rtestdide character varying(32) NOT NULL,
  cabact___rfovsnide character varying(32) NOT NULL,
  cabactlil character varying(100) NOT NULL,
  cabactlic character varying(32) NOT NULL, 
  cabacttxt character varying(1500),
  cabact___rfontride character varying(32),
  cabact_f1rfodstide character varying(32),
  cabact_f2rfodstide character varying(32),
  cabact_f3rfodstide character varying(32),
  cabact_f4rfodstide character varying(32),
  cabact_f5rfodstide character varying(32),
  cabact_f6rfodstide character varying(32),
  cabact_f7rfodstide character varying(32),
  cabact_f8rfodstide character varying(32),
  cabact_f9rfodstide character varying(32),
  cabact_c1rfodstide character varying(32),
  cabact_c2rfodstide character varying(32),
  cabact_c3rfodstide character varying(32),
  cabact_c4rfodstide character varying(32),
  cabact_c5rfodstide character varying(32),
  cabact_c6rfodstide character varying(32),
  cabact_c7rfodstide character varying(32),
  cabact_c8rfodstide character varying(32),
  cabact_c9rfodstide character varying(32),
  cabactqte double,
  cabact___rfountide character varying(32),
  cabactdtd timestamp with time zone NOT NULL,
  cabactdtf timestamp with time zone NOT NULL,
  cabactax1 integer NOT NULL DEFAULT 1,
  cabactax2 integer NOT NULL DEFAULT 2,
  cabactax3 integer NOT NULL DEFAULT 3,
  cabactax4 integer NOT NULL DEFAULT 4,
  cabactax5 integer NOT NULL DEFAULT 5,
  cabactax6 integer NOT NULL DEFAULT 6,
  cabactax7 integer NOT NULL DEFAULT 7,
  cabactax8 integer NOT NULL DEFAULT 8,
  cabactax9 integer NOT NULL DEFAULT 9
);

CREATE TABLE rfoade
(
rfoade___rforefide character varying(50) NOT NULL,
rfoade___rfovdeide character varying(50) NOT NULL,
rfoade_i_rfodstide character varying(50) NOT NULL,
rfoadeaxe integer DEFAULT 0 NOT NULL,
rfoadervs integer NOT NULL,
rfoadenpm integer DEFAULT 1,
rfoade_s_rfodstide character varying(32) NOT NULL,
rfoadegch character varying(120) DEFAULT 'AAAAA' NOT NULL,
rfoadedrt character varying(120) DEFAULT 'ZZZZZ' NOT NULL,
rfoadeniv integer DEFAULT 0 NOT NULL,
rfoadetxt character varying(1800),
rfoadenum integer DEFAULT 99999 NOT NULL,
rfoadeden integer DEFAULT 999 NOT NULL,
rfoadechm character varying(5500) DEFAULT 'INVALID' NOT NULL,
rfoadeord integer DEFAULT 999999 NOT NULL
);

select * from cabact where cabact___rforefide = 'FHSJ' and 
              cabact___rteprcide = 'CPTANA' and
              cabact___rtestdide = '100' and
              cabact___rfovsnide = '200805_001' and
(cabact_f1rfodstide IS NULL or cabact_f1rfodstide IN (select
rfoade_i_rfodstide from rfoade where rfoade___rfovdeide='SECA' and
rfoade___rforefide = 'FHSJ' and rfoadervs='1')) and
              (cabact_f2rfodstide IS NULL or cabact_f2rfodstide IN
(select rfoade_i_rfodstide from rfoade where rfoade___rfovdeide='SECA'
and rfoade___rforefide = 'FHSJ' and rfoadervs='1')) and
              (cabact_f3rfodstide IS NULL or cabact_f3rfodstide IN
(select rfoade_i_rfodstide from rfoade where rfoade___rfovdeide='SECA'
and rfoade___rforefide = 'FHSJ' and rfoadervs='1')) and
              (cabact_f4rfodstide IS NULL or cabact_f4rfodstide IN
(select rfoade_i_rfodstide from rfoade where rfoade___rfovdeide='SECA'
and rfoade___rforefide = 'FHSJ' and rfoadervs='1')) and
              (cabact_f5rfodstide IS NULL or cabact_f5rfodstide IN
(select rfoade_i_rfodstide from rfoade where rfoade___rfovdeide='SECA'
and rfoade___rforefide = 'FHSJ' and rfoadervs='1')) and
              (cabact_f6rfodstide IS NULL or cabact_f6rfodstide IN
(select rfoade_i_rfodstide from rfoade where rfoade___rfovdeide='SECA'
and rfoade___rforefide = 'FHSJ' and rfoadervs='1')) and
              (cabact_f7rfodstide IS NULL or cabact_f7rfodstide IN
(select rfoade_i_rfodstide from rfoade where rfoade___rfovdeide='SECA'
and rfoade___rforefide = 'FHSJ' and rfoadervs='1')) and
              (cabact_f8rfodstide IS NULL or cabact_f8rfodstide IN
(select rfoade_i_rfodstide from rfoade where rfoade___rfovdeide='SECA'
and rfoade___rforefide = 'FHSJ' and rfoadervs='1')) and
              (cabact_f9rfodstide IS NULL or cabact_f9rfodstide IN
(select rfoade_i_rfodstide from rfoade where rfoade___rfovdeide='SECA'
and rfoade___rforefide = 'FHSJ' and rfoadervs='1')) and
              (cabact_c1rfodstide IS NULL or cabact_c1rfodstide IN
(select rfoade_i_rfodstide from rfoade where rfoade___rfovdeide='SECA'
and rfoade___rforefide = 'FHSJ' and rfoadervs='1')) and
              (cabact_c2rfodstide IS NULL or cabact_c2rfodstide IN
(select rfoade_i_rfodstide from rfoade where rfoade___rfovdeide='SECA'
and rfoade___rforefide = 'FHSJ' and rfoadervs='1')) and
              (cabact_c3rfodstide IS NULL or cabact_c3rfodstide IN
(select rfoade_i_rfodstide from rfoade where rfoade___rfovdeide='SECA'
and rfoade___rforefide = 'FHSJ' and rfoadervs='1')) and
              (cabact_c4rfodstide IS NULL or cabact_c4rfodstide IN
(select rfoade_i_rfodstide from rfoade where rfoade___rfovdeide='SECA'
and rfoade___rforefide = 'FHSJ' and rfoadervs='1')) and
              (cabact_c5rfodstide IS NULL or cabact_c5rfodstide IN
(select rfoade_i_rfodstide from rfoade where rfoade___rfovdeide='SECA'
and rfoade___rforefide = 'FHSJ' and rfoadervs='1')) and
              (cabact_c6rfodstide IS NULL or cabact_c6rfodstide IN
(select rfoade_i_rfodstide from rfoade where rfoade___rfovdeide='SECA'
and rfoade___rforefide = 'FHSJ' and rfoadervs='1')) and
              (cabact_c7rfodstide IS NULL or cabact_c7rfodstide IN
(select rfoade_i_rfodstide from rfoade where rfoade___rfovdeide='SECA'
and rfoade___rforefide = 'FHSJ' and rfoadervs='1')) and
              (cabact_c8rfodstide IS NULL or cabact_c8rfodstide IN
(select rfoade_i_rfodstide from rfoade where rfoade___rfovdeide='SECA'
and rfoade___rforefide = 'FHSJ' and rfoadervs='1'));

drop table cabact;
drop table rfoade;
