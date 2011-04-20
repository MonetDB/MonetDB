CREATE TABLE htmtest (
	       htmid    bigint       NOT NULL,
	       ra       double ,
	       decl     double ,
	       dra      double ,
	       ddecl    double ,
	       flux     double ,
	       dflux    double ,
	       freq     double ,
	       bw       double ,
	       type     decimal(1,0),
	       imageurl varchar(100),
	       comment  varchar(100),
	       CONSTRAINT htmtest_htmid_pkey PRIMARY KEY (htmid)
);

CREATE INDEX htmid ON htmtest (htmid);

INSERT INTO HTMTEST (HTMID,RA,DECL,FLUX,COMMENT) VALUES (1, 1.2, 2.4, 3.2, 'vlabla');


UPDATE HTMTEST set COMMENT='some update' WHERE HTMID=1;

drop table HTMTEST;
