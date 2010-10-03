-- example provide by A. Nicu from Sybase
start transaction;
CREATE TABLE device(
 ID integer,
  dbId integer,
 dcrDeviceId integer,
 Name varchar(255),
 sName varchar(255)
);
insert into device values(1,2,3,'SSTB0','XHFH0');
insert into device values(2,1,3,'XHFH0','SSTB0');

CREATE TABLE Entry(
  dd integer,
  ss varchar(255)
);
insert into Entry values(1,'129.196.9.001');

SELECT DISTINCT W0.ID,W0.Name
FROM Device W0
WHERE ( W0.ID IS NOT NULL and
( W0.ID IN
                        (SELECT W9.ID
                         FROM Device W9,Entry
                         WHERE W9.dbId = Entry.dd
                         AND UPPER(Entry.ss) LIKE '%192.16.%') )
OR ( UPPER(W0.sName) LIKE '%SSMC%' )
OR ( UPPER(W0.sName) LIKE '%SSTB%' )
OR ( UPPER(W0.sName) LIKE '%XHFH%' )
OR (UPPER(W0.Name) LIKE '%SSTB%' )
OR (UPPER(W0.Name) LIKE '%XHFH%'  )
OR W0.ID IN (  ( SELECT W12.ID FROM Device W12,Entry WHERE  W12.dbId = Entry.dd AND  UPPER(Entry.ss) LIKE '%129.196.9.%' );

DROP TABLE device;
DROP TABLE entry;

rollback;
