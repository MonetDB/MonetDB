-- This example comes from A. Nicu from Sybase
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

SELECT DISTINCT Device.ID,Device.Name
FROM Device
WHERE Device.ID IN
   (SELECT DISTINCT Device.ID
    FROM Device
    WHERE Device.ID IN
       (SELECT DISTINCT Device.ID
        FROM Device
        WHERE Device.ID IN
        (SELECT DISTINCT Device.ID
         FROM Device
         WHERE Device.ID IN
            (SELECT DISTINCT Device.ID
             FROM Device
             WHERE Device.ID IN
                (SELECT DISTINCT Device.ID
                 FROM Device
                 WHERE Device.ID IN
                    (SELECT DISTINCT Device.ID
                     FROM Device
                     WHERE Device.ID IN (SELECT Device.ID
                        	 FROM Device,Entry
                        	 WHERE Device.dbId = Entry.dd
                        	 AND UPPER(Entry.ss) LIKE '%192.16.%')
                         OR Device.ID IN  (  ( SELECT ID  FROM Device
          WHERE  ( UPPER(Device.sName) LIKE '%SSMC%' )  )  ) ) 
OR Device.ID IN (  ( SELECT ID FROM Device WHERE  ( UPPER(Device.sName) LIKE '%SSTB%' )  )  ) )
 OR Device.ID IN (  ( SELECT dcrDeviceId FROM Device WHERE  ( UPPER(Device.sName) LIKE '%XHFH%' )  )  ) ) 
OR  Device.ID IN (  ( SELECT ID FROM Device WHERE  ( UPPER(Device.Name) LIKE '%SSTB%' )  )  ) ) 
OR Device.ID IN (  ( SELECT ID FROM Device WHERE  ( UPPER(Device.Name) LIKE '%XHFH%' )  )  ) ) 
OR Device.ID IN (  ( SELECT Device.ID FROM Device,Entry WHERE  (  ( Device.dbId = Entry.dd )  )  AND  ( UPPER(Entry.ss) LIKE '%129.196.9.%' )  )  ) );

drop table device;
drop table entry;
rollback;
