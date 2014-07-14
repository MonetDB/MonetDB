CREATE TABLE services (
sid tinyint NOT NULL default '0',
sname varchar(255) NOT NULL default '',
sfee float(6,2) NOT NULL default '0.00',
PRIMARY KEY (sid)
);
INSERT INTO services (sid, sname, sfee) VALUES (1, 'Accounting', '1500.00'); 
