SET auto_commit = true;

CREATE TABLE branches (
  bid int NOT NULL default '0',
  cid tinyint NOT NULL default '0',
  bdesc varchar(255) NOT NULL default '',
  bloc char(3) NOT NULL default ''
  );
INSERT INTO branches (bid, cid, bdesc, bloc) VALUES (1011, 101, 'Corporate HQ', 'CA'); 
INSERT INTO branches (bid, cid, bdesc, bloc) VALUES (1012, 101, 'Accounting Department', 'NY'); 
INSERT INTO branches (bid, cid, bdesc, bloc) VALUES (1013, 101, 'Customer Grievances Department', 'KA'); 
INSERT INTO branches (bid, cid, bdesc, bloc) VALUES (1041, 104, 'Branch Office (East)', 'MA'); 
INSERT INTO branches (bid, cid, bdesc, bloc) VALUES (1042, 104, 'Branch Office (West)', 'CA'); 
INSERT INTO branches (bid, cid, bdesc, bloc) VALUES (1101, 110, 'Head Office', 'CA'); 
INSERT INTO branches (bid, cid, bdesc, bloc) VALUES (1031, 103, 'N Region HO', 'ME'); 
INSERT INTO branches (bid, cid, bdesc, bloc) VALUES (1032, 103, 'NE Region HO', 'CT'); 
INSERT INTO branches (bid, cid, bdesc, bloc) VALUES (1033, 103, 'NW Region HO', 'NY');
CREATE TABLE branches_services (
  bid int NOT NULL default '0',
  sid tinyint NOT NULL default '0'
  );
INSERT INTO branches_services (bid, sid) VALUES (1011, 1); 
INSERT INTO branches_services (bid, sid) VALUES (1011, 2); 
INSERT INTO branches_services (bid, sid) VALUES (1011, 3); 
INSERT INTO branches_services (bid, sid) VALUES (1011, 4); 
INSERT INTO branches_services (bid, sid) VALUES (1012, 1); 
INSERT INTO branches_services (bid, sid) VALUES (1013, 5); 
INSERT INTO branches_services (bid, sid) VALUES (1041, 1); 
INSERT INTO branches_services (bid, sid) VALUES (1041, 4); 
INSERT INTO branches_services (bid, sid) VALUES (1042, 1); 
INSERT INTO branches_services (bid, sid) VALUES (1042, 4); 
INSERT INTO branches_services (bid, sid) VALUES (1101, 1); 
INSERT INTO branches_services (bid, sid) VALUES (1031, 2); 
INSERT INTO branches_services (bid, sid) VALUES (1031, 3); 
INSERT INTO branches_services (bid, sid) VALUES (1031, 4); 
INSERT INTO branches_services (bid, sid) VALUES (1032, 3); 
INSERT INTO branches_services (bid, sid) VALUES (1033, 4);
CREATE TABLE clients (

  cid tinyint NOT NULL default '0',

  cname varchar(255) NOT NULL default '',

  PRIMARY KEY (cid)

  );
INSERT INTO clients (cid, cname) VALUES (101, 'JV Real Estate'); 
INSERT INTO clients (cid, cname) VALUES (102, 'ABC Talent Agency'); 
INSERT INTO clients (cid, cname) VALUES (103, 'DMW Trading'); 
INSERT INTO clients (cid, cname) VALUES (104, 'Rabbit Foods Inc'); 
INSERT INTO clients (cid, cname) VALUES (110, 'Sharp Eyes Detective Agency');
CREATE TABLE services (

  sid tinyint NOT NULL default '0',

  sname varchar(255) NOT NULL default '',

  sfee float(6,2) NOT NULL default '0.00',

  PRIMARY KEY (sid)

  );
INSERT INTO services (sid, sname, sfee) VALUES (1, 'Accounting', 1500.00); 
INSERT INTO services (sid, sname, sfee) VALUES (2, 'Recruitment', 500.00); 
INSERT INTO services (sid, sname, sfee) VALUES (3, 'Data Management', 300.00); 
INSERT INTO services (sid, sname, sfee) VALUES (4, 'Administration', 500.00); 
INSERT INTO services (sid, sname, sfee) VALUES (5, 'Customer Support', 2500.00); 
INSERT INTO services (sid, sname, sfee) VALUES (6, 'Security', 600.00); 

SELECT * FROM clients;
SELECT cname, bdesc, bloc FROM clients, branches WHERE 
  clients.cid = branches.cid AND branches.bloc = 'CA';
SELECT cid FROM clients WHERE cname = 'Rabbit Foods Inc';
SELECT bdesc FROM branches, clients WHERE clients.cid = 
  branches.cid AND clients.cname = 'Rabbit Foods Inc';
SELECT bdesc FROM branches WHERE cid = (SELECT cid FROM clients
  WHERE cname = 'Rabbit Foods Inc');


SELECT bdesc FROM branches WHERE cid = (SELECT cid, cname FROM
  clients WHERE cname = 'Rabbit Foods Inc');
--   ERROR 1239: Cardinality error (more/less than 1 columns) 
SELECT sname FROM services WHERE sid = (SELECT sid FROM
  branches_services WHERE bid = (SELECT bid FROM branches WHERE cid = (SELECT 
  cid FROM clients WHERE cname = 'Sharp Eyes Detective Agency')));
SELECT cid, COUNT(bid) FROM branches GROUP BY cid;
SELECT cid, COUNT(bid) FROM branches GROUP BY cid HAVING COUNT(bid) = 2;
SELECT cname FROM clients WHERE cid = 104;
SELECT cname FROM clients WHERE cid = (SELECT cid FROM branches
  GROUP BY cid HAVING COUNT(bid) = 2);
SELECT cname, bdesc FROM clients, branches, branches_services,
  services WHERE services.sid = branches_services.sid AND clients.cid = branches.cid 
  AND branches.bid = branches_services.bid AND sfee = (SELECT
  MAX(sfee) FROM services); 
SELECT bid FROM branches_services GROUP BY bid HAVING COUNT(sid) >
  (SELECT COUNT(*) FROM services)/2;
SELECT c.cid, c.cname, b.bid, b.bdesc FROM clients AS c, branches AS b,
 branches_services AS bs WHERE c.cid = b.cid AND b.bid = bs.bid GROUP BY bs.bid
  HAVING COUNT(bs.sid) > (SELECT COUNT(*) FROM services)/2;
SELECT branches.bid, COUNT(sid) FROM branches, branches_services
  WHERE branches.bid = branches_services.bid GROUP BY branches.bid HAVING
  COUNT(sid) = (SELECT COUNT(*) FROM services);
SELECT clients.cname FROM clients, branches, branches_services 
  WHERE
  branches.bid = branches_services.bid AND branches.cid = clients.cid GROUP BY 
  clients.cid HAVING COUNT(sid) = (SELECT COUNT(*) FROM services);
SELECT cname FROM clients LEFT JOIN branches ON clients.cid =
  branches.cid WHERE branches.bid IS NULL;
SELECT cname FROM clients WHERE cid = (SELECT clients.cid FROM
  branches RIGHT JOIN clients ON clients.cid = branches.cid WHERE branches.bid 
  IS NULL);
SELECT bid FROM branches_services WHERE sid = (SELECT sid FROM
  services WHERE sname = 'Recruitment');
SELECT bs.bid FROM branches_services AS bs, services AS s WHERE
  s.sid = bs.sid AND s.sname = 'Recruitment';
SELECT bid, bdesc, bloc FROM branches WHERE bloc = (SELECT bloc 
  FROM
  branches WHERE bid = 1101 AND cid = 110);
SELECT table1.bid, table1.bdesc, table1.bloc FROM branches AS
  table1, branches AS table2 WHERE table2.bid = 1101 AND table2.cid = 110 AND 
  table1.bloc = table2.bloc ;
