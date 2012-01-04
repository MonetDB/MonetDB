start transaction;

create table kv17 (
	"messagetype"           string,
	"dataownercode"         string,
	"lineplanningnumber"    string,
	"operatingday"          date,
	"journeynumber"         decimal(10),
	"reinforcementnumber"   decimal(10),
	"timestamp"             timestamp,
	"reasontype"            decimal(10),
	"subreasontype"         string,
	"reasoncontent"         string,
	"advicetype"            decimal(10),
	"subadvicetype"         string,
	"advicecontent"         string,
	"userstopcode"          string,
	"passagesequencenumber" decimal(10),
	"lagtime"               decimal(10),
	"targetarrivaltime"     time,
	"targetdeparturetime"   time,
	"journeystoptype"       string,
	"destinationcode"       string,
	"destinationname50"     string,
	"destinationname16"     string,
	"destinationdetail16"   string,
	"destinationdisplay16"  string
);


INSERT INTO kv17 (timestamp, reasoncontent, subadvicetype, journeynumber, lineplanningnumber, messagetype, reasontype, operatingday, subreasontype, dataownercode, advicetype, advicecontent, reinforcementnumber) VALUES (timestamp '2007-10-31T11:44:09.000+01:00', null, 2, 1021, 'N198', 'CANCEL', 1, date '2007-10-31', '19_1', 'ARR', 1, null, 0);
INSERT INTO kv17 (journeynumber, lineplanningnumber, passagesequencenumber, dataownercode, timestamp, userstopcode, operatingday, messagetype, reinforcementnumber) VALUES (1021, 'N198', 1, 'ARR', timestamp '2007-10-31T11:44:09.000+01:00', 57330090, date '2007-10-31', 'SHORTEN', 0);
INSERT INTO kv17 (journeynumber, lineplanningnumber, passagesequencenumber, dataownercode, timestamp, userstopcode, operatingday, messagetype, reinforcementnumber) VALUES (1021, 'N198', 1, 'ARR', timestamp '2007-10-31T11:44:09.000+01:00', 57330090, date '2007-10-31', 'SHORTEN', 0);
INSERT INTO kv17 (timestamp, journeynumber, lineplanningnumber, passagesequencenumber, userstopcode, journeystoptype, targetarrivaltime, targetdeparturetime, reinforcementnumber, dataownercode, messagetype, operatingday) VALUES (timestamp '2007-10-31T11:44:09.000+01:00', 1021, 'N198', 1, 57330090, 'INTERMEDIATE', time '19:28:00', time '19:30:00', 0, 'ARR', 'CHANGEPASSTIMES', date '2007-10-31');
INSERT INTO kv17 (destinationname50, timestamp, journeynumber, lineplanningnumber, passagesequencenumber, userstopcode, operatingday, reinforcementnumber, dataownercode, destinationcode, messagetype, destinationname16, destinationdisplay16, destinationdetail16) VALUES ('Utrecht CS Westzijde', timestamp '2007-10-31T11:44:09.000+01:00', 1021, 'N198', 1, 57330090, date '2007-10-31', 0, 'ARR', 'UtrCS02', 'CHANGEDESTINATION', 'Utrecht CS', null, null);
INSERT INTO kv17 (journeynumber, lineplanningnumber, passagesequencenumber, dataownercode, timestamp, userstopcode, lagtime, operatingday, messagetype, reinforcementnumber) VALUES (1021, 'N198', 1, 'ARR', timestamp '2007-10-31T11:44:09.000+01:00', 57330090, 300, date '2007-10-31', 'LAG', 0);
INSERT INTO kv17 (timestamp, reasoncontent, subadvicetype, journeynumber, lineplanningnumber, passagesequencenumber, reasontype, userstopcode, operatingday, subreasontype, advicetype, dataownercode, messagetype, advicecontent, reinforcementnumber) VALUES (timestamp '2007-10-31T11:44:09.000+01:00', null, 2, 1021, 'N198', 1, 1, 57330090, date '2007-10-31', '19_1', 1, 'ARR', 'MUTATIONMESSAGE', null, 0);
INSERT INTO kv17 (timestamp, reasoncontent, subadvicetype, journeynumber, lineplanningnumber, messagetype, reasontype, operatingday, subreasontype, dataownercode, advicetype, advicecontent, reinforcementnumber) VALUES (timestamp '2007-10-31T11:44:09.000+01:00', null, null, 842, 'N199', 'CANCEL', null, date '2007-11-01', null, 'ARR', null, null, 0);
INSERT INTO kv17 (journeynumber, lineplanningnumber, dataownercode, timestamp, operatingday, messagetype, reinforcementnumber) VALUES (842, 'N199', 'ARR', timestamp '2007-10-31T11:44:09.000+01:00', date '2007-11-01', 'RECOVER', 0);
INSERT INTO kv17 (timestamp, reasoncontent, subadvicetype, journeynumber, lineplanningnumber, passagesequencenumber, reasontype, userstopcode, operatingday, subreasontype, advicetype, dataownercode, messagetype, advicecontent, reinforcementnumber) VALUES (timestamp '2009-10-08T07:54:00', 'Voertuig niet toegankelijk voor rolstoelgebruikers (niet lagevloers)', null, 10, 1, 0, null, 3000, date '2009-10-08', null, null, 'CXX', 'MUTATIONMESSAGE', 'Wacht op volgende bus van 10:00', 0);
INSERT INTO kv17 (journeynumber, lineplanningnumber, dataownercode, timestamp, operatingday, messagetype, reinforcementnumber) VALUES (90, 100, 'z', timestamp '2009-10-08T08:04:00', date '2009-09-23', 'ADD', 1);
INSERT INTO kv17 (journeynumber, lineplanningnumber, passagesequencenumber, dataownercode, timestamp, userstopcode, operatingday, messagetype, reinforcementnumber) VALUES (90, 100, 0, 'z', timestamp '2009-10-08T08:04:00', 1, date '2009-09-23', 'SHORTEN', 1);
INSERT INTO kv17 (journeynumber, lineplanningnumber, passagesequencenumber, dataownercode, timestamp, userstopcode, operatingday, messagetype, reinforcementnumber) VALUES (90, 100, 0, 'z', timestamp '2009-10-08T08:04:00', 1, date '2009-09-23', 'SHORTEN', 1);
INSERT INTO kv17 (timestamp, reasoncontent, subadvicetype, journeynumber, lineplanningnumber, passagesequencenumber, reasontype, userstopcode, operatingday, subreasontype, advicetype, dataownercode, messagetype, advicecontent, reinforcementnumber) VALUES (timestamp '2009-10-08T08:04:00', 'Wacht op aansluiting lijn 99', null, 90, 100, 0, null, 1, date '2009-09-23', null, null, 'z', 'MUTATIONMESSAGE', 'Op halte twee wordt een extra bus ingezet', 1);
INSERT INTO kv17 (journeynumber, lineplanningnumber, passagesequencenumber, dataownercode, timestamp, userstopcode, lagtime, operatingday, messagetype, reinforcementnumber) VALUES (90, 100, 0, 'z', timestamp '2009-10-08T08:04:00', 1, 300, date '2009-09-23', 'LAG', 1);
INSERT INTO kv17 (timestamp, reasoncontent, subadvicetype, journeynumber, lineplanningnumber, passagesequencenumber, reasontype, userstopcode, operatingday, subreasontype, advicetype, dataownercode, messagetype, advicecontent, reinforcementnumber) VALUES (timestamp '2009-10-08T08:04:00', 'Wacht op aansluiting lijn 99', null, 90, 100, 0, null, 1, date '2009-09-23', null, null, 'z', 'MUTATIONMESSAGE', 'Op halte twee wordt een extra bus ingezet', 1);
INSERT INTO kv17 (timestamp, journeynumber, lineplanningnumber, passagesequencenumber, userstopcode, journeystoptype, targetarrivaltime, targetdeparturetime, reinforcementnumber, dataownercode, messagetype, operatingday) VALUES (timestamp '2009-10-08T08:04:00', 90, 100, 0, 1, 'INTERMEDIATE', time '08:17:00', time '08:20:00', 1, 'z', 'CHANGEPASSTIMES', date '2009-09-23');
INSERT INTO kv17 (destinationname50, timestamp, journeynumber, lineplanningnumber, passagesequencenumber, userstopcode, operatingday, reinforcementnumber, dataownercode, destinationcode, messagetype, destinationname16, destinationdisplay16, destinationdetail16) VALUES ('ROTTERDAM CS', timestamp '2009-10-08T08:04:00', 90, 100, 0, 1, date '2009-09-23', 1, 'z', null, 'CHANGEDESTINATION', 'ROTTERDAM CS', null, 'via ALEXANDER');
INSERT INTO kv17 (destinationname50, timestamp, journeynumber, lineplanningnumber, passagesequencenumber, userstopcode, operatingday, reinforcementnumber, dataownercode, destinationcode, messagetype, destinationname16, destinationdisplay16, destinationdetail16) VALUES ('ROTTERDAM CS', timestamp '2009-10-08T08:04:00', 90, 100, 0, 1, date '2009-09-23', 1, 'z', null, 'CHANGEDESTINATION', 'ROTTERDAM CS', null, 'via ALEXANDER');
INSERT INTO kv17 (journeynumber, lineplanningnumber, dataownercode, timestamp, operatingday, messagetype, reinforcementnumber) VALUES (0, '1rst', 'BISON', timestamp '2009-10-08T09:20:00', date '2009-10-08', 'ADD', 0);
INSERT INTO kv17 (timestamp, journeynumber, lineplanningnumber, passagesequencenumber, userstopcode, journeystoptype, targetarrivaltime, targetdeparturetime, reinforcementnumber, dataownercode, messagetype, operatingday) VALUES (timestamp '2009-10-08T09:20:00', 0, '1rst', 0, 90, 'FIRST', time '10:00:00', time '10:00:00', 0, 'BISON', 'CHANGEPASSTIMES', date '2009-10-08');
INSERT INTO kv17 (destinationname50, timestamp, journeynumber, lineplanningnumber, passagesequencenumber, userstopcode, operatingday, reinforcementnumber, dataownercode, destinationcode, messagetype, destinationname16, destinationdisplay16, destinationdetail16) VALUES ('Rotterdam CS', timestamp '2009-10-08T09:20:00', 0, '1rst', 0, 90, date '2009-10-08', 0, 'BISON', null, 'CHANGEDESTINATION', 'Rotterdam CS', 'R\'damViaP\'buren', 'via pieterburen');
INSERT INTO kv17 (timestamp, journeynumber, lineplanningnumber, passagesequencenumber, userstopcode, journeystoptype, targetarrivaltime, targetdeparturetime, reinforcementnumber, dataownercode, messagetype, operatingday) VALUES (timestamp '2009-10-08T09:20:00', 0, '1rst', 0, 90, 'FIRST', time '10:00:00', time '10:00:00', 0, 'BISON', 'CHANGEPASSTIMES', date '2009-10-08');
INSERT INTO kv17 (destinationname50, timestamp, journeynumber, lineplanningnumber, passagesequencenumber, userstopcode, operatingday, reinforcementnumber, dataownercode, destinationcode, messagetype, destinationname16, destinationdisplay16, destinationdetail16) VALUES ('Rotterdam CS', timestamp '2009-10-08T09:20:00', 0, '1rst', 0, 90, date '2009-10-08', 0, 'BISON', null, 'CHANGEDESTINATION', 'Rotterdam CS', 'R\'damViaP\'buren', 'via pieterburen');
INSERT INTO kv17 (timestamp, journeynumber, lineplanningnumber, passagesequencenumber, userstopcode, journeystoptype, targetarrivaltime, targetdeparturetime, reinforcementnumber, dataownercode, messagetype, operatingday) VALUES (timestamp '2009-10-08T09:20:00', 0, '1rst', 0, 90, 'FIRST', time '10:00:00', time '10:00:00', 0, 'BISON', 'CHANGEPASSTIMES', date '2009-10-08');
INSERT INTO kv17 (destinationname50, timestamp, journeynumber, lineplanningnumber, passagesequencenumber, userstopcode, operatingday, reinforcementnumber, dataownercode, destinationcode, messagetype, destinationname16, destinationdisplay16, destinationdetail16) VALUES ('Rotterdam CS', timestamp '2009-10-08T09:20:00', 0, '1rst', 0, 90, date '2009-10-08', 0, 'BISON', null, 'CHANGEDESTINATION', 'Rotterdam CS', 'R\'damViaP\'buren', 'via pieterburen');

select * from kv17;

rollback;
