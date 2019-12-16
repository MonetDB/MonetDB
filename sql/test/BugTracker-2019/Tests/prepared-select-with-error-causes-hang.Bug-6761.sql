drop table if exists abacus;

create table abacus ( "'Zeitachse'" date,"'Abo_ID'" int,"'description'" varchar(256),"'Klassierungs-Typ'" clob,"'KlassierungApplikation'" clob,"'EP Netto'" decimal,"'Nettoumsatz'" decimal,"'validfrom'" date,"'validuntil'" date,"'Abo_aufgeschaltet'" int,"'Abo_deaktiviert'" int,"'Differenz'" decimal,"'User_ID'" int,"'UserName'" varchar(256),"'client'" varchar(256),"'Installations_ID'" int,"'InstallationsName'" varchar(256),"'Installationsprovider_ID'" int,"'InstallationsproviderName'" varchar(256),"'INR'" bigint,"'NAME'" varchar(256),"'PLZ'" varchar(256),"'ORT'" varchar(256),"'STAAT'" varchar(256),"'Reseller_ID'" int,"'ResellerName'" varchar(256),"'ET_ABO'" clob,"'UserName_1'" varchar(256),"'Anzahl_Abos'" decimal,"'Anzahl_User'" decimal,"'Jahr'" decimal,"'Monat'" decimal,"'Jahr_Monat'" clob,"'IFJ'" clob,"'RECNUM$'" int,"'InlineCalc_Year_Zeitachse'" int);

insert into abacus values ('2019-10-30',2239,'description','Klassierungs-Typ','Klassierung-Applikation',73.28,68.29,'2018-01-01','2018-12-01',563,63,56.3,852,'UserName','client',134,'InstallationsName',892,'InstallationsproviderName',9348,'NAME','PLZ','ORT','STAAT',934,'ResellerName','ET_ABO','UserName_1',849.2739,1742.718,395.824,39.824,'Jahr_Monat','IFJ',395824,3789);

SELECT "'ResellerName'" FROM abacus WHERE  ( ( ("'InstallationsproviderName'"='Bienz Pius Treuhand- und Revisions AG')) AND  ( ("'validuntil'"='2018-01-01' AND "'description'"='ABEA 2' AND (EXTRACT(YEAR FROM "'Zeitachse'")*100 + EXTRACT(MONTH FROM "'Zeitachse'"))/100.0='2019.010' AND "'UserName'"='AL - Astrid Lincke (Delphys)' AND "'validfrom'"='2016-12-01')) AND  ( ("'IFJ'"='ohne IFJ')) AND  ( ("'InlineCalc_Year_Zeitachse'"='2019'))) GROUP BY "'ResellerName'" LIMIT 1001 OFFSET 0;

PREPARE SELECT "'ResellerName'" FROM abacus WHERE  ( ( ("'InstallationsproviderName'"='Bienz Pius Treuhand- und Revisions AG')) AND  ( ("'validuntil'"='2018-01-01' AND "'description'"='ABEA 2' AND (EXTRACT(YEAR FROM "'Zeitachse'")*100 + EXTRACT(MONTH FROM "'Zeitachse'"))/100.0='2019.010' AND "'UserName'"='AL - Astrid Lincke (Delphys)' AND "'validfrom'"='2016-12-01')) AND  ( ("'IFJ'"='ohne IFJ')) AND  ( ("'InlineCalc_Year_Zeitachse'"='2019'))) GROUP BY "'ResellerName'" LIMIT 1001 OFFSET 0;

exec **(); -- hang in Apr2019-SP1, error in Nov2019

drop table if exists abacus;

