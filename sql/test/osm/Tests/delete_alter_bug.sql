CREATE TABLE way_tags (way integer, k varchar(255), v varchar(1024));
COPY 25 RECORDS INTO way_tags from STDIN USING DELIMITERS ',', '\n', '''';
23950357,'created_by','Potlatch 0.10f'
23950357,'highway','tertiary'
23950357,'oneway','yes'
23950375,'name','Haarholzer Straße'
23950375,'name','Haarholzer Straße'
23950375,'created_by','Potlatch 0.10f'
23950375,'created_by','Potlatch 0.10f'
23950375,'maxspeed','30'
23950375,'highway','residential'
23950375,'maxspeed','30'
23950375,'highway','residential'
23950375,'lanes','1'
23950375,'lanes','1'
24644006,'name','Kapelsesteenweg'
24644006,'highway','residential'
24644162,'created_by','Potlatch 0.9c'
24644162,'name','Kampelaer'
24644162,'name','Kampelaer'
24644162,'created_by','Potlatch 0.9c'
24644162,'highway','residential'
24644162,'highway','residential'
24644169,'highway','residential'
24644169,'name','Jakobstraße'
24644170,'created_by','Potlatch 0.9c'
24644170,'name','Werrastraße'

select distinct way from way_tags group by way, k having count(*) > 1;
select * from way_tags where way in (23950375, 24644162);
delete from way_tags where way in (23950375, 24644162);
ALTER table way_tags add primary key(way, k);

DROP table way_tags;
