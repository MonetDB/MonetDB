-- The Jack The Ripper case is used in different shredding formats
-- as the basis for functional testing the SQL/XML features or MonetDB
-- check against standard!

create table archive1( doc xml ); 
create table archive2( doc xml document ); 
create table archive3( doc xml sequence ); 

copy into archive1 from 'JackTheRipper.xml';

insert into archive2 values(XMLparse( document 'JackTheRipper.xml'));
insert into archive values(XMLvalidate( document 'JackTheRipper.xml'));

create table chapters( scene xml); -- to contain a tuple per scene (18)
copy into archive from 'JackTheRipper.xml' delimiter 'scene';

