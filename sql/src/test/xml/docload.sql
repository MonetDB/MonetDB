-- The Jack The Ripper case is used in different shredding formats
-- as the basis for functional testing the SQL/XML features or MonetDB

create table archive( doc xml ); -- to contain 1 tuple with complete document

copy into archive from 'JackTheRipper.xml';

insert into archive values(XMLvalidate( document 'JackTheRipper.xml'));

create table chapters( scene xml); -- to contain a tuple per scene (18)
copy into archive from 'JackTheRipper.xml' delimiter 'scene';

