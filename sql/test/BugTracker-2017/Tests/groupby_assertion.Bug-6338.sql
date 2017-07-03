create table sources (
    src_id        string primary key,
    src_owner     string ,
    src_name      string not null,          
    src_alias     string not null,         
    src_count     integer default -1,     
    src_query     string,                
    src_created   timestamp with time zone default now
);
    
create table source_attr ( 
    sat_id           string ,
    sat_index        integer not null,
    sat_name         string not null,
    sat_alias        string not null,   
    sat_nulls        integer ,
    sat_distincts    integer,
    sat_type         string, 
    sat_mass         string 
);

CREATE TABLE source_ac_0(i integer, j integer, k integer);
INSERT INTO source_ac_0 VALUES(0,1,6),(2,3,7),(4,5,8);
SELECT * FROM source_ac_0;

INSERT INTO sources VALUES ('src_0', 'ac_0', 'source_ac_0', 'source_ac_0', (select count(*) from source_ac_0), '',now());
        
insert into source_attr select  'src_0',
          (select count(*) from source_attr where sat_id = 'source_ac_0'),
          c.name,c.name,
          (select count(*) from source_ac_0  where  c.name is null),
          (select count(distinct c.name) from source_ac_0),
          c.type,null 
               from sys.tables as T, sys.columns as C 
               where T.id = C.table_id and T.name = 'source_ac_0';
