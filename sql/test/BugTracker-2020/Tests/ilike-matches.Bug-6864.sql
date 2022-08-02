start transaction;

create table tab1 (col1 string);
insert into tab1 values ('Guatemala'), ('Guatemala-Mobile'), 
('Guatemala-Mobile Comcel'), ('Guatemala-Mobile Movistar'), ('Guatemala-Mobile PCS'), 
('Guatemala-Telgua'), ('Guatemala-Telefonica');

SELECT DISTINCT col1 FROM tab1 WHERE col1 ILIKE '%guate%';
    -- Guatemala
    -- Guatemala-Mobile
    -- Guatemala-Mobile Comcel
    -- Guatemala-Mobile Movistar
    -- Guatemala-Mobile PCS
    -- Guatemala-Telgua
    -- Guatemala-Telefonica

SELECT DISTINCT col1 FROM tab1 WHERE col1 ILIKE '%guate%com%';
    -- Guatemala-Mobile Comcel

SELECT DISTINCT col1 FROM tab1 WHERE col1 ILIKE '%guate%mo%com%';
    -- Guatemala-Mobile Comcel

rollback;
