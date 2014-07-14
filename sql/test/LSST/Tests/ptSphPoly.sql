create table stars (ra dbl, dec dbl);
insert into stars values( 0.2, 0.2) (0.4,0.4);

create table poly ( x dbl, y dbl);
insert into poly values (0.0,0.0) (1.0,0.0), (0.0,1.0);

select qserv_ptInSphEllipse(ra,dec, poly) from stars;
