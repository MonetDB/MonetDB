create table ranges(
    x_min decimal(8,5), 
    y_min decimal(7,5),
    width  integer,
    nb integer
);

create table trips (
        tripid bigint not null, 
        x decimal(8,5) not null, 
        y decimal(8,5) not null, 
        time bigint not null
);

UPDATE ranges SET nb = (SELECT count(*) FROM  trips T
                   WHERE T.x between x_min and x_min + width
              AND T.y between y_min and y_min + width);

drop table trips;
drop table ranges;
