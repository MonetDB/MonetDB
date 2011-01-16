CREATE TABLE sys.maps (
        id      int NOT NULL,
        parent  int,
        world   int NOT NULL,
        imagefile       varchar(255)    NOT NULL,
        top_left_x      double,
        top_left_y      double,
        bot_right_x     double,
        bot_right_y     double,

        PRIMARY KEY (id) -- c1012117
-- c1012388
-- c1012118
);
ALTER TABLE sys.maps add FOREIGN KEY (parent) REFERENCES sys.maps(id);

insert into maps values (1, NULL, 1, 'world-map-1600-1700.jpg', 0, 0, 0, 0);
-- insert into maps values (1, 0, 1, 'world-map-1600-1700.jpg', 0, 0, 0, 0);
insert into maps values (2, 10, 1, 'world-map-1600-1700.jpg', 0, 0, 0, 0);

select * from maps;
