
CREATE TABLE "sys"."ways" (
        "id" int NOT NULL,
        "username" varchar(255),
        "timestamp" timestamptz(7),
        CONSTRAINT "ways_id_pkey" PRIMARY KEY ("id")
);
CREATE TABLE way_tags (way integer, k varchar(255), v varchar(1024),
primary key (way, k), foreign key(way) references ways);
alter table ways drop constraint ways_id_pkey;
SELECT count(*) FROM ways
LEFT JOIN way_tags ON
        ways.id = way_tags.way
WHERE
        k = 'highway' AND
        v = 'secondary';

drop table way_tags;
drop table ways;
