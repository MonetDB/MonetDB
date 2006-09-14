START TRANSACTION;
CREATE TABLE Region(
        regionid bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
        id bigint NOT NULL,
        type varchar(16) NOT NULL,
        comment varchar(1024) NOT NULL,
        ismask tinyint NOT NULL DEFAULT (0),
        area float NOT NULL DEFAULT (0),
        regionString text NOT NULL DEFAULT (''),
        sql text NOT NULL DEFAULT (''),
        xml text NOT NULL DEFAULT (''),
 CONSTRAINT pk_Region_regionId PRIMARY KEY
(
        regionid
)
);
SELECT * FROM Region;
DROP TABLE Region;
