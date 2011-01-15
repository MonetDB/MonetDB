CREATE TABLE _metadata (
artist VARCHAR(255),
album VARCHAR(255),
track VARCHAR(255),
tracknum INT,
duration INT
);

CREATE VIEW metadata AS
SELECT artist, album, track, tracknum, duration,
((CASE WHEN artist IS NULL OR
char_length(trim(artist))=0 THEN 0 ELSE 16 END) +
(CASE WHEN album IS NULL OR
char_length(trim(album))=0 THEN 0 ELSE 8 END) +
(CASE WHEN track IS NULL OR
char_length(trim(track))=0 THEN 0 ELSE 4 END) +
(CASE WHEN tracknum IS NULL OR tracknum=0 THEN 0
ELSE 2 END) +
(CASE WHEN duration IS NULL OR duration=0 THEN 0
ELSE 1 END)) AS weightindex
FROM _metadata;

SELECT * FROM metadata;

drop view metadata;
drop table _metadata;
