
CREATE TABLE messages (
	"id"           BIGINT        NOT NULL,
	"content"      VARCHAR(2000) NOT NULL,
	"isimage"      BOOLEAN       NOT NULL DEFAULT false,
	"creationdate" TIMESTAMP(3) WITH TIME ZONE NOT NULL,
	"locationip"   VARCHAR(40)   NOT NULL,
	"browserused"  VARCHAR(40)   NOT NULL,
	"language"     VARCHAR(40),
	"length"       INTEGER       NOT NULL,
	"creator"      BIGINT        NOT NULL,
	"forum_id"     BIGINT        NOT NULL,
	"place_id"     BIGINT        NOT NULL,
	"reply"        BIGINT,
	"post_id"      BIGINT        NOT NULL
);

PREPARE
INSERT INTO messages(id, content, isimage, creationdate, locationip, browserused, "language", length, creator, place_id, forum_id, reply, post_id)
SELECT tnew.id, tnew.content, false, tnew.creationdate, tnew.locationip, tnew.browserused, NULL, tnew.length, tnew.person_id, tnew.place_id, parent.forum_id, tnew.comment_id, parent.post_id
FROM ( VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ) AS tnew(id, content, creationdate, locationip, browserused, length, person_id, place_id, parent_id)
JOIN messages parent ON ( parent.id = tnew.parent_id );

DROP TABLE messages;

