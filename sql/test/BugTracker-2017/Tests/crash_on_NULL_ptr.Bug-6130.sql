/* Temporary tables to ease the loading process */
CREATE TABLE "LOADONLY_comments" (
    id BIGINT NOT NULL PRIMARY KEY,
    creationDate TIMESTAMP(3) WITH TIME ZONE NOT NULL,
    locationIP VARCHAR(40) NOT NULL,
    browserUsed VARCHAR(40) NOT NULL,
    content VARCHAR(2000) NOT NULL,
    length INT NOT NULL,
    creator BIGINT NOT NULL,
    place_id BIGINT NOT NULL,
    replyOfPost BIGINT,
    replyOfComment BIGINT
);

CREATE TABLE "LOADONLY_comment_tags" (
    comment_id BIGINT NOT NULL,
    tag_id BIGINT NOT NULL,
    PRIMARY KEY(comment_id, tag_id)
);

CREATE TABLE "LOADONLY_posts" (
    id BIGINT NOT NULL PRIMARY KEY,
    imageFile VARCHAR(40),
    creationDate TIMESTAMP(3) WITH TIME ZONE NOT NULL,
    locationIP VARCHAR(40) NOT NULL,
    browserUsed VARCHAR(40) NOT NULL,
    "language" VARCHAR(40),
    content VARCHAR(2000),
    length INT NOT NULL,
    creator BIGINT NOT NULL,
    forum_id BIGINT NOT NULL,
    place_id BIGINT NOT NULL
);

CREATE TABLE "LOADONLY_post_tags" (
    post_id BIGINT NOT NULL,
    tag_id BIGINT NOT NULL,
    PRIMARY KEY(post_id, tag_id)
);


/* Relations */
CREATE TABLE forums(
    id BIGINT NOT NULL PRIMARY KEY,
    title VARCHAR(80) NOT NULL,
    creationDate TIMESTAMP(3) WITH TIME ZONE NOT NULL,
    moderator BIGINT /*though it seems that all generated tuples have always a moderator */
);

CREATE TABLE forum_persons(
    forum_id BIGINT NOT NULL,
    person_id BIGINT NOT NULL,
    joinDate TIMESTAMP(3) WITH TIME ZONE NOT NULL,
    PRIMARY KEY(forum_id, person_id)
);

CREATE TABLE forum_tags(
    forum_id BIGINT NOT NULL,
    tag_id BIGINT NOT NULL,
    PRIMARY KEY(forum_id, tag_id)
);

CREATE TABLE friends(
    src BIGINT NOT NULL,
    dst BIGINT NOT NULL,
    creationDate TIMESTAMP(3) WITH TIME ZONE NOT NULL,
    PRIMARY KEY(src, dst)
);

CREATE TABLE organisations(
    id BIGINT NOT NULL PRIMARY KEY,
    "type" VARCHAR(40) NOT NULL, /* university or company */
    name VARCHAR(160) NOT NULL,
    url VARCHAR(2000) NOT NULL,
    place_id BIGINT NOT NULL
);

CREATE TABLE messages(
    id BIGINT NOT NULL PRIMARY KEY,
    content VARCHAR(2000) NOT NULL,
    isImage BOOLEAN NOT NULL DEFAULT FALSE,
    creationDate TIMESTAMP(3) WITH TIME ZONE NOT NULL,
    locationIP VARCHAR(40) NOT NULL,
    browserUsed VARCHAR(40) NOT NULL,
    "language" VARCHAR(40),
    length INT NOT NULL,
    creator BIGINT NOT NULL,
    forum_id BIGINT,
    place_id BIGINT NOT NULL,
    reply BIGINT, -- Null if this is a post, otherwise the message to which this comment is immediately appended.
    post_id BIGINT NOT NULL -- If this is a comment, the post to which this comment is ultimately appended. If it is a post, it is the same value as id
);

CREATE TABLE message_tags(
    message_id BIGINT NOT NULL,
    tag_id BIGINT NOT NULL,
    PRIMARY KEY(message_id, tag_id)
);

CREATE TABLE persons(
    id BIGINT NOT NULL PRIMARY KEY,
    firstName VARCHAR(40) NOT NULL ,
    lastName VARCHAR(40) NOT NULL,
    gender VARCHAR(40) NOT NULL,
    birthDay DATE NOT NULL,
    creationDate TIMESTAMP(3) WITH TIME ZONE NOT NULL,
    locationIP VARCHAR(40) NOT NULL,
    browserUsed VARCHAR(40) NOT NULL,
    place_id BIGINT NOT NULL
);

CREATE TABLE person_comments(
    person_id BIGINT NOT NULL,
    comment_id BIGINT NOT NULL,
    creationDate TIMESTAMP(3) WITH TIME ZONE NOT NULL,
    PRIMARY KEY(person_id, comment_id)
);

CREATE TABLE person_emails(
    id BIGINT NOT NULL,
    email VARCHAR(120) NOT NULL,
    PRIMARY KEY(id, email)
);

CREATE TABLE person_languages(
    id BIGINT NOT NULL,
    "language" VARCHAR(40) NOT NULL,
    PRIMARY KEY(id, "language")
);

CREATE TABLE person_tags(
    person_id BIGINT NOT NULL,
    tag_id BIGINT NOT NULL,
    PRIMARY KEY(person_id, tag_id)
);

CREATE TABLE person_posts(
    person_id BIGINT NOT NULL,
    post_id BIGINT NOT NULL,
    creationDate TIMESTAMP(3) WITH TIME ZONE NOT NULL,
    PRIMARY KEY(person_id, post_id)
);

CREATE TABLE person_studyAt_organisations(
    person_id BIGINT NOT NULL,
    organisation_id BIGINT NOT NULL,
    classYear INT NOT NULL,
    PRIMARY KEY(person_id, organisation_id)
);

CREATE TABLE person_workAt_organisations(
    person_id BIGINT NOT NULL,
    organisation_id BIGINT NOT NULL,
    workFrom INT NOT NULL,
    PRIMARY KEY(person_id, organisation_id)
);

CREATE TABLE places(
    id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    url VARCHAR(2000) NOT NULL, /* suspecting this is a left-over for RDF systems where this field replaces the ID */
    "type" VARCHAR(40) NOT NULL, 
    isPartOf BIGINT
);

CREATE TABLE tags(
    id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR(160) NOT NULL, 
    url VARCHAR(2000) NOT NULL /* is this field mandatory ? */
);

CREATE TABLE tag_tagclasses(
    tag_id BIGINT NOT NULL,
    tagclass_id BIGINT NOT NULL,
    PRIMARY KEY(tag_id, tagclass_id)
);

CREATE TABLE tagclasses(
    id BIGINT NOT NULL PRIMARY KEY,
    name VARCHAR(40) NOT NULL,
    url VARCHAR(2000) NOT NULL /* is this field mandatory ? */
);

CREATE TABLE tagclass_inheritance(
    subclass_id BIGINT NOT NULL,
    superclass_id BIGINT NOT NULL,
    PRIMARY KEY(subclass_id, superclass_id)
);

/* Foreign keys */
ALTER TABLE forums ADD FOREIGN KEY(moderator) REFERENCES persons(id);
ALTER TABLE forum_persons ADD FOREIGN KEY(forum_id) REFERENCES forums(id);
ALTER TABLE forum_persons ADD FOREIGN KEY(person_id) REFERENCES persons(id);
ALTER TABLE forum_tags ADD FOREIGN KEY(forum_id) REFERENCES forums(id);
ALTER TABLE forum_tags ADD FOREIGN KEY(tag_id) REFERENCES tags(id);
ALTER TABLE friends ADD FOREIGN KEY(src) REFERENCES persons(id);
ALTER TABLE friends ADD FOREIGN KEY(dst) REFERENCES persons(id);
ALTER TABLE messages ADD FOREIGN KEY(creator) REFERENCES persons(id);
ALTER TABLE messages ADD FOREIGN KEY(forum_id) REFERENCES forums(id);
ALTER TABLE messages ADD FOREIGN KEY(place_id) REFERENCES places(id);
ALTER TABLE messages ADD FOREIGN KEY(reply) REFERENCES messages(id);
ALTER TABLE messages ADD FOREIGN KEY(post_id) REFERENCES messages(id);
ALTER TABLE message_tags ADD FOREIGN KEY(message_id) REFERENCES messages(id);
ALTER TABLE message_tags ADD FOREIGN KEY(tag_id) REFERENCES tags(id);
ALTER TABLE organisations ADD FOREIGN KEY(place_id) REFERENCES places(id);
ALTER TABLE persons ADD FOREIGN KEY(place_id) REFERENCES places(id);
ALTER TABLE person_comments ADD FOREIGN KEY(person_id) REFERENCES persons(id);
ALTER TABLE person_comments ADD FOREIGN KEY(comment_id) REFERENCES messages(id);
ALTER TABLE person_emails ADD FOREIGN KEY(id) REFERENCES persons(id);
ALTER TABLE person_languages ADD FOREIGN KEY(id) REFERENCES persons(id);
ALTER TABLE person_posts ADD FOREIGN KEY(person_id) REFERENCES persons(id);
ALTER TABLE person_posts ADD FOREIGN KEY(post_id) REFERENCES messages(id);
ALTER TABLE person_studyAt_organisations ADD FOREIGN KEY(person_id) REFERENCES persons(id);
ALTER TABLE person_studyAt_organisations ADD FOREIGN KEY(organisation_id) REFERENCES organisations(id);
ALTER TABLE person_tags ADD FOREIGN KEY(person_id) REFERENCES persons(id);
ALTER TABLE person_tags ADD FOREIGN KEY(tag_id) REFERENCES tags(id);
ALTER TABLE person_workAt_organisations ADD FOREIGN KEY(person_id) REFERENCES persons(id);
ALTER TABLE person_workAt_organisations ADD FOREIGN KEY(organisation_id) REFERENCES organisations(id);
ALTER TABLE places ADD FOREIGN KEY(isPartOf) REFERENCES places(id);
ALTER TABLE tag_tagclasses ADD FOREIGN KEY(tag_id) REFERENCES tags(id);
ALTER TABLE tag_tagclasses ADD FOREIGN KEY(tagclass_id) REFERENCES tagclasses(id);
ALTER TABLE tagclass_inheritance ADD FOREIGN KEY(subclass_id) REFERENCES tagclasses(id);
ALTER TABLE tagclass_inheritance ADD FOREIGN KEY(superclass_id) REFERENCES tagclasses(id);

WITH
params AS (
  /* subtract 1 to endDate to have a closed interval for the BETWEEN operator */
  SELECT id, startDate, startDate + CAST (duration -1 AS INTERVAL DAY) AS endDate FROM (
    SELECT
      4398046512167 AS id,
      CAST('2011-11-05' AS TIMESTAMP(3)) AS startDate,
      365 AS duration
  ) AS tmp ),
friends1 AS (
  SELECT dst AS id FROM friends WHERE src = (SELECT id FROM params)
),
friends_tags AS (
  SELECT t.id, t.name, p.creationDate
  FROM messages p, message_tags pt, tags t
  WHERE p.creator IN (SELECT * FROM friends1)
    AND pt.message_id = p.id
    AND pt.tag_id = t.id
),
resultset AS (
  SELECT name, COUNT(name) as count
  FROM friends_tags ft
  WHERE ft.creationDate BETWEEN (SELECT startDate FROM params) AND (SELECT endDate FROM params)
  /* Crash */
  AND NOT EXISTS ( SELECT 1 FROM friends_tags ftcor WHERE ftcor.id = ft.id AND ftcor.creationDate < (SELECT startDate FROM params))
  GROUP BY name
)
SELECT * FROM resultset
ORDER BY count DESC, name
LIMIT 10;

/* Drop used tables if they already exist */
/* Temporary tables (only to ease the loading process) */
DROP TABLE "LOADONLY_comments";
DROP TABLE "LOADONLY_comment_tags";
DROP TABLE "LOADONLY_posts";
DROP TABLE "LOADONLY_post_tags";
/* Relations to model one to many or many to many relationships */
DROP TABLE forum_persons;
DROP TABLE forum_tags;
DROP TABLE friends;
DROP TABLE message_tags;
DROP TABLE person_comments;
DROP TABLE person_emails;
DROP TABLE person_tags;
DROP TABLE person_languages;
DROP TABLE person_posts;
DROP TABLE person_studyAt_organisations;
DROP TABLE person_workAt_organisations;
DROP TABLE tag_tagclasses;
DROP TABLE tagclass_inheritance;
/* be consistent with the foreign key dependendencies */
DROP TABLE organisations;
DROP TABlE messages;
DROP TABLE forums;
DROP TABLE persons;
DROP TABLE places;
DROP TABLE tags;
DROP TABLE tagclasses;

