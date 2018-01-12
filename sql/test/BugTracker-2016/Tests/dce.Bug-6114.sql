CREATE SCHEMA foo;
SET SCHEMA foo;

/* Relations */
CREATE TABLE comments(
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

CREATE TABLE comment_tags(
    comment_id BIGINT NOT NULL,
    tag_id BIGINT NOT NULL,
    PRIMARY KEY(comment_id, tag_id)
);

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

CREATE TABLE posts(
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

CREATE TABLE post_tags(
    post_id BIGINT NOT NULL,
    tag_id BIGINT NOT NULL,
    PRIMARY KEY(post_id, tag_id)
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
ALTER TABLE comments ADD FOREIGN KEY(creator) REFERENCES persons(id);
ALTER TABLE comments ADD FOREIGN KEY(place_id) REFERENCES places(id);
ALTER TABLE comments ADD FOREIGN KEY(replyOfPost) REFERENCES posts(id);
ALTER TABLE comments ADD FOREIGN KEY(replyOfComment) REFERENCES comments(id);
ALTER TABLE comment_tags ADD FOREIGN KEY(comment_id) REFERENCES comments(id);
ALTER TABLE comment_tags ADD FOREIGN KEY(tag_id) REFERENCES tags(id);
ALTER TABLE forums ADD FOREIGN KEY(moderator) REFERENCES persons(id);
ALTER TABLE forum_persons ADD FOREIGN KEY(forum_id) REFERENCES forums(id);
ALTER TABLE forum_persons ADD FOREIGN KEY(person_id) REFERENCES persons(id);
ALTER TABLE forum_tags ADD FOREIGN KEY(forum_id) REFERENCES forums(id);
ALTER TABLE forum_tags ADD FOREIGN KEY(tag_id) REFERENCES tags(id);
ALTER TABLE friends ADD FOREIGN KEY(src) REFERENCES persons(id);
ALTER TABLE friends ADD FOREIGN KEY(dst) REFERENCES persons(id);
ALTER TABLE organisations ADD FOREIGN KEY(place_id) REFERENCES places(id);
ALTER TABLE persons ADD FOREIGN KEY(place_id) REFERENCES places(id);
ALTER TABLE person_comments ADD FOREIGN KEY(person_id) REFERENCES persons(id);
ALTER TABLE person_comments ADD FOREIGN KEY(comment_id) REFERENCES comments(id);
ALTER TABLE person_emails ADD FOREIGN KEY(id) REFERENCES persons(id);
ALTER TABLE person_languages ADD FOREIGN KEY(id) REFERENCES persons(id);
ALTER TABLE person_posts ADD FOREIGN KEY(person_id) REFERENCES persons(id);
ALTER TABLE person_posts ADD FOREIGN KEY(post_id) REFERENCES posts(id);
ALTER TABLE person_studyAt_organisations ADD FOREIGN KEY(person_id) REFERENCES persons(id);
ALTER TABLE person_studyAt_organisations ADD FOREIGN KEY(organisation_id) REFERENCES organisations(id);
ALTER TABLE person_tags ADD FOREIGN KEY(person_id) REFERENCES persons(id);
ALTER TABLE person_tags ADD FOREIGN KEY(tag_id) REFERENCES tags(id);
ALTER TABLE person_workAt_organisations ADD FOREIGN KEY(person_id) REFERENCES persons(id);
ALTER TABLE person_workAt_organisations ADD FOREIGN KEY(organisation_id) REFERENCES organisations(id);
ALTER TABLE places ADD FOREIGN KEY(isPartOf) REFERENCES places(id);
ALTER TABLE posts ADD FOREIGN KEY(creator) REFERENCES persons(id);
ALTER TABLE posts ADD FOREIGN KEY(forum_id) REFERENCES forums(id);
ALTER TABLE posts ADD FOREIGN KEY(place_id) REFERENCES places(id);
ALTER TABLE post_tags ADD FOREIGN KEY(post_id) REFERENCES posts(id);
ALTER TABLE post_tags ADD FOREIGN KEY(tag_id) REFERENCES tags(id);
ALTER TABLE tag_tagclasses ADD FOREIGN KEY(tag_id) REFERENCES tags(id);
ALTER TABLE tag_tagclasses ADD FOREIGN KEY(tagclass_id) REFERENCES tagclasses(id);
ALTER TABLE tagclass_inheritance ADD FOREIGN KEY(subclass_id) REFERENCES tagclasses(id);
ALTER TABLE tagclass_inheritance ADD FOREIGN KEY(superclass_id) REFERENCES tagclasses(id);


WITH
    params AS ( /* input parameters */
        SELECT 
           2199023260527 AS id,
           CAST('Lin' AS VARCHAR(40)) AS name
    ),
    friends_1 AS (
        SELECT 1 as "distance", p1."id", p1.firstName, p1.lastName, p1.birthDay, p1.creationDate, p1."gender", p1.browserUsed, p1.locationIP
        FROM persons p1, friends f, params p
        WHERE p.id = f."src" AND f."dst" = p1."id"
    ),
    friends_2 AS (
        SELECT * FROM friends_1
      UNION
        SELECT 2 as "distance", p2."id", p2.firstName, p2.lastName, p2.birthDay, p2.creationDate, p2."gender", p2.browserUsed, p2.locationIP
        FROM persons p2, friends f
        WHERE f."src" IN ( SELECT "id" FROM friends_1 ) AND f."dst" = p2."id" AND p2.id NOT IN ( SELECT "id" FROM friends_1 )
    ),
    friends_3 AS (
        SELECT * FROM friends_2 f WHERE f.firstName = (SELECT name FROM params)
      UNION
        SELECT 3 as "distance", p3."id", p3.firstName, p3.lastName, p3.birthDay, p3.creationDate, p3."gender", p3.browserUsed, p3.locationIP
        FROM persons p3, friends f
        WHERE f."src" IN ( SELECT "id" FROM friends_2 ) AND f."dst" = p3."id" AND p3.id NOT IN ( SELECT "id" FROM friends_2 ) AND p3.firstName = (SELECT name FROM params) 
    ),
    filter AS (
            SELECT * FROM friends_3 f WHERE f.firstName = (SELECT name FROM params)
    )
SELECT * FROM filter ORDER BY "distance";

DROP TABLE comment_tags;
DROP TABLE forum_persons;
DROP TABLE forum_tags;
DROP TABLE friends;
DROP TABLE person_emails;
DROP TABLE person_tags;
DROP TABLE person_languages;
DROP TABLE person_comments;
DROP TABLE person_posts;
DROP TABLE person_studyAt_organisations;
DROP TABLE person_workAt_organisations;
DROP TABLE post_tags;
DROP TABLE tag_tagclasses;
DROP TABLE tagclass_inheritance;

DROP TABLE organisations;
DROP TABLE comments;
DROP TABLE posts;
DROP TABLE forums;
DROP TABLE persons;
DROP TABLE places;
DROP TABLE tags;
DROP TABLE tagclasses;

SET SCHEMA sys;
DROP SCHEMA foo;
