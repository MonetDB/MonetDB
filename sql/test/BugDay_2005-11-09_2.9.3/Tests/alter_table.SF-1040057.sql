CREATE TABLE "ggle_urls"
(
"id" integer,
"url" varchar(255)
);

ALTER TABLE "ggle_urls" ADD PRIMARY KEY("id");

CREATE TABLE "ggle_url_links"
(
"urls_id" integer,
"link" varchar(255)
);

ALTER TABLE "ggle_url_links" ADD FOREIGN KEY("urls_id")
	REFERENCES "ggle_urls"("id");

CREATE TABLE "ggle_url_data"
(
"urls_id" integer,
"doc" varchar(100),
"dom" varchar(100),
"size" varchar(25)
);

ALTER TABLE "ggle_url_data" ADD FOREIGN KEY("urls_id")
	REFERENCES "ggle_urls"("id");
