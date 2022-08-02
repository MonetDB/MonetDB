START TRANSACTION;

CREATE TABLE "productfeature" (
  "nr" int primary key,
  "label" varchar(100) default NULL,
  "comment" varchar(2000) default NULL,
  "publisher" int,
  "publishDate" date
);
CREATE TABLE "producttype" (
  "nr" int primary key,
  "label" varchar(100) default NULL,
  "comment" varchar(2000) default NULL,
  "parent" int,
  "publisher" int,
  "publishDate" date
);
CREATE TABLE "producer" (
  "nr" int primary key,
  "label" varchar(100) default NULL,
  "comment" varchar(2000) default NULL,
  "homepage" varchar(100) default NULL,
  "country" char(2) ,
  "publisher" int,
  "publishDate" date
);
CREATE TABLE "product" (
  "nr" int primary key,
  "label" varchar(100) default NULL,
  "comment" varchar(2000) default NULL,
  "producer" int default NULL,
  "propertyNum1" int default NULL,
  "propertyNum2" int default NULL,
  "propertyNum3" int default NULL,
  "propertyNum4" int default NULL,
  "propertyNum5" int default NULL,
  "propertyNum6" int default NULL,
  "propertyTex1" varchar(250) default NULL,
  "propertyTex2" varchar(250) default NULL,
  "propertyTex3" varchar(250) default NULL,
  "propertyTex4" varchar(250) default NULL,
  "propertyTex5" varchar(250) default NULL,
  "propertyTex6" varchar(250) default NULL,
  "publisher" int default NULL,
  "publishDate" date default NULL
);
CREATE TABLE "producttypeproduct" (
  "product" int not null,
  "productType" int not null,
  PRIMARY KEY ("product", "productType")
);
CREATE TABLE "productfeatureproduct" (
  "product" int not null,
  "productFeature" int not null,
  PRIMARY KEY ("product", "productFeature")
);
CREATE TABLE "vendor" (
  "nr" int primary key,
  "label" varchar(100) default NULL,
  "comment" varchar(2000) default NULL,
  "homepage" varchar(100) default NULL,
  "country" char(2) ,
  "publisher" int,
  "publishDate" date
);
CREATE TABLE "offer" (
  "nr" int primary key,
  "product" int,
  "producer" int,
  "vendor" int,
  "price" double default null,
  "validFrom" date default null,
  "validTo" date default null,
  "deliveryDays" int default null,
  "offerWebpage" varchar(100) ,
  "publisher" int,
  "publishDate" date
);
CREATE TABLE "person" (
  "nr" int primary key,
  "name" varchar(30) default NULL,
  "mbox_sha1sum" char(40) ,
  "country" char(2) ,
  "publisher" int,
  "publishDate" date
);
CREATE TABLE "review" (
  "nr" int primary key,
  "product" int,
  "producer" int,
  "person" int,
  "reviewDate" date default NULL,
  "title" varchar(200) default NULL,
  "text" text ,
  "language" char(2) ,
  "rating1" int default NULL,
  "rating2" int default NULL,
  "rating3" int default NULL,
  "rating4" int default NULL,
  "publisher" int,
  "publishDate" date
);
SELECT p.nr, p.label
  FROM product p, producttypeproduct ptp
  WHERE p.nr=ptp.product
    AND "productType"=35
    AND "propertyNum1">500
    AND "propertyNum3"<2000
    AND 168 IN (SELECT "productFeature" FROM productfeatureproduct WHERE productfeatureproduct.product=p.nr)
    AND 147 NOT IN (SELECT "productFeature" FROM productfeatureproduct WHERE product=p.nr);

ROLLBACK;
