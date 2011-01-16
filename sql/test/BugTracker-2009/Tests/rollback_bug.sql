
START TRANSACTION;

CREATE SEQUENCE "sys"."phpbb_attachments_seq" AS INTEGER;

CREATE TABLE phpbb_attachments (
	attach_id integer DEFAULT next value for "sys"."phpbb_attachments_seq",
	post_msg_id integer DEFAULT '0' NOT NULL ,
	topic_id integer DEFAULT '0' NOT NULL ,
	in_message smallint DEFAULT '0' NOT NULL ,
	poster_id integer DEFAULT '0' NOT NULL ,
	is_orphan LONG DEFAULT '1' NOT NULL ,
	physical_filename varchar(255) DEFAULT '' NOT NULL,
	real_filename varchar(255) DEFAULT '' NOT NULL,
	download_count integer DEFAULT '0' NOT NULL ,
	attach_comment varchar(4000) DEFAULT '' NOT NULL,
	extension varchar(100) DEFAULT '' NOT NULL,
	mimetype varchar(100) DEFAULT '' NOT NULL,
	filesize integer DEFAULT '0' NOT NULL ,
	filetime integer DEFAULT '0' NOT NULL ,
	thumbnail smallint DEFAULT '0' NOT NULL ,
	PRIMARY KEY (attach_id)
);

/*
	Table: 'phpbb_acl_groups'
*/
CREATE TABLE phpbb_acl_groups (
	group_id integer DEFAULT '0' NOT NULL ,
	forum_id integer DEFAULT '0' NOT NULL ,
	auth_option_id integer DEFAULT '0' NOT NULL ,
	auth_role_id integer DEFAULT '0' NOT NULL ,
	auth_setting smallint DEFAULT '0' NOT NULL
);

COMMIT;

START TRANSACTION;

CREATE SEQUENCE "sys"."phpbb_attachments_seq" AS INTEGER;

CREATE TABLE phpbb_attachments (
	attach_id integer DEFAULT next value for "sys"."phpbb_attachments_seq",
	post_msg_id integer DEFAULT '0' NOT NULL ,
	topic_id integer DEFAULT '0' NOT NULL ,
	in_message smallint DEFAULT '0' NOT NULL ,
	poster_id integer DEFAULT '0' NOT NULL ,
	is_orphan smallint DEFAULT '1' NOT NULL ,
	physical_filename varchar(255) DEFAULT '' NOT NULL,
	real_filename varchar(255) DEFAULT '' NOT NULL,
	download_count integer DEFAULT '0' NOT NULL ,
	attach_comment varchar(4000) DEFAULT '' NOT NULL,
	extension varchar(100) DEFAULT '' NOT NULL,
	mimetype varchar(100) DEFAULT '' NOT NULL,
	filesize integer DEFAULT '0' NOT NULL ,
	filetime integer DEFAULT '0' NOT NULL ,
	thumbnail smallint DEFAULT '0' NOT NULL ,
	PRIMARY KEY (attach_id)
);

CREATE TABLE phpbb_acl_groups (
	group_id integer DEFAULT '0' NOT NULL ,
	forum_id integer DEFAULT '0' NOT NULL ,
	auth_option_id integer DEFAULT '0' NOT NULL ,
	auth_role_id integer DEFAULT '0' NOT NULL ,
	auth_setting smallint DEFAULT '0' NOT NULL
);

COMMIT;

select * from phpbb_acl_groups;

drop table phpbb_acl_groups;
drop table phpbb_attachments;
drop sequence phpbb_attachments_seq;

