
CREATE TABLE "sys"."phpbb_topics" ( 
        "topic_id" int NOT NULL DEFAULT '0',
        "forum_id" int NOT NULL DEFAULT '0', 
        "icon_id" int NOT NULL DEFAULT '0', 
        "topic_attachment" smallint NOT NULL DEFAULT '0', 
        "topic_approved" smallint NOT NULL DEFAULT '1', 
        "topic_reported" smallint NOT NULL DEFAULT '0', 
        "topic_title" varchar(255) NOT NULL DEFAULT '', 
        "topic_poster" int NOT NULL DEFAULT '0', 
        "topic_time" int NOT NULL DEFAULT '0', 
        "topic_time_limit" int NOT NULL DEFAULT '0', 
        "topic_views" int NOT NULL DEFAULT '0', 
        "topic_replies" int NOT NULL DEFAULT '0', 
        "topic_replies_real" int NOT NULL DEFAULT '0', 
        "topic_status" smallint NOT NULL DEFAULT '0', 
        "topic_type" smallint NOT NULL DEFAULT '0', 
        "topic_first_post_id" int NOT NULL DEFAULT '0', 
        "topic_first_poster_name" varchar(255) NOT NULL DEFAULT '', 
        "topic_first_poster_colour" varchar(6) NOT NULL DEFAULT '', 
        "topic_last_post_id" int NOT NULL DEFAULT '0', 
        "topic_last_poster_id" int NOT NULL DEFAULT '0', 
        "topic_last_poster_name" varchar(255) NOT NULL DEFAULT '', 
        "topic_last_poster_colour" varchar(6) NOT NULL DEFAULT '', 
        "topic_last_post_subject" varchar(255) NOT NULL DEFAULT '', 
        "topic_last_post_time" int NOT NULL DEFAULT '0', 
        "topic_last_view_time" int NOT NULL DEFAULT '0', 
        "topic_moved_id" int NOT NULL DEFAULT '0', 
        "topic_bumped" smallint NOT NULL DEFAULT '0', 
        "topic_bumper" int NOT NULL DEFAULT '0', 
        "poll_title" varchar(255) NOT NULL DEFAULT '', 
        "poll_start" int NOT NULL DEFAULT '0', 
        "poll_length" int NOT NULL DEFAULT '0', 
        "poll_max_options" smallint NOT NULL DEFAULT '1', 
        "poll_last_vote" int NOT NULL DEFAULT '0', 
        "poll_vote_change" smallint NOT NULL DEFAULT '0', 
        CONSTRAINT "phpbb_topics_topic_id_pkey" PRIMARY KEY ("topic_id") 
); 

UPDATE 
	phpbb_topics 
SET 
	topic_first_poster_name = 'Skinkie',
	topic_last_poster_name = 'Skinkie' 
WHERE 
	topic_first_poster_name = 'Admin' OR topic_last_poster_name = 'Admin';

drop table phpbb_topics;
