declare id_event1 integer;
set id_event1 = -1;

declare id_media1 integer;
set id_media1 = -1;

declare id_media_description1 integer;
set id_media_description1 = -1234567890;

set id_event1 = (select event_id from event where event_name = 'event 1');

set id_media_description1 = (select max(media_description_id) from media_description);

set id_media1 = add_media(id_event1, id_media_description1, NULL, 1, 25, 125);
