declare id_event1 integer;

declare id_media1 integer;

declare id_media_description1 integer;

set id_event1 = (select max(event_id) + 1 from event);

set id_media_description1 = (select max(media_description_id) from media_description);

set id_media1 = add_media(id_event1, id_media_description1, 'description 3', 1, 25, 125);

select id_media1;
