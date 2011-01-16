declare id_event integer;
set id_event = -1;

set id_event = add_event('event 4', 123456, 'artist2', 'location3', 'description3');

select id_event;
set id_event = add_event('event 4', '2009-06-23', NULL, NULL, 'description3');
select id_event;

delete from event where event_name = 'event 2';

insert into event_metadata (
    event_id,
    metadata_name,
    metadata_value )
values ( 1,
        'artist',
        'artist1' );

set id_event = add_event('event 4', '2009-06-24', 'artist4', 'location4', 'description4');

select id_event; 
