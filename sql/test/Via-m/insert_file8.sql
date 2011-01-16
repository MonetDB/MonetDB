declare return_id integer;
set return_id = -1234567890;

set return_id = i_add_video_file_event('fabchannel2007',
                                       'event 4',
                                       'dummy description',
                                       4,
                                       'filename 4',
                                       250,
                                       250,
                                       25,
                                       125,
                                       'codec 1',
                                       2000);

select return_id;
