declare return_id integer;
set return_id = -1;

set return_id = i_add_video_file('dummy description',
                                 'fabchannel2007',
                                 'identifier4',
                                 'filename 3',
                                 250,
                                 250,
                                 25,
                                 125,
                                 'codec 1',
                                 2000);

select return_id;
