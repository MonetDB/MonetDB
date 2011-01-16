declare id_file1 integer;
set id_file1 = -1234567890;
declare id_media1 integer;
set id_media1 = -1234567890;

set id_media1 = (select media_id
                 from   media
                 where  identifier = 'identifier1');

set id_file1 = add_file(id_media1,
                        'fabchannel2007',
                        'filename 1',
                        'codec 1',
                        2000,
                        1,
                        'url 1',
                        'empty',
                        250,
                        250);

select id_file1;
