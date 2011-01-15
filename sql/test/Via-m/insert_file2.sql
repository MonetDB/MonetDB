declare next_file_quid integer;
set next_file_quid = -1234567890;

set next_file_quid = get_next_file_quid('fabchannel2007');

select next_file_quid;
