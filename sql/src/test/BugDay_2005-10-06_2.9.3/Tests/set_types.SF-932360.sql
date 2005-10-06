set reply_size = 1;
set auto_commit = false;
set reply_size = true;
ROLLBACK;
set auto_commit = 60;
ROLLBACK;
set reply_size = false;
ROLLBACK;
set auto_commit = 0;
