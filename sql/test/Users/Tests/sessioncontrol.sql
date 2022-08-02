select sessionid, s."username", s."optimizer", s.sessiontimeout, s.querytimeout, s.workerlimit, s.memorylimit from sessions as s;

call setoptimizer('minimal_pipe');
call setsessiontimeout(5000);
call setquerytimeout(123);
call setworkerlimit(12);
call setmemorylimit(8000);
select sessionid, s."username", s."optimizer", s.sessiontimeout, s.querytimeout, s.workerlimit, s.memorylimit from sessions as s;

call setoptimizer('');
call setsessiontimeout(-1);
call setquerytimeout(-1);
call setworkerlimit( -1);
call setmemorylimit(10);
select sessionid, s."username", s."optimizer", s.sessiontimeout, s.querytimeout, s.workerlimit, s.memorylimit from sessions as s;

call setoptimizer(0, 'sequential_pipe');
call setsessiontimeout(0, 1000);
call setquerytimeout(0, 60);
call setworkerlimit(0,8);
call setmemorylimit(2000);
select sessionid, s."username", s."optimizer", s.sessiontimeout, s.querytimeout, s.workerlimit, s.memorylimit from sessions as s;
