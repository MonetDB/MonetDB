-- The stride must be smaller than
create stream table cqinput01(aaa integer) set window 4 stride 5; --error

create stream table cqinput01(aaa integer) set window 5 stride 5; --ok

alter stream table cqinput01 set stride 10; --error

alter stream table cqinput01 set window 4; --error

alter stream table cqinput01 set window 6; --ok

alter stream table cqinput01 set stride 6; --ok

drop table cqinput01;
