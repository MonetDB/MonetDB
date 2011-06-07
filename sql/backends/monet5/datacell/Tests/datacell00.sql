#set up the minimal test environment for datacell

create schema datacell;
create table datacell.X( id int, tag timestamp, payload int);
create table datacell.Y( id int, tag timestamp, payload int, msdelay int);
