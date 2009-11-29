create table cm_tmp(i int);
plan copy into cm_tmp from 'file1','file2';
drop table cm_tmp;
