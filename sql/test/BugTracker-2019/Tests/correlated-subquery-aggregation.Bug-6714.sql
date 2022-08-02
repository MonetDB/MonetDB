start transaction;
create table functions_test(id int, name varchar(64));
create table args_test(id int, func_id int, name varchar(64), number int);
insert into functions_test values (1495, 'copyfrom'), (6743, 'querylog_calls'), (6802, 'tracelog'), (7234, 'bbp'),
                                  (1, 'dummy');
COPY 27 RECORDS INTO args_test FROM STDIN NULL AS '';
6393|1495|res_0|0|
6394|1495|arg_1|1|
6395|1495|arg_2|2|
6396|1495|arg_3|3|
6744|6743|id|0|
6745|6743|start|1|
6746|6743|stop|2|
6747|6743|arguments|3|
6748|6743|tuples|4|
6749|6743|run|5|
6750|6743|ship|6|
6751|6743|cpu|7|
6752|6743|io|8|
6803|6802|event|0|
6804|6802|clk|1|
6805|6802|pc|2|
6806|6802|thread|3|
6807|6802|ticks|4|
6808|6802|rrsmb|5|
6809|6802|vmmb|6|
6810|6802|reads|7|
6811|6802|writes|8|
6812|6802|minflt|9|
6813|6802|majflt|10|
6814|6802|nvcsw|11|
6815|6802|stmt|12|
1|1|dummy|1|

select func_id, (select name from functions_test f where f.id = func_id) as name, max(number), count(*) from args_test
group by func_id having count(*) > 8 order by func_id limit 12;
rollback;
