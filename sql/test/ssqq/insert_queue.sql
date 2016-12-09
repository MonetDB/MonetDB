select * from quser;

select * from query;

select queue_id, query_id, queue_number, os_version, monetdb_version, start_query, query_ready from ssqq_queue;

declare id_result int;

set id_result = -1;

set id_result = insert_queue(1, 'monet', 'linux');

select id_result;

select queue_id, query_id, queue_number, os_version, monetdb_version, start_query, query_ready from ssqq_queue;

set id_result = -1;

set id_result = insert_queue(10, 'monet', 'linux');

select id_result;

select queue_id, query_id, queue_number, os_version, monetdb_version, start_query, query_ready from ssqq_queue;

set id_result = -1;

set id_result = insert_queue(2, 'monet', 'linux');

select id_result;

select queue_id, query_id, queue_number, os_version, monetdb_version, start_query, query_ready from ssqq_queue;

set id_result = -1;

set id_result = insert_queue(4, 'monet', 'linux');

select id_result;

select * from quser;

select * from query;

select queue_id, query_id, queue_number, os_version, monetdb_version, start_query, query_ready from ssqq_queue;

