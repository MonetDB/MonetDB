select * from quser;

select * from query;

select * from ssqq_queue;

declare id_result int;

set id_result = -1;

set id_result = insert_quser(NULL, NULL, NULL, NULL);

select id_result;

select * from quser;

set id_result = -1;

set id_result = insert_quser('user1', 'email1@example.com', 1, 'secret');

select id_result;

select * from quser;

set id_result = -1;

set id_result = insert_quser('user2', 'email2@example.com', 2, 'notsecret');

select id_result;

select * from quser;

set id_result = -1;

set id_result = insert_quser('user1', 'email3@example.com', 3, 'nosecret');

select id_result;

select * from quser;

select * from query;

select * from ssqq_queue;
