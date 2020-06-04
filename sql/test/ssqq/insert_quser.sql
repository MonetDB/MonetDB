select * from quser;

select * from query;

select * from ssqq_queue;

select insert_quser(NULL, NULL, NULL, NULL);

select * from quser;

select insert_quser('user1', 'email1@example.com', 1, 'secret');

select * from quser;

select insert_quser('user2', 'email2@example.com', 2, 'notsecret');

select * from quser;

select insert_quser('user1', 'email3@example.com', 3, 'nosecret');

select * from quser;

select * from query;

select * from ssqq_queue;
