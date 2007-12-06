
CREATE TABLE query_users (
        user_id INT NOT NULL AUTO_INCREMENT,
        user_name VARCHAR(20),
        PRIMARY KEY (user_id)
);


INSERT INTO query_users (user_id, user_name) VALUES (1, 'john');

INSERT INTO query_users (user_id, user_name) VALUES (2, 'jack');
INSERT INTO query_users (user_id, user_name) VALUES (3, 'ed');
INSERT INTO query_users (user_id, user_name) VALUES (4, 'wendy');
INSERT INTO query_users (user_id, user_name) VALUES (5, 'laura');
INSERT INTO query_users (user_id, user_name) VALUES (6, 'ralph');
INSERT INTO query_users (user_id, user_name) VALUES (7, 'fido');

select * from query_users;

SELECT query_users.user_id, query_users.user_name
FROM query_users ORDER BY query_users.user_id
LIMIT 3 OFFSET 2;

SELECT query_users.user_id, query_users.user_name
FROM query_users LIMIT 3 OFFSET 2;

select * from query_users OFFSET 2;

select * from query_users order by user_name;
select * from query_users order BY user_name OFFSET 2;

drop table query_users;
