CREATE TABLE query_users (
        user_id INT NOT NULL AUTO_INCREMENT,
        user_name VARCHAR(20),
        PRIMARY KEY (user_id)
);


INSERT INTO query_users (user_id, user_name) VALUES (1, 'matt');

INSERT INTO query_users (user_id, user_name) VALUES (2, 'fred');

INSERT INTO query_users (user_id, user_name) VALUES (3, null);

SELECT user_name
FROM query_users
WHERE user_name IN ('matt', 'fred');

-- should return empty set, returns null
SELECT user_name
FROM query_users
WHERE user_name NOT IN ('matt', 'fred');

drop table query_users;
