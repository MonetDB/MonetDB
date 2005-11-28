CREATE TABLE a (
        var1 char(4)      NOT NULL,
        var2 varchar(255),
        var3 bigint,
        var4 varchar(16)  NOT NULL,
        PRIMARY KEY (var1),
        UNIQUE(var1)
);

CREATE TABLE b (
        r_id int         NOT NULL,
        id char(4)        NOT NULL,
        var1 bigint,
        var2 bigint,
        var3 varchar(20)  NOT NULL,
        var4 char(20) NOT NULL,
        PRIMARY KEY (r_id),
        FOREIGN KEY (id) REFERENCES a (var1) ON DELETE CASCADE
);

SELECT * FROM a LEFT JOIN b ON a.var1 = b.id WHERE a.var1 = 'aJan';

SELECT * FROM a LEFT JOIN b ON a.var1 = b.id WHERE var1 = 'aJan';

INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('1', 'aaab', 1877611059, 2136665433, 'Bibliotheek', 'U');
INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('2', 'aaab', 1181479988, NULL, 'hier', 'verwachten');
INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('3', 'aaab', 1906079357, 1955202416, 'tot', 'van');
INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('4', 'aaac', 676937782, NULL, 'als zijn', 'z');
INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('5', 'aaac', 429573099, 1767983525, 'o', 'het');
INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('6', 'aaad', 1173429043, 216615742, 'Welk', 'nieuwe boeken');
INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('7', 'aaad', 1722898798, 412510482, 'onlangs', 'oege');
INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('8', 'aaad', 1111774637, 2025755091, 'Er is', 'lijst met');
INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('9', 'aaad', 2106443074, 500880029, 'be', 'schikb');
INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('10', 'aaae', 1862088833, 1636412274, '', 'Klik');
INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('11', 'aaae', 740103512, 1959552303, 'om een gratis', 'te');
INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('12', 'aaae', 615168073, 1166551474, 'Doo', 'met de rechter');
INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('13', 'aaae', 1714170141, 1144940450, 'is', 'op');
INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('14', 'aaaf', 293281126, 1101721875, 'klikken', 'kunt u');
INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('15', 'aaaf', 1371008872, 1420507527, 'bestan', 'naar uw');
INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('16', 'aaaf', 1238690311, 2013589728, 'overhalen', 'en');
INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('17', 'aaaf', 1598516811, 1682499981, 'opslaan. U', 'op');
INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('18', 'aaag', 1551706168, 1147370461, 'reide', 'en');
INSERT INTO b (r_id, id, var1, var2, var3, var4) VALUES ('19', 'aaag', 1178314077, 670110160, '', 'boek');

SELECT * FROM a LEFT JOIN b ON a.var1 = b.id WHERE a.var1 = 'aJan';

SELECT * FROM a LEFT JOIN b ON a.var1 = b.id WHERE var1 = 'aJan';

drop table b;
drop table a;
