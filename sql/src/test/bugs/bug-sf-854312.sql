CREATE TABLE a (
        var1 char(4)      NOT NULL,
        var2 varchar(255),
        var3 int,
        var4 varchar(16)  NOT NULL,
        PRIMARY KEY (var1),
        UNIQUE(var1)
);

CREATE TABLE b (
        rowid int         NOT NULL,
        id char(4)        NOT NULL,
        var1 int,
        var2 int,
        var3 varchar(20)  NOT NULL,
        var4 char(20) NOT NULL,
        PRIMARY KEY (rowid),
        FOREIGN KEY (id) REFERENCES a (var1) ON DELETE CASCADE
);

SELECT * FROM a LEFT JOIN b ON a.var1 = b.id WHERE a.var1 = 'aJan';

SELECT * FROM a LEFT JOIN b ON a.var1 = b.id WHERE var1 = 'aJan';

