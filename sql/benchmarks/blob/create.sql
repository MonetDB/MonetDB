CREATE TABLE adttest (
        id INTEGER NOT NULL,
        dosname CHAR(11),
        filename VARCHAR(255),
        contenttype VARCHAR(255),
        blobsize INTEGER,
        blobdata BLOB,
        PRIMARY KEY (id)
        );
