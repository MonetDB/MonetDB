START TRANSACTION;
CREATE TABLE whatgoodisadatabaseifyoucannotimport(a STRING, b INTEGER);
COPY 1 RECORDS INTO whatgoodisadatabaseifyoucannotimport FROM STDIN USING DELIMITERS ' ', '\n', '';
asdf\ 42
SELECT * FROM whatgoodisadatabaseifyoucannotimport;
ROLLBACK;
