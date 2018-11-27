START TRANSACTION;
COPY INTO atom  from 'PWD/ATOM.int.tbl'  USING DELIMITERS ',',E'\n';
COPY INTO bond  from 'PWD/BOND.int.tbl'  USING DELIMITERS ',',E'\n';
COPY INTO model from 'PWD/MODEL.int.tbl' USING DELIMITERS ',',E'\n';
commit;
