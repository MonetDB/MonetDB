COPY INTO atom  from 'PWD/ATOM.flt.tbl'  USING DELIMITERS ',','\n';
COPY INTO bond  from 'PWD/BOND.flt.tbl'  USING DELIMITERS ',','\n';
COPY INTO model from 'PWD/MODEL.flt.tbl' USING DELIMITERS ',','\n';
commit;
