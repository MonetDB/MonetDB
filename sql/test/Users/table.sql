SET ROLE my_role;

CREATE TABLE my_schema.my_table (
  obsid INT NOT NULL AUTO_INCREMENT,
  time_s BIGINT NULL, 
  time_e BIGINT NULL,
  PRIMARY KEY (obsid)
);
INSERT INTO my_schema.my_table (time_s) values (300);
