start transaction;
CREATE TABLE ProductTarget (ProductID SERIAL, Name VARCHAR(100) NOT NULL, ProductNumber BIGINT, Color VARCHAR(30) NOT NULL);
CREATE TABLE ProductSource (ProductID SERIAL, Name VARCHAR(100) NOT NULL, ProductNumber BIGINT, Color VARCHAR(30) NOT NULL);
MERGE INTO ProductTarget T USING ProductSource S ON S.ProductID = T.ProductID WHEN MATCHED THEN UPDATE SET Name = S.Name;
rollback;
