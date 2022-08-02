CREATE TABLE ProductTarget (ProductID SERIAL, Name VARCHAR(100) NOT NULL, ProductNumber BIGINT, Color VARCHAR(30) NOT NULL);
CREATE TABLE ProductSource (ProductID SERIAL, Name VARCHAR(100) NOT NULL, ProductNumber BIGINT, Color VARCHAR(30) NOT NULL);

MERGE INTO ProductTarget T USING ProductSource S ON S.ProductID = T.ProductID WHEN MATCHED THEN UPDATE SET Name = S.Name;

MERGE INTO ProductTarget T USING ProductSource S ON S.ProductID = T.ProductID WHEN MATCHED THEN UPDATE SET T.Name = S.Name, ProductTarget.ProductNumber = S.ProductNumber, ProductTarget.Color = S.Color;

MERGE INTO ProductTarget T USING ProductSource S ON S.ProductID = T.ProductID WHEN NOT MATCHED THEN INSERT (T.ProductID, ProductTarget.ProductNumber, ProductTarget.Color, T.Name) VALUES (S.ProductID, S.ProductNumber, S.Color, S.Name);

MERGE INTO ProductTarget T USING ProductSource S ON S.ProductID = T.ProductID
 WHEN MATCHED THEN UPDATE SET Name = S.Name, ProductNumber = S.ProductNumber, Color = S.Color
 WHEN NOT MATCHED THEN INSERT (ProductID, ProductNumber, Color, Name) VALUES (S.ProductID, S.ProductNumber, Color, Name);

MERGE INTO ProductTarget T USING ProductSource S ON S.ProductID = T.ProductID
 WHEN MATCHED     THEN UPDATE SET ProductTarget.Name = S.Name, ProductTarget.ProductNumber = S.ProductNumber, T.Color = S.Color
 WHEN NOT MATCHED THEN INSERT (T.ProductID, ProductTarget.ProductNumber, ProductTarget.Color, T.Name) VALUES (ProductID, S.ProductNumber, S.Color, Name);

MERGE INTO ProductTarget T USING ProductSource S ON T.ProductID = S.ProductID
 WHEN MATCHED     THEN UPDATE SET T.Name = S.Name, T.ProductNumber = ProductNumber, T.Color = Color
 WHEN NOT MATCHED THEN INSERT (T.ProductID, T.ProductNumber, T.Color, T.Name) VALUES (ProductID, S.ProductNumber, S.Color, Name);

DROP TABLE ProductTarget;
DROP TABLE ProductSource;

