-- http://dev.lsstcorp.org/trac/wiki/dbQuery000
-- the queries assume the following views and table
-- exist in addition to Precursor Schema:

DROP VIEW Galaxy;
CREATE VIEW Galaxy AS
    SELECT Object.*,
           ObjectPhotoZ.redshift, ObjectPhotoZ.redshiftErr
    FROM   Object
    JOIN   _Object2Type USING (objectId)
    JOIN   ObjectType   USING (typeId)
    JOIN   ObjectPhotoZ ON (Object.objectId=ObjectPhotoZ.objectId)
    WHERE  ObjectType.description = "galaxy"
      AND  _Object2Type.probability > 90;  -- 0-100%

DROP VIEW Star;
CREATE VIEW Star AS
    SELECT Object.*,
           ObjectPhotoZ.redshift, ObjectPhotoZ.redshiftErr
    FROM   Object
    JOIN   _Object2Type USING (objectId)
    JOIN   ObjectType   USING (typeId)
    JOIN   ObjectPhotoZ ON (Object.objectId=ObjectPhotoZ.objectId)
    WHERE  ObjectType.description = "star"
      AND  _Object2Type.probability > 90;  -- 0-100%

--DROP TABLE IF EXISTS Neighbors;
--DROP TABLE Neighbors;
CREATE TABLE Neighbors (
  objectId         BIGINT,
  neighborObjectId BIGINT,
  distance         FLOAT,
  neighborType     TINYINT
);
