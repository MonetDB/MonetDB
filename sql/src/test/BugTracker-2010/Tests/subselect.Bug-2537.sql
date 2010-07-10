CREATE TABLE Image (ImageId integer, imageBinaries string);

INSERT INTO Image VALUES (1, 'A');

SELECT * from Image;

INSERT INTO Image
       (ImageId
       ,imageBinaries)
VALUES
       (MAX(ImageId)+1,'data');

SELECT * from Image;

INSERT INTO Image
       (ImageId
       ,imageBinaries)
VALUES
       ( (SELECT MAX(ImageId)+1 from Image),'data');

SELECT * from Image;

DROP TABLE Image;
