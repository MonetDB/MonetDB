-- Content based selection from the earth photos.
--    earth : 15 Satellite photographs of earth
--            8bit unsigned  integer (byte) intensity values
--            800 * 800 * 15 pixels
--
--            9 images contain images with values ranging from 0..249
--            3 images contain a single value, 253, 254 ,255 resp.
--            3 images contain values from 0..249 in all but one cell,
--                     with a single cell of value 250, 251 ,252 resp.
--
--            Used in value-based selection, using the values 250..255
--            to 'implicitly select 0..6 images.


-- 1 images 1
SELECT * FROM earth e1 
WHERE e1.x <= 254 AND 
      e1.y <= 254 AND 
      e1.id IN ( SELECT e2.id FROM earth e2 
                 WHERE e2.val = 250);

-- 2 images 1
SELECT * FROM earth e1 
WHERE e1.x <= 254 AND 
      e1.y <= 254 AND 
      e1.id IN ( SELECT e2.id FROM earth e2 
                 WHERE e2.val = 250 OR 
                       e2.val = 251 );

-- 3 images 1
SELECT * FROM earth e1 
WHERE e1.x <= 254 AND 
      e1.y <= 254 AND 
      e1.id IN ( SELECT e2.id FROM earth e2 
                 WHERE e2.val = 250 OR 
                       e2.val = 251 OR
                       e2.val = 252 );

-- 1 images 2
SELECT * FROM earth e1 
WHERE e1.x <= 254 AND 
      e1.y <= 254 AND 
      e1.id IN ( SELECT e2.id FROM earth e2 
                 WHERE e2.val = 253);

-- 2 images 2
SELECT * FROM earth e1 
WHERE e1.x <= 254 AND 
      e1.y <= 254 AND 
      e1.id IN ( SELECT e2.id FROM earth e2 
                 WHERE e2.val = 253 OR 
                       e2.val = 254 );

-- 3 images 2
SELECT * FROM earth e1 
WHERE e1.x <= 254 AND 
      e1.y <= 254 AND 
      e1.id IN ( SELECT e2.id FROM earth e2 
                 WHERE e2.val = 253 OR 
                       e2.val = 254 OR
                       e2.val = 255 );

-- 4 images
SELECT * FROM earth e1 
WHERE e1.x <= 254 AND 
      e1.y <= 254 AND 
      e1.id IN ( SELECT e2.id FROM earth e2 
                 WHERE e2.val = 250 OR 
                       e2.val = 251 OR
                       e2.val = 252 OR
                       e2.val = 253 );

-- 5 images
SELECT * FROM earth e1 
WHERE e1.x <= 254 AND 
      e1.y <= 254 AND 
      e1.id IN ( SELECT e2.id FROM earth e2 
                 WHERE e2.val = 250 OR 
                       e2.val = 251 OR
                       e2.val = 252 OR
                       e2.val = 253 OR
                       e2.val = 254 );

-- 6 images
SELECT * FROM earth e1 
WHERE e1.x <= 254 AND 
      e1.y <= 254 AND 
      e1.id IN ( SELECT e2.id FROM earth e2 
                 WHERE e2.val = 250 OR 
                       e2.val = 251 OR
                       e2.val = 252 OR
                       e2.val = 253 OR
                       e2.val = 254 OR
                       e2.val = 255 );

