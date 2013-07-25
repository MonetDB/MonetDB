-- FIXME: don't we miss an 'img' in the FROM clause?
UPDATE img SET
  v = (SELECT d.v + e.v * power(-1, img.x) FROM d, e
       WHERE img.y = d.y AND img.y = e.y AND d.x = img.x/2 AND e.x = img.x/2);

