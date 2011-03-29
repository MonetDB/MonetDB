-- FIXME: 1) don't we miss an 'img' in the FROM clause?
--        2) added an 'img.' to the 'x' in the 'power(...)'
UPDATE img SET
  img[x][y].v = (
    SELECT d[x/2][y].v + e[x/2][y].v * power(-1,img.x) 
    FROM d, e);

