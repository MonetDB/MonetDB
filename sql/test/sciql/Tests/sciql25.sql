UPDATE img SET v = (SELECT d.v + e.v * power(-1,x) FROM d, e
WHERE img.y = d.y and img.y = e.y AND d.x = img.x/2 AND e.x = img.x/2);
