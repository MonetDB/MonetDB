CREATE FUNCTION intens2radiance(b int, lmin real, lmax real) RETURNS real RETURN (lmax-lmin) * b /255.0 + lmin;

CREATE ARRAY ndvi ( x int DIMENSION[1024], y int DIMENSION[1024], b1 real, b2 real, v real);
UPDATE ndvi 
	SET b1 = (SELECT intens2radiance(v, lmin, lmax)
		FROM landsat WHERE channel = 1
		AND landsat.x = ndvi.x AND landsat.y = ndvi.y), 
	b2 = (SELECT intens2radiance(v, lmin, lmax)
		FROM landsat WHERE channel =2
		AND landsat.x = ndvi.x AND	landsat.y = ndvi.y), 
	v=(b2-b1)/(b2+b1);
