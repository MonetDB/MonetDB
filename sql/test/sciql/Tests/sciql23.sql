CREATE FUNCTION intens2radiance(b int, lmin real, lmax real) RETURNS real RETURN (lmax-lmin) * b /255.0 + lmin;

CREATE ARRAY ndvi ( x int DIMENSION[1024], y int DIMENSION[1024], b1 real, b2 real, v real);
UPDATE ndvi SET
  ndvi[x][y].b1 = (
    SELECT intens2radiance(landsat[1][x][y].v, lmin, lmax)
    FROM landsat),
  ndvi[x][y].b2 = (
    SELECT intens2radiance(landsat[1][x][y].v, lmin, lmax)
    FROM landsat);
UPDATE ndvi SET
  ndvi[x][y].v = (ndvi[x][y].b2 - ndvi[x][y].b1) /
                 (ndvi[x][y].b2 + ndvi[x][y].b1);

