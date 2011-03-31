CREATE VIEW ARRAY wcs_img (
    wcs_x FLOAT DIMENSION,
    wcs_y FLOAT DIMENSION,
    v INTEGER DEFAULT 0) AS
SELECT s[0].v * (m[0][0].v * (img.x - ref[0].v) +
                 m[0][1].v * (img.y - ref[1].v)),
       s[1].v * (m[1][0].v * (img.x - ref[0].v) +
                 m[1][1].v * (img.y - ref[1].v)),
       img.v
FROM img, m, ref, s;

