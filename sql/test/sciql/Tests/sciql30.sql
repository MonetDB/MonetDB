UPDATE img 
	SET wcs_x = (SELECT s[0].v * (m[0][0].v * (img.x-ref[0].v) + m[0][1].v * (img.y - ref[1].v)) FROM m, ref, s), 
	wcs_y = (SELECT s[1].v * (m[1][0].v * (img.x-ref[0].v) + m[1][1].v * (img.y - ref[1].v)) FROM m, ref, s);
