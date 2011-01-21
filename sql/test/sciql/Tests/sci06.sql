SELECT x, y, v FROM matrix WHERE v >2; 
SELECT x, y, v FROM stripes WHERE v >2; 
SELECT [x], [y], v FROM matrix WHERE v >2; 

CREATE TABLE T (i int, k int);
SELECT [T.k], [y], v FROM matrix JOIN T ON matrix.x = T.i;
