start transaction;

CREATE TABLE geo_values (
	    id integer NOT NULL,
	    strdfgeo geometry
);

CREATE TABLE label_values (
	    id integer NOT NULL,
	    value character varying(255) NOT NULL
);

CREATE TABLE long_label_values (
	    id integer NOT NULL,
	    value text NOT NULL
);

CREATE TABLE long_uri_values (
	    id integer NOT NULL,
	    value text NOT NULL
);

CREATE TABLE triples (
	    ctx integer NOT NULL,
	    subj integer NOT NULL,
	    pred integer NOT NULL,
	    obj integer NOT NULL,
	    expl boolean NOT NULL,
	    interval_start timestamp,
	    interval_end timestamp
);

CREATE TABLE uri_values (
	    id integer NOT NULL,
	    value character varying(255) NOT NULL
);

SELECT DISTINCT 
h3.obj, 
CASE 
WHEN u_ENDPOINT.value IS NOT NULL THEN u_ENDPOINT.value 
WHEN l_ENDPOINT.value IS NOT NULL THEN l_ENDPOINT.value 
WHEN ll_ENDPOINT.value IS NOT NULL THEN ll_ENDPOINT.value 
WHEN lu_ENDPOINT.value IS NOT NULL THEN lu_ENDPOINT.value 
END
FROM 
triples t0 
INNER JOIN triples h1 ON (h1.pred = 21 AND h1.obj = 42 AND h1.subj =
t0.subj) 
INNER JOIN triples h2 ON (h2.pred = 5 AND h2.subj = t0.subj) 
INNER JOIN triples h3 ON (h3.pred = 20 AND h3.subj = t0.subj)     
INNER JOIN triples i4 ON (i4.pred = 28 AND i4.subj = h2.obj)     
INNER JOIN triples c5 ON (c5.pred = 8 AND c5.subj = h2.obj)     
INNER JOIN triples h6 ON (h6.pred = 11 AND h6.subj = c5.obj)     
INNER JOIN triples h7 ON (h7.subj = 50 AND h7.pred = 11 )     
LEFT JOIN uri_values u_PROPERTYTYPE ON (u_PROPERTYTYPE.id = i4.obj)     
LEFT JOIN long_uri_values lu_PROPERTYTYPE ON (lu_PROPERTYTYPE.id = i4.obj)  
LEFT JOIN label_values l_PROPERTYTYPE ON (l_PROPERTYTYPE.id = i4.obj)     
LEFT JOIN long_label_values ll_PROPERTYTYPE ON (ll_PROPERTYTYPE.id =
i4.obj)     
LEFT JOIN geo_values l_SERVICEREGIONGEO ON (l_SERVICEREGIONGEO.id = h6.obj) 
LEFT JOIN long_label_values ll_SERVICEREGIONGEO ON (ll_SERVICEREGIONGEO.id
= h6.obj)     
LEFT JOIN geo_values l_SOLENTGEO ON (l_SOLENTGEO.id = h7.obj)     
LEFT JOIN long_label_values ll_SOLENTGEO ON (ll_SOLENTGEO.id = h7.obj) 
LEFT JOIN uri_values u_ENDPOINT ON (u_ENDPOINT.id = h3.obj)     
LEFT JOIN label_values l_ENDPOINT ON (l_ENDPOINT.id = h3.obj)     
LEFT JOIN long_label_values ll_ENDPOINT ON (ll_ENDPOINT.id = h3.obj) 
LEFT JOIN long_uri_values lu_ENDPOINT ON (lu_ENDPOINT.id = h3.obj)     
WHERE
t0.pred = 2 
AND t0.obj = 3 
AND (
CASE WHEN NOT(u_PROPERTYTYPE.value IS NULL AND lu_PROPERTYTYPE.value IS
NOT NULL AND lu_PROPERTYTYPE.value IS NULL ) AND NOT(u_PROPERTYTYPE.value IS
NULL AND lu_PROPERTYTYPE.value IS NULL ) THEN 
CASE     WHEN u_PROPERTYTYPE.value IS NOT NULL THEN
u_PROPERTYTYPE.value 
WHEN lu_PROPERTYTYPE.value IS NOT NULL THEN
lu_PROPERTYTYPE.value 
END 
WHEN NOT(l_PROPERTYTYPE.value IS NULL AND ll_PROPERTYTYPE.value IS NOT
NULL AND ll_PROPERTYTYPE.value IS NULL ) AND NOT(l_PROPERTYTYPE.value IS NULL
AND ll_PROPERTYTYPE.value IS NULL ) THEN 
CASE     WHEN l_PROPERTYTYPE.value IS NOT NULL THEN
l_PROPERTYTYPE.value 
WHEN ll_PROPERTYTYPE.value IS NOT NULL THEN
ll_PROPERTYTYPE.value 
END 
END = 'example1' 
OR
CASE WHEN NOT(u_PROPERTYTYPE.value IS NULL AND lu_PROPERTYTYPE.value IS
NOT NULL AND lu_PROPERTYTYPE.value IS NULL ) AND NOT(u_PROPERTYTYPE.value IS
NULL AND lu_PROPERTYTYPE.value IS NULL ) THEN 
CASE     WHEN u_PROPERTYTYPE.value IS NOT NULL THEN
u_PROPERTYTYPE.value 
WHEN lu_PROPERTYTYPE.value IS NOT NULL THEN
lu_PROPERTYTYPE.value 
END 
WHEN NOT(l_PROPERTYTYPE.value IS NULL AND ll_PROPERTYTYPE.value IS NOT
NULL AND ll_PROPERTYTYPE.value IS NULL ) AND NOT(l_PROPERTYTYPE.value IS NULL
AND ll_PROPERTYTYPE.value IS NULL ) THEN 
CASE     WHEN l_PROPERTYTYPE.value IS NOT NULL THEN
l_PROPERTYTYPE.value 
WHEN ll_PROPERTYTYPE.value IS NOT NULL THEN
ll_PROPERTYTYPE.value 
	END
	END = 'example2' 
	)
	AND (ST_Within(l_SERVICEREGIONGEO.strdfgeo,l_SOLENTGEO.strdfgeo));
rollback;
