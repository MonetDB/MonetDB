--
-- CREATE_AGGREGATE
--

-- all functions CREATEd
CREATE AGGREGATE newavg (
   sfunc = int4_accum, basetype = integer, stype = _numeric, 
   finalfunc = numeric_avg,
   initcond1 = '{0,0,0}'
);

-- test comments




-- without finalfunc; test obsolete spellings 'sfunc1' etc
CREATE AGGREGATE newsum (
   sfunc1 = int4pl, basetype = integer, stype1 = integer, 
   initcond1 = '0'
);

-- value-independent transition function
CREATE AGGREGATE newcnt (
   sfunc = int4inc, basetype = 'any', stype = integer,
   initcond = '0'
);




