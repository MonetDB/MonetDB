-- To be done tests.
-- Using an 'int' rather then a 'tinyint' calls for casting the generated values first
-- The two join cases illustrate how a join could be optimized by 'looking' up the correct value.

select * from generate_series(0,10,2) X, generate_series(0,4,2) Y where X.value = Y.value;
select * from generate_series(0,4,2) X, generate_series(0,10,2) Y where X.value = Y.value;

select * from generate_series(0,10,3) X, generate_series(0,4,2) Y where X.value = Y.value;
