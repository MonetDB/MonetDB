select * from GENERATE_SERIES(1,41) sample 1.25;            --should give error
select * from GENERATE_SERIES(1,41) seed 1; --error, seed without sample

prepare select * from GENERATE_SERIES(1,41) sample ?;
prepare select * from GENERATE_SERIES(1,41) sample 0.2 seed ?;
prepare select * from GENERATE_SERIES(1,41) sample ? seed ?;
