select * from GENERATE_SERIES(1,41) sample 0;               --empty set
select * from GENERATE_SERIES(1,41) sample 1 seed 1234;     --1 sample
select * from GENERATE_SERIES(1,41) sample 10 seed 1234;    --10 samples
select * from GENERATE_SERIES(1,41) sample 20 seed 1234;    --20 samples
select * from GENERATE_SERIES(1,41) sample 30 seed 1234;    --30 samples
select * from GENERATE_SERIES(1,41) sample 30 seed 4321;    --another 30 samples
select * from GENERATE_SERIES(1,41) sample 40;              --full table

select * from GENERATE_SERIES(1,41) sample 0.0;             --empty set
select * from GENERATE_SERIES(1,41) sample 0.025 seed 1234; --1 sample
select * from GENERATE_SERIES(1,41) sample 0.25 seed 1234;  --10 samples
select * from GENERATE_SERIES(1,41) sample 0.5 seed 1234;   --20 samples
select * from GENERATE_SERIES(1,41) sample 0.75 seed 1234;  --30 samples
select * from GENERATE_SERIES(1,41) sample 0.75 seed 4321;  --another 30 samples
select * from GENERATE_SERIES(1,41) sample 1.0;             --full table
select * from GENERATE_SERIES(1,41) sample 1.25;            --should give error
