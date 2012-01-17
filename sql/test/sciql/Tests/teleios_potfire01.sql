create array HRIT_039_image_array (x integer dimension[300], y integer dimension[300], value integer default 23);
create array HRIT_108_image_array (x integer dimension[300], y integer dimension[300], value integer default 42);
create array input_image_array    (x integer dimension[300], y integer dimension[300], channel1_value integer default 0, channel2_value integer default 0);
create array atomic_values (x integer dimension[300], y integer dimension[300], T039_value integer default 0, T108_value integer default 0);

declare threshold_1 float; set threshold_1 = 310.0;
declare threshold_2 float; set threshold_2 =   2.5;
declare threshold_3 float; set threshold_3 =   8.0;
declare threshold_4 float; set threshold_4 =   2.0;
declare threshold_5 float; set threshold_5 = 310.0;
declare threshold_6 float; set threshold_6 =   4.0;
declare threshold_7 float; set threshold_7 =  10.0;
declare threshold_8 float; set threshold_8 =   8.0;

create function
	T039 ( t039_value float )
returns
	float
begin 
	declare slope  float; set slope  =    0.00365867;
	declare voffset float; set voffset =   -0.186592;
	declare c1     float; set c1     =    0.0000119104;
	declare c2     float; set c2     =    1.4387700000;
	declare vc     float; set vc     = 2569.0940000;
	declare a      float; set a      =    0.9959000000;
	declare b      float; set b      =    3.4710000000;
	return
		(
			c2 * vc
			/
			log (
				c1 * vc * vc * vc
				/
				( slope * t039_value + voffset )
				+
				1
			)
			-
			b
		)
		/
		a;
end;

create function
	T108 ( t108_value float )
returns
	float
begin 
	declare slope  float; set slope  =   0.205034;
	declare voffset float; set voffset = -10.4568;
	declare c1     float; set c1     =   0.0000119104;
	declare c2     float; set c2     =   1.4387700000;
	declare vc     float; set vc     = 930.659;
	declare a      float; set a      =   0.9983;
	declare b      float; set b      =   0.627;
	return
		(
			c2 * vc
			/
			log(
				c1 * vc * vc * vc
				/
				( slope * t108_value + voffset )
				+
				1
			)
			-
			b
		)
		/
		a;
end;

create function
	square ( a_value float )
returns
	float
begin 
	return a_value * a_value;
end;

insert into input_image_array
	select 
		channel1.x, -- could be omitted with "JOIN ON DIMENSIONS"
		channel1.y, -- could be omitted with "JOIN ON DIMENSIONS"
		channel1.value as channel1_value,
		channel2.value as channel2_value
	from
			HRIT_039_image_array as channel1
		join
			HRIT_108_image_array as channel2
		on -- DIMENSIONS -- aka. / á la "natural join"
			channel1.x = channel2.x and
			channel1.y = channel2.y
;

select
	[atomic_values.x],
	[atomic_values.y],
	case
		when
			T039_value              > threshold_5 and
			T039_value - T108_value > threshold_7 and
			T039_standard_deviation > threshold_6 and
			T108_standard_deviation < threshold_8
		then
			2
		when
			T039_value              > threshold_1 and
			T039_value - T108_value > threshold_3 and
			T039_standard_deviation > threshold_2 and
			T108_standard_deviation < threshold_4
		then
			1
		else
			0
	end
	as confidence
from (
	select 
		[x],
		[y],
		T039 ( channel1_value ) as T039_value,
		T108 ( channel2_value ) as T108_value,
		sqrt(T039_standard_deviation_tmp - square(T_039_mean)) as T039_standard_deviation,
		sqrt(T108_standard_deviation_tmp - square(T_108_mean)) as T108_standard_deviation
	from
		(
			select
				[x],
				[y], 
				channel1_value,
				channel2_value,
				avg ( T039 ( channel1_value ) )            as T039_mean,
				avg ( T108 ( channel2_value ) )            as T018_mean,
				avg ( square ( T039 ( channel1_value ) ) ) as T039_standard_deviation_tmp,
				avg ( square ( T108 ( channel2_value ) ) ) as T108_standard_deviation_tmp
			from
				input_image_array
			group by
				input_image_array[x-1:x+2][y-1:y+2]
		) 
		as tmp
	) as tmp2
;

insert into atomic_values
	select
		x,
		y,
		T039 ( channel1_value ) as T039_value,
		T108 ( channel2_value ) as T108_value
	from
		input_image_array
;

select 
	[x],
	[y],
	T039_value,
	T108_value,
	sqrt(T039_standard_deviation_tmp - T_039_mean * T_039_mean) as T039_standard_deviation,
	sqrt(T108_standard_deviation_tmp - T_108_mean * T_108_mean) as T108_standard_deviation
from
	(
		select
			[x],
			[y], 
			T039_value,
			T108_value,
			avg ( T039_value )              as T039_mean,
			avg ( T108_value )              as T018_mean,
			avg ( T039_value * T039_value ) as T039_standard_deviation_tmp,
			avg ( T108_value * T108_value ) as T108_standard_deviation_tmp
		from
			atomic_values
		group by
			atomic_values[x-1:x+2][y-1:y+2]
	) 
	as tmp
;

