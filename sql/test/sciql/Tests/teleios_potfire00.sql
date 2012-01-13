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
	declare offset float; set offset =   -0.186592;
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
				( slope * t039_value + offset )
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
	declare offset float; set offset = -10.4568;
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
				( slope * t108_value + offset )
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

with
	input_image_array
as (
	select
		[channel1.x],
		[channel1.y],
		channel1.value as channel1_value,
		channel2.value as channel2_value
	from
			HRIT_039_image_array as channel1
		join
			HRIT_108_image_array as channel2
		on
			channel1.x = channel2.x and
			channel1.y = channel2.y
)
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
from
		(
			select
				[x],
				[y], 
				avg ( T039 ( channel1_value ) )                     as T039_mean,               -- NOT USED !???
				avg ( T108 ( channel2_value ) )                     as T018_mean,               -- NOT USED !???
				sqrt ( avg ( square ( T039 ( channel1_value ) ) ) ) as T039_standard_deviation,
				sqrt ( avg ( square ( T108 ( channel2_value ) ) ) ) as T108_standard_deviation
			from
				input_image_array
			group by
				input_image_array[x-1:x+1][y-1:y+1]
		)
		as windowed_averages
	join
		(
			select
				[x],
				[y], 
				channel1_value,                        -- NOT USED !???
				channel2_value,                        -- NOT USED !???
				T039 ( channel1_value ) as T039_value,
				T108 ( channel2_value ) as T108_value
			from
				input_image_array
		)
		as atomic_values
	on
		windowed_averages.x = atomic_values.x and
		windowed_averages.y = atomic_values.y
;
