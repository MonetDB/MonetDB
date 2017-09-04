create function aggr00()     
returns integer
begin
    declare s int;
	set s = 0;
	while (true)
	do
		set s = s + 1;
		yield s;
	end while;
	return s;
END;
select name from functions where name ='aggr00';

-- to call a continuous function in the scheduler, we must pass the keyword "function" explicitly
start continuous function aggr00();

call cquery.wait(1000); #give it time to start
select aggr00(); #should return 1
select aggr00(); #should return 2

stop continuous function aggr00();
drop function aggr00;

--factory factories.aggr00():int;             	#[0] (0)  0 
--#function user.aggr00():int;             	#[0] (0)  0 
--    As := nil:int;                      	#[1] (0)  2 <- 3 
--    As := 0:int;                        	#[2] (0)  2 <- 5 
--barrier X_8 := true;                    	#[3] (0)  8 <- 7 
--    leave X_8 := false:bit;             	#[4] (0)  8 <- 54 
--    X_12 := As;                         	#[5] (0)  12 <- 2 
--    X_13 := calc.lng(X_12);             	#[6] (0) CMDvarCONVERT 13 <- 12 
--    X_16:lng := calc.+(X_13, 1:lng);    	#[7] (0) CMDvarADDsignal 16 <- 13 15 
--    X_17 := calc.int(X_16);             	#[8] (0) CMDvarCONVERT 17 <- 16 
--    As := X_17;                         	#[9] (0)  2 <- 17 
--	yield aggr00:int := As;
--    # return aggr00:int := As;            	#[10] (0)  0 <- 2 
--    redo X_8 := true;                   	#[11] (0)  8 <- 7 
--exit X_8;                               	#[12] (0)  8 
--    return aggr00:int := As;            	#[13] (0)  0 <- 2 
--end factories.aggr00;                        	#[14] (0)  
