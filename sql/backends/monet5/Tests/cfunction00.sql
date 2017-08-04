create function myaggr()     
returns integer
begin
    declare s int;
	set s = 0;
	while (true)
	do
		set s = s + 1;
		return s ; --yield s;
	end while;
	return s;
END;
select * from functions where name ='myaggr';

-- a continuous procedure can be called like any other procedure
start continuous  myaggr();

select myaggr(); #should return 1
select myaggr(); #should return 2

drop function myproc;

--factory factories.myaggr():int;             	#[0] (0)  0 
--#function user.myaggr():int;             	#[0] (0)  0 
--    As := nil:int;                      	#[1] (0)  2 <- 3 
--    As := 0:int;                        	#[2] (0)  2 <- 5 
--barrier X_8 := true;                    	#[3] (0)  8 <- 7 
--    leave X_8 := false:bit;             	#[4] (0)  8 <- 54 
--    X_12 := As;                         	#[5] (0)  12 <- 2 
--    X_13 := calc.lng(X_12);             	#[6] (0) CMDvarCONVERT 13 <- 12 
--    X_16:lng := calc.+(X_13, 1:lng);    	#[7] (0) CMDvarADDsignal 16 <- 13 15 
--    X_17 := calc.int(X_16);             	#[8] (0) CMDvarCONVERT 17 <- 16 
--    As := X_17;                         	#[9] (0)  2 <- 17 
--	yield myaggr:int := As;
--    # return myaggr:int := As;            	#[10] (0)  0 <- 2 
--    redo X_8 := true;                   	#[11] (0)  8 <- 7 
--exit X_8;                               	#[12] (0)  8 
--    return myaggr:int := As;            	#[13] (0)  0 <- 2 
--end factories.myaggr;                        	#[14] (0)  
