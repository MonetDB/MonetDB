create schema lrbm_bug_2831;

CREATE TABLE lrbm_bug_2831.accident(carid1 integer, carid2 integer, firstMinute integer,
lastMinute integer, dir integer, seg integer, pos integer);
CREATE TABLE lrbm_bug_2831.statistics(dir  int,seg  int,time_minute int,numvehicles 
int,lav   float,toll  int,accident int,accidentSeg int);
UPDATE lrbm_bug_2831.statistics 
    SET    toll = 0, accident = 1 
    WHERE EXISTS(   
        SELECT acc.seg
        FROM   lrbm_bug_2831.accident AS acc
        WHERE  acc.dir = statistics.dir AND
            acc.firstMinute + 1 <= statistics.time_minute AND
            acc.lastMinute + 1 >= statistics.time_minute AND
            ( 
                (
                    (acc.dir = 0) AND
                    (acc.seg >= statistics.seg) AND
                    (acc.seg <= statistics.seg + 4)
                )
                OR
                (
                    (acc.dir <> 0) AND
                    (acc.seg <= statistics.seg) AND
                    (acc.seg >= statistics.seg - 4)
                )
            )
    );

drop table lrbm_bug_2831.statistics;
drop table lrbm_bug_2831.accident;
drop schema lrbm_bug_2831;
