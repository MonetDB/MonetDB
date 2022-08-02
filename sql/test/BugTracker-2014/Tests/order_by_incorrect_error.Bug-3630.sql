
CREATE TABLE "sys"."sirivm" (
"vehiclenumber"          INTEGER,
"recordedattime"         TIMESTAMP,
"linkdistance"           INTEGER,
"percentage"             DECIMAL(5,2),
"lineref"                CHARACTER LARGE OBJECT,
"dataframeref"           CHARACTER LARGE OBJECT,
"datedvehiclejourneyref" CHARACTER LARGE OBJECT,
"monitored"              BOOLEAN,
"mon_error"              CHARACTER LARGE OBJECT,
"blockref"               CHARACTER LARGE OBJECT,
"delay"                  INTEGER,
"lon"                    DOUBLE,
"lat"                    DOUBLE,
"laststopref"            CHARACTER LARGE OBJECT,
"laststoporder"          INTEGER,
"lastaimedarr"           TIMESTAMP,
"lastaimeddep"           TIMESTAMP,
"monstopref"             CHARACTER LARGE OBJECT,
"monstoporder"           INTEGER,
"monaimedarr"            TIMESTAMP,
"monaimeddep"            TIMESTAMP,
"monatstop"              CHARACTER LARGE OBJECT,
"monexpectedarr"         TIMESTAMP,
"monexpecteddep"         TIMESTAMP,
"nextstopref"            CHARACTER LARGE OBJECT,
"nextstoporder"          INTEGER,
"nextaimedarr"           TIMESTAMP,
"nextaimeddep"           TIMESTAMP,
"nextexpectedarr"        TIMESTAMP,
"nextexpecteddep"        TIMESTAMP,
"journeypattern"         CHARACTER LARGE OBJECT
); 
select a.recordedattime, a.monexpectedarr, a.monexpecteddep, a.monaimedarr, b.monaimeddep, a.delay, b.delay, (b.recordedattime - a.monaimedarr)/1000 as ourdelay,  b.recordedattime, b.monexpectedarr, b.monexpecteddep, monstopref from sirivm as a join sirivm as b using (vehiclenumber, lineref, datedvehiclejourneyref, monstopref) where a.monatstop = 'false' and b.monatstop = 'true' order by abs(ourdelay) desc limit 10;

drop TABLE "sys"."sirivm";

