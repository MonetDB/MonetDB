@echo on
@prompt # $t $g  

	%MTIMEOUT% Mserver -monetrc %MONETRC% -?
	%MTIMEOUT% MapiClient -?
	%MTIMEOUT% Mcreatedb -?
	%MTIMEOUT% Mdestroydb -?
	%MTIMEOUT% Mshutdown -?

