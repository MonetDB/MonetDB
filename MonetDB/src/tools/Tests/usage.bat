@echo on
@prompt # $t $g  

	%MTIMEOUT% Mserver --config=%MONETRC% -?
	%MTIMEOUT% MapiClient -?
	%MTIMEOUT% Mshutdown -?

