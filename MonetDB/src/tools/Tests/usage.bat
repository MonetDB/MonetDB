@echo on
@prompt # $t $g  

	%MTIMEOUT% Mserver "--config=%MONET_CONF%" -?
	%MTIMEOUT% MapiClient -?
	%MTIMEOUT% MapiClient.py -?
	%MTIMEOUT% msqldump -?
