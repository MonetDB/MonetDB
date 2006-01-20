@echo on
@prompt # $t $g

@set NAME=%1

echo "%MSERVER% --dbname=$TSTDB --set monet_daemon=yes"
%MSERVER% --dbname=%TSTDB% --set monet_daemon=yes"

