@echo on
@prompt # $t $g

@set NAME=%1

echo "%MSERVER% --dbpath=%GDK_DBFARM%\$TSTDB --set monet_daemon=yes"
%MSERVER% "--dbpath=%GDK_DBFARM%\%TSTDB%" --set monet_daemon=yes"

