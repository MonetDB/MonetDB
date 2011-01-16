@echo off

echo monet_daemon=no > "%TSTTRGBASE%\config"

prompt # $t $g  
echo on

call echo loaded(); quit(); | Mserver %setGDK_DBFARM% %setMONETDB_MOD_PATH% "--config=%TSTTRGBASE%\config"
