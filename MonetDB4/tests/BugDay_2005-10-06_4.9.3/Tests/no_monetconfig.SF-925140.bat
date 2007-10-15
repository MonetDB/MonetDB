@echo off

echo monet_daemon=no > "%TSTTRGBASE%\config"

call Mlog.bat echo "loaded(); quit();" ^| Mserver %setGDK_DBFARM% %setMONETDB_MOD_PATH% "--config=%TSTTRGBASE%\config"
call          echo  loaded(); quit();   | Mserver %setGDK_DBFARM% %setMONETDB_MOD_PATH% "--config=%TSTTRGBASE%\config"
