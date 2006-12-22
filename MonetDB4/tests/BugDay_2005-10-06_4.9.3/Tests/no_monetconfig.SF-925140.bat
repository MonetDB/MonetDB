echo "monet_daemon=no" > %TSTTRGBASE%\config
call Mlog.bat "echo 'loaded(); quit();' \| Mserver --config=%TSTTRGBASE%\config"
call           echo 'loaded(); quit();'  | Mserver --config=%TSTTRGBASE%\config
