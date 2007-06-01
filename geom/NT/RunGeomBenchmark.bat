@echo off
pushd %~dp0

set mildir=%cd%\..\src\Tests
set milscript=benchmark.mil

set logfile=%mildir%\benchmark.log
echo. > "%logfile%"

echo %time%
echo %time% >> "%logfile%"

call RunMserver.bat --set monet_prompt= --trace < "%mildir%\%milscript%" >> "%logfile%"

echo.
echo. >> "%logfile%"
echo %time%
echo %time% >> "%logfile%"

popd

pause
