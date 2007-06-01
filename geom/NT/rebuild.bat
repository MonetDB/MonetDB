call "%VS80COMNTOOLS%vsvars32.bat"

pushd %~dp0

nmake NEED_MX=1 clear-all
nmake NEED_MX=1 check
