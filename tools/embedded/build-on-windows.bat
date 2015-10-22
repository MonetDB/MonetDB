"c:\Program Files (x86)\GnuWin32\bin\wget.exe" http://dev.monetdb.org/Assets/R/MonetDBLite_0.1.0.tar.gz
"c:\Program Files\R\R-3.2.2\bin\R" CMD INSTALL --build MonetDBLite_0.1.0.tar.gz 
rm MonetDBLite_0.1.0.tar.gz 
pause
