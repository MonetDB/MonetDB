"c:\Program Files (x86)\GnuWin32\bin\wget.exe" http://dev.monetdb.org/Assets/R/MonetDBLite_0.2.0.tar.gz
"c:\Program Files\R\R-3.2.3\bin\R" CMD INSTALL --build --force-biarch MonetDBLite_0.2.0.tar.gz 
rm MonetDBLite_0.2.0.tar.gz 
pause
