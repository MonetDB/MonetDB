rm MonetDBLite_0.2.2.zip
"c:\Program Files (x86)\GnuWin32\bin\wget.exe" http://dev.monetdb.org/Assets/R/MonetDBLite_0.2.2.tar.gz
"c:\Program Files\R\R-3.2.4revised\bin\R" CMD INSTALL --build --merge-multiarch MonetDBLite_0.2.2.tar.gz 
rm MonetDBLite_0.2.2.tar.gz 
pause
