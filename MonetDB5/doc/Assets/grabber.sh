#!/bin/bash
echo "Collect manuals from the source directories "

M4DIR=/ufs/mk/opensource/MonetDB/
SQLDIR=/ufs/mk/opensource/sql

echo "make Mapi manual"
cp ${M4DIR}/src/mapi/clients/C/Mapi.mx .
Mx -i -B -H1 Mapi.mx 
mv Mapi.bdy.texi mapimanual.texi
rm Mapi.mx

