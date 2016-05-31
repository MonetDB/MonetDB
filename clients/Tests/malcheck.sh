#!/bin/sh
cd $TSTSRCBASE
hg files -I 'monetdb5/**.[ch]' -I 'geom/**.[ch]' -I 'sql/**.[ch]' -X '**/Tests/**' | python -c 'import MonetDBtesting.malcheck'
