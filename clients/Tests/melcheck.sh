#!/bin/sh
cd $TSTSRCBASE

hg_root="$(hg root 2>/dev/null)"
git_root="$(git rev-parse --show-toplevel 2>/dev/null)"

if [ "$hg_root" = "$TSTSRCBASE" ]; then
	hg -q files -I '{monetdb5,sql,geom}/**.[ch]' -X '**/Tests/**'
elif [ "$git_root" = "$TSTSRCBASE" ]; then
	git ls-files -- 'monetdb5/**.[ch]' 'sql/**.[ch]' 'geom/**.[ch]' ':!:**/Tests/*'
fi \
| python3 -c 'import MonetDBtesting.melcheck'
