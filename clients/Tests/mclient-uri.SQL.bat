@prompt # $t $g  
@echo on

@rem Windows doesn't do UNIX domain sockets, so only the one test here.

mclient -d "mapi:monetdb://%HOST%:%MAPIPORT%/%TSTDB%?language=sql&user=monetdb" -f test -E utf-8 -s "select 1"
