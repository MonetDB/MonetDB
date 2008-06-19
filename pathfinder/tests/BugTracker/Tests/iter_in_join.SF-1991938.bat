@echo off

set q=iter_in_join.SF-1991938.xq
set p=pf -M
@echo "%p% %q% >/dev/null"
@echo "%p% %q% >/dev/null" >&2
@%p% %q% > nul
set p=pf -A
@echo "%p% %q% >/dev/null"
@echo "%p% %q% >/dev/null" >&2
@%p% %q% > nul
