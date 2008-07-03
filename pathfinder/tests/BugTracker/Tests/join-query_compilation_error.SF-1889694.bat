@echo off

set q=join-query_compilation_error.SF-1889694.xq
set p=pf -M
@echo "%p% %q% >/dev/null"
@echo "%p% %q% >/dev/null" >&2
@%p% %q% > nul
set p=pf -A
@echo "%p% %q% >/dev/null"
@echo "%p% %q% >/dev/null" >&2
@%p% %q% > nul
