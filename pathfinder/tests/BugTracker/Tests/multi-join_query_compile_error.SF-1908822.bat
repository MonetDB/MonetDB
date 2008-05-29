@echo off

set q=multi-join_query_compile_error.SF-1908822.OK.xq
set p=pf -M
@echo "%p% %q% >/dev/null"
@echo "%p% %q% >/dev/null" >&2
@%p% %q% > nul
set p=pf -A
@echo "%p% %q% >/dev/null"
@echo "%p% %q% >/dev/null" >&2
@%p% %q% > nul

set q=multi-join_query_compile_error.SF-1908822.KO.xq
set p=pf -M
@echo "%p% %q% >/dev/null"
@echo "%p% %q% >/dev/null" >&2
@%p% %q% > nul
set p=pf -A
@echo "%p% %q% >/dev/null"
@echo "%p% %q% >/dev/null" >&2
@%p% %q% > nul
