@echo off

set q=slow_compilation.SF-2898944.q1.xq
set p=pf -M
@echo "%p% %q% >/dev/null"
@echo "%p% %q% >/dev/null" >&2
@%p% %q% > nul
set p=pf -A
@echo "%p% %q% >/dev/null"
@echo "%p% %q% >/dev/null" >&2
@%p% %q% > nul

set q=slow_compilation.SF-2898944.q2.xq
set p=pf -M
@echo "%p% %q% >/dev/null"
@echo "%p% %q% >/dev/null" >&2
@%p% %q% > nul
set p=pf -A
@echo "%p% %q% >/dev/null"
@echo "%p% %q% >/dev/null" >&2
@%p% %q% > nul
