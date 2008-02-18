@echo off

set q=uncompilable_comparison.SF-1871790.xq
@echo "pf %q% >/dev/null"
@echo "pf %q% >/dev/null" >&2
@%PF% %q% > nul
