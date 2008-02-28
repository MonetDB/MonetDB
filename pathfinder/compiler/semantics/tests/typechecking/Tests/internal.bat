@echo off

REM Invoke Pathfinder-internal test routines.
REM
REM There's no means to feed a pair of types for testing purposes
REM into the compiler.  Hence, we hardcode test cases into the
REM compiler (only if --enable-assert) and invoke them with the
REM -d command line option.

call echo 42 | pf -d subtyping -s11
