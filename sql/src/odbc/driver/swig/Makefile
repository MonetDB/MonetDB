SWIG = swig
PYTHON = python

all:	odbc_wrap.c setup.py
	$(PYTHON) setup.py build

odbc_wrap.c: odbc.i
	rm -rf build
	$(SWIG) -python -DHAVE_LONG_LONG odbc.i

clean:
	rm -rf odbc_wrap.c odbc.py build
