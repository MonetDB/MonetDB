SWIG = swig
PYTHON = python

odbc_wrap.c: odbc.i
	rm -rf build
	$(SWIG) -python -DHAVE_LONG_LONG odbc.i

build:
	$(PYTHON) setup.py build

clean:
	rm -rf odbc_wrap.c odbc.py build
