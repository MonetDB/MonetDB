SWIG = swig
PYTHON = python

all:	odbc_wrap.c setup.py
	$(PYTHON) setup.py build

odbc_wrap.c: odbc.i
	rm -rf build
	$(SWIG) -python -DHAVE_LONG_LONG -I/ufs/sjoerd/Monet/include odbc.i
	echo 'error = _odbc.error' >> odbc.py

clean:
	rm -rf odbc_wrap.c odbc.py odbc.pyc odbc.pyo build
