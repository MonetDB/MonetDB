build: Mapi_wrap.c setup.py
	python setup.py build

Mapi_wrap.c: Mapi.i ../../C/Mapi/Mapi.h
	swig -python -I../../C/Mapi Mapi.i

install: build
	python setup.py install
