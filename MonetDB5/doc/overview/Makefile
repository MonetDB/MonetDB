#This makefile should be converted to Makefile.ag 
overview:
	rm -rf ../../Linux/doc
	mkdir ../../Linux/doc
	python mktexdoc.py ../.. ../../Linux/
	cp overview.tex  ../../Linux/doc
	cp latex8.sty  ../../Linux/doc
	cp multiFloats.sty ../../Linux/doc
	(cd ../../Linux/doc; \
	latex -interaction=batchmode overview.tex ;\
	dvips overview.dvi -o overview.ps ; \
	ps2pdf overview.ps overview.pdf)
