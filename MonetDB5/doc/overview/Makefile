#This Makefile can be used to generate the documentation
#for off-line printing and creates a webstructure for
#inclusion in the MonetDB website
ps,pdf:
	cp modules.tex overview.tex *.sty  ../../Linux/share/MonetDB
	(cd ../../Linux/share/MonetDB/; \
	latex -interaction=batchmode overview.tex ;\
	latex -interaction=batchmode overview.tex ;\
	dvips overview.dvi -o overview.ps ; \
	ps2pdf overview.ps overview.pdf; \
	latex -interaction=batchmode modules.tex ;\
	latex -interaction=batchmode modules.tex ;\
	dvips modules.dvi -o modules.ps ; \
	ps2pdf modules.ps modules.pdf; \
	)

web:
	rm -rf ../../Linux/share/MonetDB/Web
	mkdir ../../Linux/share/MonetDB/web
	cp overview.shtml ../../Linux/share/MonetDB/web
	(cd ../../Linux/share/MonetDB/; \
	mkdir web/mal; cp docs/mal/*html web/mal; \
	mkdir web/gdk; cp docs/gdk/*html web/gdk; \
	)
	echo "Web created"
