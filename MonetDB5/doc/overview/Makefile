#This Makefile can be used to generate the documentation
#for off-line printing and creates a webstructure for
#inclusion in the MonetDB5 website
ps,pdf: 
	cp modules.tex overview.tex *.sty  ../../Linux/share/MonetDB5
	(cd ../../Linux/share/MonetDB5/; \
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
	rm -rf ../../Linux/share/MonetDB5/Web
	mkdir ../../Linux/share/MonetDB5/web
	cp overview.shtml ../../Linux/share/MonetDB5/web
	(cd ../../Linux/share/MonetDB5/; \
	mkdir web/mal; cp docs/mal/*html web/mal; \
	mkdir web/gdk; cp docs/gdk/*html web/gdk; \
	)
	echo "Web created"

mklocal: ../../Linux/share/MonetDB5/overview.pdf
	cp ../../Linux/share/MonetDB5/overview.pdf /ufs/mk/cwi/public_html/monet5
	cp ../../Linux/share/MonetDB5/modules.pdf /ufs/mk/cwi/public_html/monet5
