#This Makefile can be used to generate the documentation
#for off-line printing and creates a webstructure for
#inclusion in the MonetDB5 website
ps,pdf: 
	cp modules.tex overview.tex *.sty  ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
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
	rm -rf ${MONET5_PREFIX}/share/MonetDB5/Web
	mkdir ${MONET5_PREFIX}/share/MonetDB5/web
	cp overview.shtml ${MONET5_PREFIX}/share/MonetDB5/web
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	mkdir web/mal; cp docs/mal/*html web/mal; \
	)
	echo "Web created"

mklocal: ${MONET5_PREFIX}/share/MonetDB5/overview.pdf
	cp ${MONET5_PREFIX}/share/MonetDB5/overview.pdf /ufs/mk/cwi/public_html/monet5
	cp ${MONET5_PREFIX}/share/MonetDB5/modules.pdf /ufs/mk/cwi/public_html/monet5
