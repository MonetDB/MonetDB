#This Makefile can be used to generate the documentation
#for off-line printing and creates a webstructure for
#inclusion in the MonetDB5 website

pdf: 
	-mkdir ${MONET5_PREFIX}/share/MonetDB5
	mkbdy; cp manual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	pdftex manual.texi \
	)
	cp ${MONET5_PREFIX}/share/MonetDB5/manual.pdf /ufs/mk/cwi/public_html 
	echo "Manual created" 

web:
	-mkdir ${MONET5_PREFIX}/share/MonetDB5
	mkbdy; cp manual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	makeinfo --force --html manual.texi \
	)
	cp -r ${MONET5_PREFIX}/share/MonetDB5/MonetDB /ufs/mk/cwi/public_html 
	echo "Web created"

xml:
	-mkdir ${MONET5_PREFIX}/share/MonetDB5
	mkbdy; cp manual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	makeinfo --force --xml manual.texi \
	)
	cp ${MONET5_PREFIX}/share/MonetDB5/MonetDB5.xml /ufs/mk/cwi/public_html 
	echo "XML file created"

gdkpdf: 
	-mkdir ${MONET5_PREFIX}/share/MonetDB5
	mkbdy; cp gdkmanual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	pdftex gdkmanual.texi \
	)
	cp ${MONET5_PREFIX}/share/MonetDB5/gdkmanual.pdf /ufs/mk/cwi/public_html 
	echo "Manual created" 
modpdf: 
	-mkdir ${MONET5_PREFIX}/share/MonetDB5
	mkbdy; cp modulesmanual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	pdftex modulesmanual.texi \
	makeinfo --force --html modulesmanual.texi \
	)
	cp ${MONET5_PREFIX}/share/MonetDB5/modulesmanual.pdf /ufs/mk/cwi/public_html 
	echo "Manual created" 
