#This Makefile can be used to generate the documentation
#for off-line printing and creates a webstructure for
#inclusion in the MonetDB5 website

ps,pdf: 
	mkbdy; cp manual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	pdftex manual.texi \
	)
	cp ${MONET5_PREFIX}/share/MonetDB5/manual.pdf /ufs/mk/cwi/public_html 
	echo "Manual created" 

web:
	mkbdy; cp manual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	makeinfo --force --html manual.texi \
	)
	cp -r ${MONET5_PREFIX}/share/MonetDB5/MonetDB /ufs/mk/cwi/public_html 
	echo "Web created"

xml:
	mkbdy; cp manual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	makeinfo --force --xml manual.texi \
	)
	cp ${MONET5_PREFIX}/share/MonetDB5/MonetDB5.xml /ufs/mk/cwi/public_html 
	echo "XML file created"
