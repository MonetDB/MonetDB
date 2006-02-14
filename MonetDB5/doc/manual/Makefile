#This Makefile can be used to generate the documentation
#for off-line printing and creates a webstructure for
#inclusion in the MonetDB5 website

pdf: 
	-mkdir -p ${MONET5_PREFIX}/share/MonetDB5
	mkbdy; cp manual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	pdftex manual.texi \
	)
	cp ${MONET5_PREFIX}/share/MonetDB5/manual.pdf /ufs/mk/cwi/public_html 
	echo "Manual created" 

web:
	-mkdir -p ${MONET5_PREFIX}/share/MonetDB5
	mkbdy; cp manual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp manual.css ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	makeinfo --force --html manual.texi --css-include=manual.css \
	)
	cp -r ${MONET5_PREFIX}/share/MonetDB5/MonetDB /ufs/mk/cwi/public_html 
	echo "Web created"

web2:
	-mkdir -p ${MONET5_PREFIX}/share/MonetDB5
	mkbdy; cp manual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp manual.css ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	texi2html --split=node  --css-include=manual.css manual.texi; \
	cp -r manual /ufs/mk/cwi/public_html/MonetDB \
	)
	echo "Web created"

xml:
	-mkdir -p ${MONET5_PREFIX}/share/MonetDB5
	mkbdy; cp manual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	cp paperclip.xsl ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	makeinfo --force --xml manual.texi \
	)
	sed -i -e "s/&bullet;/<bullet\/>/g" \
		-e "s/&ndash;/<ndash\/>/g" \
		-e "s/&mdash;/<mdash\/>/g" \
		-e "s/&dots;/<dots\/>/g" \
		-e "s/&eosperiod;/<eosperiod\/>/g" \
		-e "s/&space;/<space\/>/g" \
			${MONET5_PREFIX}/share/MonetDB5/MonetDB.xml
	cp ${MONET5_PREFIX}/share/MonetDB5/MonetDB.xml /ufs/mk/cwi/public_html 
	echo "XML file created"

gdkpdf: 
	-mkdir -p ${MONET5_PREFIX}/share/MonetDB5
	mkbdy; cp gdkmanual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	pdftex gdkmanual.texi \
	)
	cp ${MONET5_PREFIX}/share/MonetDB5/gdkmanual.pdf /ufs/mk/cwi/public_html 
	echo "Manual created" 
modpdf: 
	-mkdir -p ${MONET5_PREFIX}/share/MonetDB5
	mkbdy; cp modulesmanual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	pdftex modulesmanual.texi \
	makeinfo --force --html modulesmanual.texi \
	)
	cp ${MONET5_PREFIX}/share/MonetDB5/modulesmanual.pdf /ufs/mk/cwi/public_html 
	echo "Manual created" 
