#This Makefile can be used to generate the documentation
#for off-line printing and creates a webstructure for
#inclusion in the MonetDB5 website

all:
	make pdf
	make web
	make xml

pdf: 
	-mkdir -p ${MONET5_PREFIX}/share/MonetDB5
	mkbdy; cp manual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	pdftex manual.texi \
	)
	cp ${MONET5_PREFIX}/share/MonetDB5/manual.pdf ..
	(cd ..; pdfnup manual.pdf)
	echo "Manual created" 

web:
	-mkdir -p ${MONET5_PREFIX}/share/MonetDB5
	mkbdy; cp manual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp manual.css ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	makeinfo --force --html manual.texi --css-include=manual.css \
	)
	cp -r ${MONET5_PREFIX}/share/MonetDB5/MonetDB ../web
	echo "Web version created"

web2:
	-mkdir -p ${MONET5_PREFIX}/share/MonetDB5
	mkbdy; cp manual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp manual.css ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	texi2html --split=node  --css-include=manual.css manual.texi; )
	cp -r $(MONET5_PREFIX)/share/MonetDB5/MonetDB ../web/MonetDB_split
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
	cp ${MONET5_PREFIX}/share/MonetDB5/MonetDB.xml ../manual.xml
	echo "XML file created"

gdkpdf: 
	-mkdir -p ${MONET5_PREFIX}/share/MonetDB5
	mkbdy; cp gdkmanual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	pdftex gdkmanual.texi \
	)
	cp ${MONET5_PREFIX}/share/MonetDB5/gdkmanual.pdf ..
	(cd ..; pdfnup gdkmanual.pdf)
	echo "GDK manual created" 
modpdf: 
	-mkdir -p ${MONET5_PREFIX}/share/MonetDB5
	mkbdy; cp modulesmanual.texi ${MONET5_PREFIX}/share/MonetDB5
	cp -r ../Assets ${MONET5_PREFIX}/share/MonetDB5
	(cd ${MONET5_PREFIX}/share/MonetDB5/; \
	pdftex modulesmanual.texi \
	makeinfo --force --html modulesmanual.texi \
	)
	cp ${MONET5_PREFIX}/share/MonetDB5/modulesmanual.pdf ../modules.pdf
	(cd ..; pdfnup modules.pdf)
	echo "Modules manual created" 
