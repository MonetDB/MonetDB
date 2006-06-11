#This Makefile can be used to generate the documentation
#for off-line printing and creates a webstructure for
#inclusion in the MonetDB5 website

all:
	-mkdir -p ${MONET5_PREFIX}/share/doc
	make pdf
	make web
	make xml
	make clean

pdf: 
	-mkdir -p ${MONET5_PREFIX}/share/tmp
	mkbdy; cp manual.texi ${MONET5_PREFIX}/share/tmp
	cp -r ../Assets ${MONET5_PREFIX}/share/tmp
	(cd ${MONET5_PREFIX}/share/tmp/; pdftex manual.texi )
	cp ${MONET5_PREFIX}/share/tmp/manual.pdf ../share
	(cd ../share/; pdfnup manual.pdf)
	echo "Manual created" 

web:
	-mkdir -p ${MONET5_PREFIX}/share/tmp
	mkbdy; cp manual.texi ${MONET5_PREFIX}/share/tmp
	cp manual.css ${MONET5_PREFIX}/share/tmp
	cp -r ../Assets ${MONET5_PREFIX}/share/tmp
	(cd ${MONET5_PREFIX}/share/tmp/; \
	makeinfo --force --html manual.texi --css-include=manual.css \
	)
	cp -r ${MONET5_PREFIX}/share/tmp/MonetDB ../share
	echo "Web version created"

web2:
	-mkdir -p ${MONET5_PREFIX}/share/tmp
	mkbdy; cp manual.texi ${MONET5_PREFIX}/share/tmp
	cp manual.css ${MONET5_PREFIX}/share/tmp
	cp -r ../Assets ${MONET5_PREFIX}/share/tmp
	(cd ${MONET5_PREFIX}/share/tmp/; \
	texi2html --split=node  --css-include=manual.css manual.texi; )
	cp -r $(MONET5_PREFIX)/share/tmp/MonetDB ../share
	echo "Web created"

xml:
	-mkdir -p ${MONET5_PREFIX}/share/tmp
	mkbdy; cp manual.texi ${MONET5_PREFIX}/share/tmp
	cp -r ../Assets ${MONET5_PREFIX}/share/tmp
	(cd ${MONET5_PREFIX}/share/tmp/; \
	makeinfo --force --xml manual.texi \
	)
	sed -i -e "s/&bullet;/<bullet\/>/g" \
		-e "s/&ndash;/<ndash\/>/g" \
		-e "s/&mdash;/<mdash\/>/g" \
		-e "s/&dots;/<dots\/>/g" \
		-e "s/&eosperiod;/<eosperiod\/>/g" \
		-e "s/&space;/<space\/>/g" \
			${MONET5_PREFIX}/share/tmp/MonetDB.xml
	cp ${MONET5_PREFIX}/share/tmp/MonetDB.xml ../share
	echo "XML file created"

gdkpdf: 
	-mkdir -p ${MONET5_PREFIX}/share/tmp
	mkbdy; cp gdkmanual.texi ${MONET5_PREFIX}/share/tmp
	cp -r ../Assets ${MONET5_PREFIX}/share/tmp
	(cd ${MONET5_PREFIX}/share/tmp/; \
	pdftex gdkmanual.texi \
	)
	cp ${MONET5_PREFIX}/share/tmp/gdkmanual.pdf ../share
	(cd $(MONET5_PREFIX)/share/doc; pdfnup gdkmanual.pdf)
	echo "GDK manual created" 

modpdf: 
	-mkdir -p ${MONET5_PREFIX}/share/tmp
	mkbdy; cp modulesmanual.texi ${MONET5_PREFIX}/share/tmp
	cp -r ../Assets ${MONET5_PREFIX}/share/tmp
	(cd ${MONET5_PREFIX}/share/tmp/; \
	pdftex modulesmanual.texi \
	makeinfo --force --html modulesmanual.texi \
	)
	cp ${MONET5_PREFIX}/share/tmp/modulesmanual.pdf ../share
	(cd share; pdfnup modules.pdf)
	echo "Modules manual created" 

clean:
	rm -rf $(MONET5_PREFIX)/share/tmp
