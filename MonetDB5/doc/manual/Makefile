#This Makefile can be used to generate the documentation
#for off-line printing and creates a webstructure for
#inclusion in the MonetDB5 website

M5DIR = ../..
M4DIR = /path/to/m4/sources

all:
	mkdir -p $(M5DIR)/doc/share/doc
	make pdf
	make web
	make xml
	make clean

pdf: 
	mkdir -p $(M5DIR)/doc/share/tmp
	mkbdy $(M5DIR) $(M4DIR); cp manual.texi $(M5DIR)/doc/share/tmp
	cp -r ../Assets $(M5DIR)/doc/share/tmp
	(cd $(M5DIR)/doc/share/tmp/; pdftex manual.texi; pdftex manual.texi )
	cp $(M5DIR)/doc/share/tmp/manual.pdf $(M5DIR)/doc/share
	(cd $(M5DIR)/doc/share/; pdfnup manual.pdf)
	echo "Manual created" 

web:
	echo "Create tmp:" $(M5DIR)/doc/share/tmp
	echo "using:" $(M4DIR)
	mkdir -p $(M5DIR)/doc/share/tmp
	mkbdy $(M5DIR) $(M4DIR)
	cp manual.texi $(M5DIR)/doc/share/tmp
	cp manual.css $(M5DIR)/doc/share/tmp
	cp -r ../Assets $(M5DIR)/doc/share/tmp
	(cd $(M5DIR)/doc/share/tmp/; \
	makeinfo --force --html manual.texi --css-include=manual.css \
	)
	cp -r $(M5DIR)/doc/share/tmp/MonetDB $(M5DIR)/doc/share
	echo "Web version created"

web2:
	mkdir -p $(M5DIR)/doc/share/tmp
	mkbdy $(M5DIR) $(M4DIR); cp manual.texi $(M5DIR)/doc/share/tmp
	cp manual.css $(M5DIR)/doc/share/tmp
	cp -r $(M5DIR)/doc/Assets $(M5DIR)/doc/share/tmp
	(cd $(M5DIR)/doc/share/tmp/; \
	texi2html --split=node  --css-include=manual.css manual.texi; )
	cp -r $(M5DIR)/doc/share/tmp/manual $(M5DIR)/doc/share/MonetDB2
	echo "Web created"

xml:
	mkdir -p $(M5DIR)/doc/share/tmp
	mkbdy $(M5DIR) $(M4DIR); cp manual.texi $(M5DIR)/doc/share/tmp
	cp -r $(M5DIR)/doc/Assets $(M5DIR)/doc/share/tmp
	(cd $(M5DIR)/doc/share/tmp/; \
	makeinfo --force --xml manual.texi \
	)
	sed -i -e "s/&bullet;/<bullet\/>/g" \
		-e "s/&ndash;/<ndash\/>/g" \
		-e "s/&mdash;/<mdash\/>/g" \
		-e "s/&dots;/<dots\/>/g" \
		-e "s/&eosperiod;/<eosperiod\/>/g" \
		-e "s/&space;/<space\/>/g" \
			$(M5DIR)/doc/share/tmp/MonetDB.xml
	cp $(M5DIR)/doc/share/tmp/MonetDB.xml $(M5DIR)/doc/share
	echo "XML file created"

gdkpdf: 
	mkdir -p $(M5DIR)/doc/share/tmp
	mkbdy $(M5DIR) $(M4DIR); cp gdkmanual.texi $(M5DIR)/doc/share/tmp
	cp -r $(M5DIR)/doc/Assets $(M5DIR)/doc/share/tmp
	(cd $(M5DIR)/doc/share/tmp/; \
	pdftex gdkmanual.texi \
	)
	cp $(M5DIR)/doc/share/tmp/gdkmanual.pdf $(M5DIR)/doc/share
	(cd $(M5DIR)/doc/share/doc; pdfnup gdkmanual.pdf)
	echo "GDK manual created" 

modpdf: 
	mkdir -p $(M5DIR)/doc/share/tmp
	mkbdy $(M5DIR) $(M4DIR); cp modulesmanual.texi $(M5DIR)/doc/share/tmp
	cp -r $(M5DIR)/doc/Assets $(M5DIR)/doc/share/tmp
	(cd $(M5DIR)/doc/share/tmp/; \
	pdftex modulesmanual.texi \
	makeinfo --force --html modulesmanual.texi \
	)
	cp $(M5DIR)/doc/share/tmp/modulesmanual.pdf $(M5DIR)/doc/share
	(cd $(M5DIR)/doc/share; pdfnup modules.pdf)
	echo "Modules manual created" 

clean:
	rm -rf $(M5DIR)/doc/share/tmp
