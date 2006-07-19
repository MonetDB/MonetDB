#This Makefile can be used to generate the documentation
#for off-line printing and creates a webstructure for
#inclusion in the MonetDB5 website

# FIXME / WARNING: this is not "portable" and probably not the best
#                  solution either.  Actually, it will not work.
#M5DIR=/ufs/mk/monet5/
#M4DIR=/ufs/mk/opensource/MonetDB
MANUAL=manual

all:
	mkdir -p $(M5DIR)/doc/share/doc
	make pdf
	make web
	make xml
	make clean

pdfs: 
	make MANUAL=manual pdf
	make MANUAL=sqlmanual pdf
	make MANUAL=m5server pdf
pdf: 
	mkdir -p $(M5DIR)/doc/share/tmp
	mkbdy $(M5DIR) $(M4DIR); cp $(MANUAL).texi $(M5DIR)/doc/share/tmp
	cp  -r $(M5DIR)/doc/Assets/ $(M5DIR)/doc/share/tmp
	(cd $(M5DIR)/doc/share/tmp/; pdftex $(MANUAL).texi; pdftex $(MANUAL).texi )
	cp $(M5DIR)/doc/share/tmp/$(MANUAL).pdf $(M5DIR)/doc/share
	(cd $(M5DIR)/doc/share/; pdfnup $(MANUAL).pdf)
	echo "Manual created" 

webs: 
	make MANUAL=manual web
	make MANUAL=sqlmanual web
	make MANUAL=m5server web
web:
	echo "Create tmp:" $(M5DIR)/doc/share/tmp/$(MANUAL)
	echo "using:" $(M4DIR)
	mkdir -p $(M5DIR)/doc/share/tmp/$(MANUAL)
	mkbdy $(M5DIR) $(M4DIR)
	cp $(MANUAL).texi $(M5DIR)/doc/share/tmp/$(MANUAL)
	cp manual.css $(M5DIR)/doc/share/tmp/$(MANUAL)
	cp  -r $(M5DIR)/doc/Assets/ $(M5DIR)/doc/share/tmp/$(MANUAL)
	(cd $(M5DIR)/doc/share/tmp/$(MANUAL)/; \
	makeinfo --force --html $(MANUAL).texi --css-include=manual.css \
	)
	echo "Web version created"

web2:
	mkdir -p $(M5DIR)/doc/share/tmp
	mkbdy $(M5DIR) $(M4DIR); cp $(MANUAL).texi $(M5DIR)/doc/share/tmp
	cp manual.css $(M5DIR)/doc/share/tmp
	cp -r $(M5DIR)/doc/Assets $(M5DIR)/doc/share/tmp
	(cd $(M5DIR)/doc/share/tmp/; \
	texi2html --split=node  --css-include=manual.css $(MANUAL).texi; )
	cp -r $(M5DIR)/doc/share/tmp/manual $(M5DIR)/doc/share/MonetDB2
	echo "Web created"

xml:
	mkdir -p $(M5DIR)/doc/share/tmp
	mkbdy $(M5DIR) $(M4DIR); cp $(MANUAL).texi $(M5DIR)/doc/share/tmp
	cp -r $(M5DIR)/doc/Assets $(M5DIR)/doc/share/tmp
	(cd $(M5DIR)/doc/share/tmp/; \
	makeinfo --force --xml $(MANUAL).texi \
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
	mkbdy $(M5DIR) $(M4DIR); cp gdk$(MANUAL).texi $(M5DIR)/doc/share/tmp
	cp -r $(M5DIR)/doc/Assets $(M5DIR)/doc/share/tmp
	(cd $(M5DIR)/doc/share/tmp/; \
	pdftex gdk$(MANUAL).texi \
	)
	cp $(M5DIR)/doc/share/tmp/gdkmanual.pdf $(M5DIR)/doc/share
	(cd $(M5DIR)/doc/share/doc; pdfnup gdkmanual.pdf)
	echo "GDK manual created" 

modpdf: 
	mkdir -p $(M5DIR)/doc/share/tmp
	mkbdy $(M5DIR) $(M4DIR); cp modules$(MANUAL).texi $(M5DIR)/doc/share/tmp
	cp -r $(M5DIR)/doc/Assets $(M5DIR)/doc/share/tmp
	(cd $(M5DIR)/doc/share/tmp/; \
	pdftex modules$(MANUAL).texi \
	makeinfo --force --html modules$(MANUAL).texi \
	)
	cp $(M5DIR)/doc/share/tmp/modulesmanual.pdf $(M5DIR)/doc/share
	(cd $(M5DIR)/doc/share; pdfnup modules.pdf)
	echo "Modules manual created" 

clean:
	rm -rf $(M5DIR)/doc/share/tmp
