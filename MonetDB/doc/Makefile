###########################################################################
# documentation makefile

###########################################################################
# determine os name and revision, overwrite if necessary
#ifndef PREFIX
#UNAME_BASE=$(shell uname)
#UNAME_REV=$(shell uname -r)
#PREFIX=$(UNAME_BASE)$(UNAME_REV)
#endif

TARGETS=YES
INCLUDE_FILES=OFF

###########################################################################
# compilation parameters are retrieved from the top-level makefile:
#include $(MONETHOME)/makefiles/$(PREFIX).mk
include $(MONETHOME)/Makefile

###########################################################################
# these modules form ths standard documentation collection

DOC=		$(patsubst %.mx, %, $(wildcard *.mx))
MANSRC=	Mcreatedb.mx Mserver.mx Mclient.mx Mshutdown.mx Mdump.mx \
	Mload.mx Minspect.mx Mrecover.mx Mapi.mx Mcgi.mx
MAN=$(MANSRC:%.mx=%) mel WWWidx WWWhtml WWWlink odlp

STD=		$(patsubst plain/%,%,$(KERNEL_MODULES))
EXT=		$(filter-out $(STD), \
			$(patsubst $(MONETHOME)/modules/plain/%.mx, %, \
				$(wildcard $(MONETHOME)/modules/plain/*.mx)))
ODMG=		$(patsubst $(MONETHOME)/modules/odmg/%.mx, %, \
			$(wildcard $(MONETHOME)/modules/odmg/*.mx))
CC_ODMG=	$(patsubst $(MONETHOME)/modules/CC_odmg/%.mx, %, \
			$(wildcard $(MONETHOME)/modules/CC_odmg/*.mx))
JODMG=		$(patsubst $(MONETHOME)/modules/jOdmg/odmg/%.java, %, \
			$(wildcard $(MONETHOME)/modules/jOdmg/odmg/*.java))

###########################################################################
# where is the big source code web? You might generate it locally.. 
#WWWROOT=	http:\/\/www\.cwi\.nl\/\~monet\/www
WWWROOT=	..\/scw
#WWWROOT=	http:\/\/vlieland\.cwi\.nl\:8080\/\~manegold\/Monet\/Current\/monet\/www

###########################################################################
# where should the documentation go 
WWWDOC=$(MONETHOME)/www/doc

all:	kernel modules odmg

kernel:	$(DOC:%=$(WWWDOC)/%.html)\
	$(MAN:%=$(WWWDOC)/man/%.html)

modules:$(EXT:%=$(WWWDOC)/extlib/%.html)\
	$(STD:%=$(WWWDOC)/stdlib/%.html)

odmg:	$(ODMG:%=$(WWWDOC)/odmg/%.html)\
	$(CC_ODMG:%=$(WWWDOC)/CC_odmg/%.html)\
	$(WWWDOC)/jOdmg/Package-odmg.html

$(WWWDOC)/stdlib/%.html: $(WWWDOC)/stdlib/
	(cd $(MONETHOME)/modules/plain;\
		$(MX) -R$(WWWDOC)/stdlib -H1 -w $*.mx) 

$(WWWDOC)/extlib/%.html: $(WWWDOC)/extlib/
	(cd $(MONETHOME)/modules/plain;\
		$(MX) -R$(WWWDOC)/extlib -H1 -w $*.mx)

$(WWWDOC)/man/mel.html: $(MONETHOME)/src/mel/mel.mx $(WWWDOC)/man/
	(cd $(MONETHOME)/src/mel;\
		$(MX) -R$(WWWDOC)/man -H1 -w mel.mx) 

$(WWWDOC)/man/WWW%.html: $(MONETHOME)/src/utils/www/WWW%.mx $(WWWDOC)/man/
	(cd $(MONETHOME)/src/utils/www;\
		$(MX) -R$(WWWDOC)/man -H1 -w WWW$*.mx) 

$(WWWDOC)/man/odlp.html: $(MONETHOME)/src/odlparser/odlp.mx $(WWWDOC)/man/
	(cd $(MONETHOME)/src/odlparser;\
		$(MX) -R$(WWWDOC)/man -H1 -w odlp.mx)

$(WWWDOC)/man/%.html: $(MONETHOME)/src/tools/%.mx $(WWWDOC)/man/
	(cd $(MONETHOME)/src/tools;\
		$(MX) -R$(WWWDOC)/man -H1 -w $*.mx)

$(WWWDOC)/jOdmg/Package-odmg.html: $(JODMG:%=$(MONETHOME)/modules/jOdmg/odmg/%.java) $(WWWDOC)/jOdmg/
	(cd $(MONETHOME)/modules/jOdmg;\
	 if javadoc -d $(WWWDOC)/jOdmg -sourcepath .:$(CLASSPATH) odmg;\
	        then echo "jOdmg documentation generated";\
	        else echo "javadoc doesn't work";\
	 fi;\
	 cd $(WWWDOC)/jOdmg;\
	 cp $(MONETHOME)/doc/JavaImages.tar.Z .;\
	 uncompress JavaImages.tar.Z;\
	 tar xvf JavaImages.tar;\
	 rm JavaImages.tar)

$(WWWDOC)/odmg/%.html: $(MONETHOME)/modules/odmg/%.mx $(WWWDOC)/odmg/
	(cd $(MONETHOME)/modules/odmg;\
		$(MX) -R$(WWWDOC)/odmg -H1 -w $*.mx)

$(WWWDOC)/CC_odmg/%.html: $(MONETHOME)/modules/CC_odmg/%.mx $(WWWDOC)/CC_odmg/
	(cd $(MONETHOME)/modules/CC_odmg;\
		$(MX) -R$(WWWDOC)/CC_odmg -H1 -w $*.mx)

$(WWWDOC)/%.html: %.mx $(WWWDOC)/	
	sed "s/WWWROOT/$(WWWROOT)/" $*.mx > $(WWWDOC)/$*.mx
	$(MX) -H1 -R$(WWWDOC) -w $(WWWDOC)/$*.mx 

###########################################################################
# clauses to make directories if they do not exist
$(WWWDOC)/:	
			-mkdir -p $(WWWDOC)
$(WWWDOC)/man/: $(WWWDOC)/
			-mkdir -p $(WWWDOC)/man
$(WWWDOC)/stdlib/: $(WWWDOC)/
			-mkdir -p $(WWWDOC)/stdlib
$(WWWDOC)/extlib/: $(WWWDOC)/
			-mkdir -p $(WWWDOC)/extlib
$(WWWDOC)/jOdmg/: $(WWWDOC)/
			-mkdir -p $(WWWDOC)/jOdmg
$(WWWDOC)/odmg/: $(WWWDOC)/
			-mkdir -p $(WWWDOC)/odmg
$(WWWDOC)/CC_odmg/: $(WWWDOC)/
			-mkdir -p $(WWWDOC)/CC_odmg

test2:
	echo $(MX)
