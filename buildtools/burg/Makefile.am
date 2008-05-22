bin_PROGRAMS = burg
burg_SOURCES = b.h be.c burs.c closure.c delta.c fe.c fe.h gram.y item.c \
	       lex.c list.c main.c map.c nonterminal.c operator.c pattern.c \
	       plank.c queue.c rule.c string.c symtab.c table.c trim.c zalloc.c

AM_YFLAGS = -d

gram.c: gram.y
	$(LOCKFILE) waiting
	$(YACC) $(YFLAGS) $< || { $(RM) waiting ; exit 1 ; }
	if [ -f y.tab.c ]; then $(MV) y.tab.c gram.c ; fi
	$(RM) waiting

gram.h: gram.y
	$(LOCKFILE) waiting
	$(YACC) $(YFLAGS) -d $< || { $(RM) waiting ; exit 1 ; }
	if [ -f y.tab.h ]; then $(MV) y.tab.h gram.h ; fi
	$(RM) waiting

lex.o: gram.h
