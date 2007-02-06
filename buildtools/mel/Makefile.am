# automake 1.10 complains about GNU-isms, ignore them for now, chosing
# the lazy approach from
# http://www.nabble.com/warning-about-GNU-make-extension-with-automake-1.10-t2934239.html
# Disable as long as we deal with automakes from the stone-age on e.g.
# Darwin
#AUTOMAKE_OPTIONS = -Wno-portability 

MX = @MX@
bin_PROGRAMS = mel
nodist_mel_SOURCES = any_arg.cc any_arg.h atom_arg.cc atom_arg.h      \
		     atom.cc atom.h atomops.cc atomops.h bat_arg.cc   \
		     bat_arg.h builtin.cc builtin.h command.cc	      \
		     command.h debug.cc debug.h depend.cc	      \
		     dependency.cc dependency.h depend.h epilogue.cc  \
		     epilogue.h FileInfo.cc FileInfo.h glue.cc glue.h \
		     html.cc html.h iterator.cc iterator.h language.h \
		     link.cc link.h list.h ListIterator.cc	      \
		     ListIterator.h mel.cc mel.h mel.tab.cc mel.tab.h \
		     mel.yy.cc mil.cc mil.h module.cc module.h	      \
		     object.cc object.h operator.cc operator.h ops.cc \
		     ops.h prelude.cc prelude.h proto.cc proto.h      \
		     symbol.cc symbol.h symtable.cc symtable.h	      \
		     type_arg.cc type_arg.h use.cc use.h var_arg.cc   \
		     var_arg.h
dist_mel_SOURCES =

YFLAGS = -d

any_arg.cc: any_arg.mx
any_arg.h: any_arg.mx
any_arg.lo any_arg.o: any_arg.cc any_arg.h symbol.h list.h ListIterator.h type_arg.h language.h symtable.h module.h dependency.h atom.h ops.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h prelude.h epilogue.h
atom_arg.cc: atom_arg.mx
atom_arg.h: atom_arg.mx
atom_arg.lo atom_arg.o: atom_arg.cc atom_arg.h symbol.h list.h ListIterator.h atom.h ops.h type_arg.h language.h symtable.h module.h dependency.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
atom.cc: atom.mx
atom.h: atom.mx
atom.lo atom.o: atom.cc atom.h ops.h symbol.h list.h ListIterator.h type_arg.h language.h symtable.h module.h dependency.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
atomops.cc: atomops.mx
atomops.h: atomops.mx
atomops.lo atomops.o: atomops.cc atomops.h ops.h symbol.h list.h ListIterator.h language.h symtable.h module.h dependency.h atom.h type_arg.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
bat_arg.cc: bat_arg.mx
bat_arg.h: bat_arg.mx
bat_arg.lo bat_arg.o: bat_arg.cc bat_arg.h symbol.h list.h ListIterator.h type_arg.h language.h symtable.h module.h dependency.h atom.h ops.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h any_arg.h prelude.h epilogue.h
builtin.cc: builtin.mx
builtin.h: builtin.mx
builtin.lo builtin.o: builtin.cc builtin.h symbol.h list.h ListIterator.h language.h symtable.h module.h dependency.h atom.h ops.h type_arg.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
command.cc: command.mx
command.h: command.mx
command.lo command.o: command.cc command.h symbol.h list.h ListIterator.h mel.h symtable.h FileInfo.h ops.h atomops.h type_arg.h mel.tab.h language.h module.h dependency.h atom.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
debug.cc: debug.mx
debug.h: debug.mx
debug.lo debug.o: debug.cc debug.h language.h symbol.h list.h ListIterator.h symtable.h module.h dependency.h atom.h ops.h type_arg.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
depend.cc: depend.mx
dependency.cc: dependency.mx
dependency.h: dependency.mx
dependency.lo dependency.o: dependency.cc dependency.h symbol.h list.h ListIterator.h language.h symtable.h module.h atom.h ops.h type_arg.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
depend.h: depend.mx
depend.lo depend.o: depend.cc depend.h language.h symbol.h list.h ListIterator.h symtable.h module.h dependency.h atom.h ops.h type_arg.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
epilogue.cc: epilogue.mx
epilogue.h: epilogue.mx
epilogue.lo epilogue.o: epilogue.cc epilogue.h symbol.h list.h ListIterator.h language.h symtable.h module.h dependency.h atom.h ops.h type_arg.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h
FileInfo.cc: FileInfo.mx
FileInfo.h: FileInfo.mx
FileInfo.lo FileInfo.o: FileInfo.cc FileInfo.h list.h ListIterator.h language.h symbol.h symtable.h module.h dependency.h atom.h ops.h type_arg.h atomops.h command.h mel.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
glue.cc: glue.mx
glue.h: glue.mx
glue.lo glue.o: glue.cc glue.h language.h symbol.h list.h ListIterator.h symtable.h module.h dependency.h atom.h ops.h type_arg.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
html.cc: html.mx
html.h: html.mx
html.lo html.o: html.cc html.h language.h symbol.h list.h ListIterator.h symtable.h module.h dependency.h atom.h ops.h type_arg.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
iterator.cc: iterator.mx
iterator.h: iterator.mx
iterator.lo iterator.o: iterator.cc iterator.h command.h symbol.h list.h ListIterator.h mel.h symtable.h FileInfo.h ops.h atomops.h type_arg.h mel.tab.h language.h module.h dependency.h atom.h operator.h builtin.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
language.h: language.mx
link.cc: link.mx
link.h: link.mx
link.lo link.o: link.cc link.h language.h symbol.h list.h ListIterator.h symtable.h module.h dependency.h atom.h ops.h type_arg.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
list.h: list.mx
ListIterator.cc: ListIterator.mx
ListIterator.h: ListIterator.mx
ListIterator.lo ListIterator.o: ListIterator.cc ListIterator.h
mel.cc: mel.mx
mel.h: mel.mx
mel.ll: mel.mx
mel.lo mel.o: mel.cc mel.h symtable.h symbol.h list.h ListIterator.h FileInfo.h ops.h atomops.h type_arg.h mel.tab.h use.h language.h module.h dependency.h atom.h command.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h link.h depend.h proto.h html.h glue.h mil.h
mel.tab.cc: mel.yy mel.h symtable.h symbol.h list.h ListIterator.h FileInfo.h ops.h atomops.h type_arg.h module.h dependency.h atom.h command.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
mel.tab.h: mel.yy mel.h symtable.h symbol.h list.h ListIterator.h FileInfo.h ops.h atomops.h type_arg.h module.h dependency.h atom.h command.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
mel.tab.lo mel.tab.o: mel.tab.cc
mel.yy.cc: mel.ll mel.h symtable.h symbol.h list.h ListIterator.h FileInfo.h ops.h atomops.h type_arg.h mel.tab.h
mel.yy.lo mel.yy.o: mel.yy.cc
mel.yy: mel.mx
mil.cc: mil.mx
mil.h: mil.mx
mil.lo mil.o: mil.cc mil.h language.h symbol.h list.h ListIterator.h symtable.h module.h dependency.h atom.h ops.h type_arg.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
module.cc: module.mx
module.h: module.mx
module.lo module.o: module.cc module.h symbol.h list.h ListIterator.h language.h symtable.h dependency.h atom.h ops.h type_arg.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
object.cc: object.mx
object.h: object.mx
object.lo object.o: object.cc object.h symbol.h list.h ListIterator.h language.h symtable.h module.h dependency.h atom.h ops.h type_arg.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
operator.cc: operator.mx
operator.h: operator.mx
operator.lo operator.o: operator.cc operator.h command.h symbol.h list.h ListIterator.h mel.h symtable.h FileInfo.h ops.h atomops.h type_arg.h mel.tab.h language.h module.h dependency.h atom.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
ops.cc: ops.mx
ops.h: ops.mx
ops.lo ops.o: ops.cc ops.h symbol.h list.h ListIterator.h language.h symtable.h module.h dependency.h atom.h type_arg.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
prelude.cc: prelude.mx
prelude.h: prelude.mx
prelude.lo prelude.o: prelude.cc prelude.h symbol.h list.h ListIterator.h language.h symtable.h module.h dependency.h atom.h ops.h type_arg.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h epilogue.h
proto.cc: proto.mx
proto.h: proto.mx
proto.lo proto.o: proto.cc proto.h language.h symbol.h list.h ListIterator.h symtable.h module.h dependency.h atom.h ops.h type_arg.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
symbol.cc: symbol.mx
symbol.h: symbol.mx
symbol.lo symbol.o: symbol.cc symbol.h list.h ListIterator.h mel.h symtable.h FileInfo.h ops.h atomops.h type_arg.h language.h module.h dependency.h atom.h command.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
symtable.cc: symtable.mx
symtable.h: symtable.mx
symtable.lo symtable.o: symtable.cc symtable.h symbol.h list.h ListIterator.h
type_arg.cc: type_arg.mx
type_arg.h: type_arg.mx
type_arg.lo type_arg.o: type_arg.cc type_arg.h symbol.h list.h ListIterator.h language.h symtable.h module.h dependency.h atom.h ops.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
use.cc: use.mx
use.h: use.mx
use.lo use.o: use.cc use.h language.h symbol.h list.h ListIterator.h symtable.h module.h dependency.h atom.h ops.h type_arg.h atomops.h command.h mel.h FileInfo.h mel.tab.h operator.h builtin.h iterator.h object.h atom_arg.h var_arg.h bat_arg.h any_arg.h prelude.h epilogue.h
var_arg.cc: var_arg.mx
var_arg.h: var_arg.mx
var_arg.lo var_arg.o: var_arg.cc var_arg.h symbol.h list.h ListIterator.h type_arg.h iterator.h command.h mel.h symtable.h FileInfo.h ops.h atomops.h mel.tab.h language.h module.h dependency.h atom.h operator.h builtin.h object.h atom_arg.h bat_arg.h any_arg.h prelude.h epilogue.h

include rules.mk
