import os
import sys
import re

def mal_include(filename):
    if os.path.isdir(filename):
        files = os.listdir(filename)
        files.sort()
        ret = ""
        for f in files:
            if f.endswith(".mal"):
                ret += mal_include(os.path.join(filename, f))
        return ret
    elif os.path.isfile(filename):
        print filename
        content = open(filename).read()
        content = re.sub("^#.*$", "", content, flags=re.MULTILINE)
        while True:
            match = re.search("include (\\w+);", content)
            if (match == None): break
            modname = match.groups(0)[0]
            incfile = mal_include(modname + ".mal" if os.path.isfile(modname + ".mal") else modname)
#            if (modname == 'sql'):
#                incfile = "library sql;\n" + incfile
            content = content[:match.start()] + incfile + content[match.end():]
        return content
    else:
        return ""

wd = os.getcwd()

os.chdir(sys.argv[1])
s = mal_include("mal_init.mal")
os.chdir(wd)
open("monetdb5/mal/mal_init_inline.h", "w").write("char* mal_init_inline = \"\\x" + "\\x".join("{:02x}".format(ord(c)) for c in s) + "\\n\";\n")

os.chdir(sys.argv[1])
s = ""
files = os.listdir("createdb")
files.sort()
for f in files:
    if f.endswith(".sql"):
        print(f)
        s += open(os.path.join("createdb", f)).read() + "\n"
os.chdir(wd)
open("sql/backends/monet5/createdb_inline.h", "w").write("char* createdb_inline = \"\\x" + "\\x".join("{:02x}".format(ord(c)) for c in s) + "\\n\";\n")

