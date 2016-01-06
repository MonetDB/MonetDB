import os
import sys
import re

def mal_include(filename):
    if os.path.isdir(filename):
        files = os.listdir(filename)
        ret = ""
        for f in files:
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
            if (modname == 'sql'):
                incfile = "library sql;\n" + incfile
            content = content[:match.start()] + incfile + content[match.end():]
        return content
    else:
        return ""

wd = os.getcwd()
os.chdir(sys.argv[1])
s = mal_include("mal_init.mal")
os.chdir(wd)
open(sys.argv[2], "w").write("char* mal_init_inline = \"\\x" + "\\x".join("{:02x}".format(ord(c)) for c in s) + "\\n\";\n")
