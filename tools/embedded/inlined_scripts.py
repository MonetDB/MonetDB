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
        content = open(filename).read() + "\n"
        content = re.sub("^#.*$", "", content, flags=re.MULTILINE)
        while True:
            match = re.search("include (\\w+);", content)
            if (match == None): break
            modname = match.groups(0)[0]
            incfile = mal_include(modname + ".mal" if os.path.isfile(modname + ".mal") else modname)
            content = content[:match.start()] + incfile + content[match.end():]
        return content
    else:
        return ""

def to_hex(s, n=1024):
    result = ""
    for chunk in [s[i:i+n] for i in range(0, len(s), n)]:
        result += "\\x" + "\\x".join("{:02x}".format(ord(c)) for c in chunk) +  "\\\n"
    return "\\\n" + result

wd = os.getcwd()

os.chdir(sys.argv[1])
s = mal_include("mal_init.mal")
os.chdir(wd)
outf = open("tools/embedded/inlined_scripts.c", "w")
outf.write("char* mal_init_inline = \"" + to_hex(s) + "\";\n")

os.chdir(sys.argv[1])
s = ""
files = os.listdir("createdb")
files.sort()
for f in files:
    if f.endswith(".sql"):
        print(f)
        s += open(os.path.join("createdb", f)).read() + "\n"
os.chdir(wd)
outf.write("\nchar* createdb_inline = \"" + to_hex(s) + "\";\n")

