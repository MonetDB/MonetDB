import os
import sys
import re

# these includes are skipped
blacklist = ('remote', 'srvpool', 'mal_mapi', 'json', 'json_util', 'mcurl', 'xml', 'batxml', 'recycle', 'txtsim', 'tokenizer', 'zorder', '70_vault', '80_lsst', '80_udf', '23_skyserver', '24_zorder', '40_json', '80_udf')

def mal_include(filename):
    if os.path.isdir(filename):
        files = os.listdir(filename)
        files.sort()
        ret = ""
        for f in files:
            if f.endswith(".mal") and not f.startswith(blacklist):
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
            if not modname.startswith(blacklist):
                incfile = mal_include(modname + ".mal" if os.path.isfile(modname + ".mal") else modname)
            content = content[:match.start()] + incfile + content[match.end():]
        return content
    else:
        return ""

def to_hex(s, n=1024):
    result = "{"
    for chunk in [s[i:i+n] for i in range(0, len(s), n)]:
        result += ",".join(str(ord(c)) for c in chunk) +  ",\n"
    return "\n" + result + "0}"

wd = os.getcwd()

os.chdir(sys.argv[1])
s = mal_include("mal_init.mal")
os.chdir(wd)
outf = open("tools/embedded/inlined_scripts.c", "w")
outf.write("char mal_init_inline_arr[] = " + to_hex(s) + ";\n")
outf.write("char* mal_init_inline = mal_init_inline_arr;\n")

os.chdir(sys.argv[1])
s = ""
files = os.listdir("createdb")
files.sort()
for f in files:
    if f.endswith(".sql") and not f.startswith(blacklist):
        print(f)
        s += open(os.path.join("createdb", f)).read() + "\n"
os.chdir(wd)
outf.write("\nchar createdb_inline_arr[] = " + to_hex(s) + ";\n")
outf.write("char* createdb_inline = createdb_inline_arr;\n")

