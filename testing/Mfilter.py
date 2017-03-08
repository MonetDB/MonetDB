# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.

import sys
import os
import re

#############################################################################
#       FUNCTIONS

def Usage(THISFILE) :
    print("""

Usage:  %s [-I<exp>] <files>

-I<exp> : ignore lines matching <exp> during first count (optional, default: -I'^#`)
<files> : list of files to be processed

""" % THISFILE)
### Usage(THISFILE) #

def warn(THISFILE,TEXT) :
    sys.stderr.write("%s warning: %s\n" % (THISFILE,TEXT))
### warn(THISFILE,TEXT) #

def wlen(str) :
    return len(' '.join(str.split()))
### wlen(str) #

test = (
        # potential differences, which we want to ignore
        re.compile('(?:'+')|(?:'.join([
                # MAPI port numbers
                    r"^MAPI  = (.*@.*:\d*|\([a-zA-Z0-9_]+\) /.*\.s\.monetdb\.\d+)$",
                # dplyr (R package) includes the MonetDB version in its output, ignore
                    r"^Source: MonetDB .*$",
                 ])+')',  re.MULTILINE),
        # differences (e.g., due to property-checking), which we want to treat as "minor differences"
        re.compile('(?:'+')|(?:'.join([
                # varying variable names in dataflow barriers
                    r'^barrier X_\d+ := language.dataflow\(\);$',
                    r'^exit X_\d+;$',
                # varying width of table frames
                    r'^\+[=-]+\+$',
                # varying error message
                    r"^ERROR = !conversion of string '.*' to type [^ ]* failed\.$",
                # table_function_with_column_subselects.Bug-3172 & create_function.Bug-3172:
                # id in error message depends on #threads
                    r"^.*!TypeException:user.s2_1\[[0-9]+\]:'.*$",
                 ])+')',  re.MULTILINE),
        # warnings and messages that should be treated as errors:
        re.compile('(?:'+')|(?:'.join([
                    r'^#BATpropcheck: .*$',
                 ])+')',  re.MULTILINE)
       )

# differences in BBP.dir entries
# 16 BAT fields, 12 column fields (head and tail), optionally: 3 var
# heap fields (head and tail)
# we want to ignore differences in groups 5, 16+8, 16+12+8 (lastused,
# halign, talign)
# the regular expression (-?\d+) is for numeric fields, the regular
# expression ([^ ]+) is for string fields

bbp_dir = re.compile(r'^(-?\d+) (-?\d+) ([^ ]+) ([^ ]+) ([^ ]+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+)'
                     r' ([^ ]+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+)'
                     r' ([^ ]+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+) (-?\d+)'
                     r'( (-?\d+) (-?\d+) (-?\d+))?'
                     r'( (-?\d+) (-?\d+) (-?\d+))?$')

# "normalize" differences. e.g., in error messages caused by flex/bison:
# matched groups from "norm_in" are replaced by the respective
# element of "norm_out", unless the latter is "None" (then, the
# respective match is kept as-is).
norm_in  = re.compile('(?:'+')|(?:'.join([
                                                                                                                                                # id: #groups
### r'^(ERROR = !| *!|)(syntax|parse|parse error: syntax)( error, )(unexpected .* on line |unexpected .* in: )?(.*)\n',                         # 1: 5
    r'^(ERROR = !| *!|)(syntax|parse|parse error: syntax)( error, )(?:unexpected .* on line |unexpected .* in: )?(?:.*)\n',                     # 2: 5
    r"^(QUERY|ERROR)( =.* connect)( to|)( ')(localhost)(' port )(\d+)( .*)\n",                                                                  # 3: 8
    r"^([Uu]sage: )(/.*/\.libs/|/.*/lt-|)([A-Za-z0-9_]+:?[ \t].*)\n",                                                                           # 4: 3
    r'^(ERROR = !.*Exception:remote\.[^:]*:\(mapi:monetdb://monetdb@)([^/]*)(/mTests_.*\).*)\n',                                                # 5: 4
    r"^(DBD::monetdb::db table_info warning: Catalog parameter c has to be an empty string, as MonetDB does not support multiple catalogs at )([\./].+/|[A-Z]:\\.+[/\\])([^/\\]+\.pl line \d+\.)\n",            # 6: 3
    r'^(ERROR REPORTED: DBD:|SyntaxException:parseError)(:.+ at )([\./].+/|[A-Z]:\\.+[/\\])([^/\\]+\.pm line \d+\.)\n',                         # 7: 4
# filter for geos 3.3 vs. geos 3.2, can be removed if we have 3.3 everywhere
    r"^(ERROR = !ParseException: Expected )('EMPTY' or '\(')( but encountered : '\)')\n",                                                       # 8: 3
# filter for AVG_of_SQRT.SF-2757642: result not always exactly 1.1
    r'^(\[ "avg\(sqrt\(n8\)\) == 1\.1",\s+)(1\.09999\d*|1\.10000\d*)(\s+\])\n',                                                                 # 9: 3
    # POLYGONs can be traversed in multiple directions
    r'^(\[.*POLYGON.*\(59\.0{16} 18\.0{16}, )(59\.0{16} 13\.0{16})(, 67\.0{16} 13\.0{16}, )(67\.0{16} 18\.0{16})(, 59\.0{16} 18\.0{16}\).*)',   # 10: 5
    # test geom/BugTracker/Tests/X_crash.SF-1971632.* might produce different error messages, depending on evaluation order
    r'^(ERROR = !MALException:geom.wkbGetCoordinate:Geometry ")(.*)(" not a Point)\n',                                                          # 11: 3
    r"^(QUERY = COPY BINARY INTO)( .*);\n",                     # 12: 3
])+')',  re.MULTILINE)
norm_hint = '# the original non-normalized output was: '
norm_out = (
### None, 'syntax/parse', None, 'unexpected ... on line/in: ', None,                                    # 1: 5
    None, 'syntax/parse', None,                                                                         # 2: 5
    None, None, None, None, '<HOST>', None, '<MAPIPORT>', None,                                         # 3: 8
    None, '', None,                                                                                     # 4: 3
    None, 'localhost', None,                                                                            # 5: 4
    None, '...', None,                                                                                  # 6: 3
    None, None, '...', None,                                                                            # 7: 4
    None, "'Z', 'M', 'ZM', 'EMPTY' or '('", None,                                                       # 8: 3
    None, '1.1', None,                                                                                  # 9: 3
    None, '67.0000000000000000 18.0000000000000000', None, '59.0000000000000000 13.0000000000000000', None, # 10: 5
    None, '...', None,                                                                                  # 11: 3
    None, '...', None,                                                                                  # 12: 3
)

# match "table_name" SQL table header line to normalize "(sys)?.L[0-9]*" to "(sys)?."
table_name = re.compile(r'^%.*[\t ](|sys)\.L[0-9]*[, ].*# table_name$')
name = re.compile(r'^%.*[\t ]L[0-9]*[, ].*# name$')

attrre = re.compile(r'\b[-:a-zA-Z_0-9]+\s*=\s*(?:\'[^\']*\'|"[^"]*")')
elemre = re.compile(r'<[-:a-zA-Z_0-9]+(?P<attrs>(\s+' + attrre.pattern + r')+)\s*/?>')
# we're only interested in elements with attributes, hence the +^

def mFilter (FILE, IGNORE) :
    # translate a pattern suitable for diff to one suitable for re
    # this is pretty simple-minded and does not do everything that
    # might be needed
    ign = []
    i = 0
    while i < len(IGNORE):
        if IGNORE[i] == '\\':
            i += 1
            if IGNORE[i] not in '()|':
                ign.append('\\')
        elif IGNORE[i] in '()|':
            ign.append('\\')
        ign.append(IGNORE[i])
        i += 1
    IGNORE = ''.join(ign)

    fin = open(FILE, "rU")
    LINE = fin.readline()
    while  len(LINE)  and  ( len(LINE) < 15  or  LINE[:15] not in ("stdout of test ", "stderr of test ") ):
        LINE = fin.readline()
    fin.close()
    if  len(LINE) >= 15  and  LINE[:15] in ("stdout of test ", "stderr of test "):
        WHAT, TST, TSTDIR = re.search("^std(out|err) of test .(.*). in directory .(.*). itself:", LINE, re.MULTILINE).groups()
    else:
        WHAT, TST, TSTDIR = "", "", ""


    ftmp = []
    ig = n = 0
    il = iw = ic = el = ew = ec = al = aw = ac = 0
    for iline in open(FILE, 'rU'):
        iline = iline.replace('\033[?1034h','')
        if iline.startswith('# builtin opt') or \
           iline.startswith('# cmdline opt') or \
           iline.startswith('# config opt'):
            continue
        # normalize exponents in floating point representation: remove
        # leading zeros from exponent (but keeping at least one digit,
        # even if zero)
        iline = re.sub(r'(\d+(?:\.\d*)?e[-+]?)0*(\d+)', r'\1\2', iline)
        oline = xline = ""
        if iline == "#~BeginVariableOutput~#\n"  or  iline == "#~BeginProfilingOutput~#\n" or iline == "[ \"~BeginVariableOutput~\"\t]\n"  or  iline == "[ \"~BeginProfilingOutput~\"\t]\n":
            ig = 1
            n = 0
        if ig  and  ( len(iline) == 0  or  iline[0] != "!"  or  iline[:9] != "ERROR = !" ):
            # ignore differences in "VariableOutput" or "ProfilingOutput"
            oline = "#~ " + iline
            n = n + 1
        elif test[2].match(iline):
            # warnings and messages that should be treated as errors:
            oline = "!~" + iline
        elif test[0].match(iline):
            # potential differences, which we want to ignore; see above
            oline = "#~ " + iline
        elif test[1].match(iline):
            # differences (e.g., due to property-checking), which we want to treat as "minor differences"; see above
            oline = "=" + iline
        elif bbp_dir.match(iline):
            l = iline.split(' ')
            l[5] = '<lastused>'
            l[16+8] = '<halign>'
            l[16+12+8] = '<talign>'
            oline = ' '.join(l)
            xline = norm_hint + iline
        elif norm_in.match(iline):
            # "normalize" differences in error messages caused by flex/bison; see above
            grps_in = norm_in.match(iline).groups()
            oline = ''
            i = 0
            while i < len(grps_in):
                if grps_in[i] is not None:
                    if norm_out[i] is None:
                        oline += grps_in[i]
                    else:
                        oline += norm_out[i]
                i+=1
            oline += '\n'
            xline = norm_hint + iline
        elif table_name.match(iline):
            # normalize "(sys)?.L[0-9]*" to "(sys)?." in "table_name" line of SQL table header
            oline = re.sub(r'([ \t])(|sys)(\.)L[0-9]*([, ])', r'\1\2\3\4', iline)
            # keep original line for reference as comment (i.e., ignore diffs, if any)
            xline = iline.replace('%','#',1)
        elif name.match(iline):
            # normalize "L[0-9]*" to "L" in "name" line of SQL table header
            oline = re.sub(r'([ \t])L[0-9]*([, ])', r'\1L\2', iline)
            # keep original line for reference as comment (i.e., ignore diffs, if any)
            xline = iline.replace('%','#',1)
        else:
            oline = iline
        if iline == "#~EndVariableOutput~#\n" or iline == "[ \"~EndVariableOutput~\"\t]\n":
            ig = 0
            xline = "~ " + str(n) + " ~\n"
        if iline == "#~EndProfilingOutput~#\n" or iline == "[ \"~EndProfilingOutput~\"\t]\n":
            ig = 0
        for ln in oline, xline:
            if len(ln):
                w = len(ln.split())
                c = wlen(ln)
                al = al + 1
                aw = aw + w
                ac = ac + c
                if ln != '\n' and not re.match(IGNORE,ln, re.MULTILINE):
                    el = el + 1
                    ew = ew + w
                    ec = ec + c
                    if not re.match('^=',ln, re.MULTILINE):
                        il = il + 1
                        iw = iw + w
                        ic = ic + c
                ftmp.append(ln)

    fout = open(FILE + ".FILTERED", "w")
    if (al > 0) or (aw > 0) or (ac >0):
        fout.write("Statistics about std"+WHAT+" of test '"+TST+"` in directory '"+TSTDIR+"`:\n")
        fout.write("  %9d lines, %9d words, %9d chars   in lines not matching '^$|%s|^=`\n" % (il,iw,ic,IGNORE))
        fout.write("= %9d lines, %9d words, %9d chars   in lines not matching '^$|%s`\n"    % (el,ew,ec,IGNORE))
        fout.write("# %9d lines, %9d words, %9d chars   in all lines\n"                     % (al,aw,ac))
        fout.write("\n")
        treatxml = False
        for ln in ftmp:
            if not treatxml and '<?xml' in ln:
                treatxml = True
            if treatxml and (ln[:1] != '#' or ln[:3] == '#~ '):
                # Add a newline after each > and before each <, but
                # only add a single one between >< and don't add an
                # extra one when the line starts with < or ends with >.
                # We also recognize some line prefixes and repeat them
                # after each added newline.
                for pref in ['#~ ', '!~', '=']:
                    if ln[:len(pref)] == pref:
                        break
                else:
                    pref = ''
                preflen = len(pref)
                pos = preflen
                res = elemre.search(ln, pos)
                while res is not None:
                    attrsold = res.group('attrs')
                    attrsnew = ' ' + ' '.join(sorted(attrre.findall(attrsold)))
                    ln = ln[:res.start('attrs')] + attrsnew + ln[res.end('attrs'):]
                    pos = res.end(0) + len(attrsnew) - len(attrsold)
                    res = elemre.search(ln, pos)
                ln = ln.replace('>', '>&\n').replace('<','&\n<').replace('>&\n&\n<', '>&\n<')
                if ln[:2 + preflen] == pref + '&\n':
                    ln = pref + ln[2 + preflen:]
                if ln[-3:] == '&\n\n':
                    ln = ln[:-3] + '\n'
                if pref:
                    ln = ln[:-1].replace('\n', '\n' + pref) + '\n'
            try:
                fout.write(ln.expandtabs())
            except IOError:
                IOerrNo = sys.exc_info()[1].errno
                IOerrStr = sys.exc_info()[1].strerror
                warn(THISFILE, "Writing to output file '%s' failed with #%d: '%s'." % (fout.name, IOerrNo, IOerrStr))
                if IOerrNo == 28:
                    # No space left on device
                    warn(THISFILE, "Removing input file '%s'." % FILE)
                    try:
                        os.remove(FILE)
                        fin = open(FILE,"w")
                        fin.write("%s: Removed '%s' to create space for '%s'.\n" % (THISFILE, FILE, fout.name))
                        fin.close()
                    except:
                        pass
                    try:
                        fout.write(ln.expandtabs())
                    except IOError:
                        IOerrNo = sys.exc_info()[1].errno
                        IOerrStr = sys.exc_info()[1].strerror
                        warn(THISFILE, "Writing to output file '%s' failed with #%d: '%s'." % (fout.name, IOerrNo, IOerrStr))
        fout.flush()
    fout.close()
### mFilter (FILE, IGNORE) #

#############################################################################
#       MAIN

def main(argv) :
    import getopt
    THISFILE = os.path.basename(argv[0])
    try:
        opts, args = getopt.getopt(argv[1:], "?hI:", ["help"])
    except getopt.GetoptError:
        Usage(THISFILE)
        sys.exit(1)

    IGNORE = "^#"
    for o, a in opts:
        if o in ("-?", "-h", "--help"):
            Usage(THISFILE)
            sys.exit(0)
        if o == "-I":
            IGNORE = a

    for f in args:
        if os.path.isfile(f):
            mFilter(f, IGNORE)
        else:
            warn(THISFILE, "file missing: " + f)
### main(argv) #

if __name__ == "__main__":
    main(sys.argv)

#       END
#############################################################################
