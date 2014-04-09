import os, sys, re
try:
    from MonetDBtesting import process
except ImportError:
    import process

TSTSRCBASE = os.environ['TSTSRCBASE']
TSTDIR = os.environ['TSTDIR']
SRCDIR = os.path.join(TSTSRCBASE,TSTDIR,"Tests")

def new_client():
    return process.client('sql', stdin = process.PIPE, stdout = process.PIPE, stderr = process.PIPE)

def set_var(c, name, value, numeric=True):
    if numeric:
        c.stdin.write('DECLARE %s integer;'% name)
        c.stdin.write('SET %s=%s;'%(name, value))
    else:
        c.stdin.write('DECLARE %s varchar(32);'% name)
        c.stdin.write('SET %s=\'%s\';'% (name, value))

# start with loading bam files into the database
c = new_client()
for ln in open(os.path.join(SRCDIR,"load.sql")):
    c.stdin.write(ln.replace('PWD', SRCDIR))

out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)


# now retrieve the file ids that have been inserted
# (should be 1,2,..,nr_files, but still we extract it from the
# db to be flexible to logical changes in the bam loader as
# much as possible)
c = new_client()
c.stdin.write("SELECT file_id, dbschema FROM bam.files;")
out, err = c.communicate()
p = re.compile('^\s*\[\s*(\d)\s*,\s*(\d)\s*\]\s*$', re.MULTILINE)
files_to_test = []
for match in p.finditer(out):
    files_to_test.append((int(match.group(1)), int(match.group(2))))

# Now we will execute all benchmark queries on all BAM files in the bam.files table
# and output all data contained in the aux table.
# Furthermore, we transfer every file to the export table, use sam_export to write
# the contents to a SAM file and then print the raw contents of this SAM file
c = new_client()
set_var(c, 'rname_1_3', 'chr22', numeric=False)
set_var(c, 'pos_1_3_1', 1000000)
set_var(c, 'pos_1_3_2', 20000000)
set_var(c, 'qname_1_4', 'sim_22_1_1', numeric=False)
set_var(c, 'mapq_1_5', 200)
set_var(c, 'mapq_2_1', 100)
set_var(c, 'rname_2_9', 'chr22', numeric=False)
set_var(c, 'pos_2_9', 39996433)
set_var(c, 'rname_2_10', 'chr22', numeric=False)
set_var(c, 'pos_2_10', 80000000)
set_var(c, 'distance_2_12', 10000000)

output_files = []
for f in files_to_test:
    # benchmark 1
    for uc in range(1, 6):
        for ln in open(os.path.join(SRCDIR, 'benchmarks_%s/query1.%d.sql'% (f[1], uc))):
            c.stdin.write(ln.replace('alignments_i', 'alignments_%d'% f[0]))

    #benchmark 2
    for uc in range(1, 13):
        for ln in open(os.path.join(SRCDIR, 'benchmarks_%s/query2.%d.sql'% (f[1], uc))):
            c.stdin.write(ln.replace('alignments_i', 'alignments_%d'% f[0])
                            .replace('alignments_j', 'alignments_%d'% f[0]))

    #write all aux data
    c.stdin.write("SELECT * FROM bam.alignments_extra_%d;"% f[0]);

    #load into export table
    c.stdin.write("INSERT INTO bam.export (SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual FROM bam.%salignments_%d);"\
        % (('unpaired_all_' if f[1] == 1 else ''), f[0]))
    output_files.append(os.path.join(SRCDIR, ("output_%d.sam"% f[0])))
    c.stdin.write("CALL sam_export('%s');"% output_files[-1])




out, err = c.communicate()

# The output will contain explicit file_ids in the table names, making the table names
# variable. We don't want a test to fail on this, so we have to remove te file
# ids from the table names.
def replace(matchobj):
    return 'alignments_%si'% (matchobj.group(1) if matchobj.group(1) else '')
p = re.compile('alignments_(extra_)?\d+')
out = (re.subn(p, replace, out))[0]

sys.stdout.write(out)
sys.stderr.write(err)


# All that is left to do is write the contents of the exported SAM files and delete them
for f in output_files:
    sys.stdout.write("\n\nContents of exported SAM file '%s':\n"% f)
    for ln in open(f):
        sys.stdout.write(ln)
    os.remove(f)