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
        c.stdin.write('DECLARE %s integer;\n'% name)
        c.stdin.write('SET %s=%s;\n'%(name, value))
    else:
        c.stdin.write('DECLARE %s varchar(32);\n'% name)
        c.stdin.write('SET %s=\'%s\';\n'% (name, value))





# start with loading bam files into the database with all loader functions we have built
# we assume that all bam or sam files are stored into the files directory.
# these files will all be loaded into the database with all loading methods that we have.
# files in the directory files/queryname are assumed to be ordered by queryname and are
# therefore also loaded into the pairwise storage schema

c = new_client()

# get files
files_path = os.path.join(SRCDIR, "files")
files_qname_path = os.path.join(files_path, "queryname")
files = [ f for f in os.listdir(files_path) if os.path.isfile(os.path.join(files_path, f)) ]
files_qname = [ f for f in os.listdir(files_qname_path) if os.path.isfile(os.path.join(files_qname_path, f)) ]

# use bam_loader_repos
c.stdin.write("CALL bam_loader_repos('%s', 0, 8);\n"% files_path)
c.stdin.write("CALL bam_loader_repos('%s', 1, 8);\n"% files_qname_path)

# use bam_loader_files by first writing temporary files and then invoking these loaders
f_tmp_path = os.path.join(SRCDIR, "tmp_list.txt")
f_tmp_qname_path = os.path.join(SRCDIR, "tmp_list_qname.txt")
f_tmp = open(f_tmp_path, "w")
f_tmp_qname = open(f_tmp_qname_path, "w")

for file in files:
    f_tmp.write("%s\n"% os.path.join(files_path, file))

for file in files_qname:
    f_tmp_qname.write("%s\n"% os.path.join(files_qname_path, file))

f_tmp.close()
f_tmp_qname.close()

c.stdin.write("CALL bam_loader_files('%s', 0, 8);\n"% f_tmp_path)
c.stdin.write("CALL bam_loader_files('%s', 1, 8);\n"% f_tmp_qname_path)

# use bam_loader_file on all separate files
for file in files:
    c.stdin.write("CALL bam_loader_file('%s', 0);\n"% os.path.join(files_path, file))

for file in files_qname:
    c.stdin.write("CALL bam_loader_file('%s', 1);\n"% os.path.join(files_qname_path, file))

out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

# clean up the temporary files
os.remove(f_tmp_path)
os.remove(f_tmp_qname_path)


# extract the files that have been inserted into the database
# gives us a list of (file_id, dbschema) tuples
c = new_client()
c.stdin.write("SELECT file_id, dbschema FROM bam.files;\n")
out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)

p = re.compile('^\s*\[\s*(\d+)\s*,\s*(\d)\s*\]\s*$', re.MULTILINE) # Parses raw DB output
inserted = []
for match in p.finditer(out):
    inserted.append((int(match.group(1)), int(match.group(2))))



# now we will test the drop functionality by removing all duplicate bam/sam files from the database
# we know that only the first len(files) + len(files_qname) files are unique, so we remove
# everything with a file id higher than len(files) + len(files_qname)
to_query = [] # to_query will be filled with the files that will be used to run queries on
c = new_client()
for (file_id, dbschema) in inserted:
    if file_id > len(files) + len(files_qname):
        c.stdin.write("CALL bam_drop_file(%d, %d);\n"% (file_id, dbschema))
    else:
        to_query.append((file_id, dbschema))

# and just to check, count the number of bam files
c.stdin.write("SELECT COUNT(*) FROM bam.files;\n")

out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)





# Now we will execute all benchmark queries on all SAM/BAM files in the to_query list
# Furthermore, we will load every file entirely into the export table and then export
# it to both the SAM and the BAM file formats by using sam_export and bam_export
# respectively.

# Note: some things are commented out, since the bam_export functionality is not available yet

def tmp_output(file_id, sam=True):
    return os.path.join(SRCDIR, "output_%d.%s"% (file_id, "sam" if sam else "bam"))

def projection(file_id, dbschema):
    return "SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual FROM bam.%salignments_%d"\
        % (('unpaired_all_' if dbschema == 1 else ''), file_id)

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

# Determine the next file_id that will be assigned to a file that is loaded into the database
next_file_id = 1
for (file_id, dbschema) in inserted:
    next_file_id = max(next_file_id, file_id+1)

for (file_id, dbschema) in to_query:
    # benchmark 1
    for uc in range(1, 6):
        for ln in open(os.path.join(SRCDIR, 'benchmarks_%s/query1.%d.sql'% (dbschema, uc))):
            c.stdin.write(ln.replace('alignments_i', 'alignments_%d'% file_id))

    # benchmark 2
    for uc in range(1, 13):
        for ln in open(os.path.join(SRCDIR, 'benchmarks_%s/query2.%d.sql'% (dbschema, uc))):
            c.stdin.write(ln.replace('alignments_i', 'alignments_%d'% file_id)
                            .replace('alignments_j', 'alignments_%d'% file_id))

    # load this file into export table (export table is always cleared after exporting, so it should be empty here)
    c.stdin.write("INSERT INTO bam.export (%s);\n"% projection(file_id, dbschema))

    # write it to SAM file
    c.stdin.write("CALL sam_export('%s');\n"% tmp_output(file_id))

    # insert it again into the export table
    c.stdin.write("INSERT INTO bam.export (%s);\n"% projection(file_id, dbschema))

    # and write it to BAM file (will throw exception))
    c.stdin.write("CALL bam_export('%s');\n"% tmp_output(file_id, sam=False))

    # Now load SAM (and BAM) files back into the database
    sam_id = next_file_id
    # bamid = next_file_id + 1
    # next_file_id += 2
    next_file_id += 1

    # Insert both the SAM and BAM files into the database. Note that we always insert into
    # dbschema 0, since the outputted files aren't necessarily ordered by qname.
    c.stdin.write("CALL bam_loader_file('%s', 0);\n"% tmp_output(file_id))
    #c.stdin.write("CALL bam_loader_file('%s', 0);\n"% tmp_output(file_id, sam=False))

    # We now have the alignment data for this file in three different alignment tables. The ultimate
    # export/import test is now to see if the data in them is exactly the same (except for virtual_offset)
    # by taking the except (should be empty of course)
    c.stdin.write("%s\nEXCEPT\n%s;\n"% (projection(file_id, dbschema), projection(sam_id, 0)))
    #c.stdin.write("%s EXCEPT %s";\n% (projection(file_id, dbschema), projection(bam_id, 0)))


# The output will contain explicit file_ids in the table names, making the table names
# variable. We don't want a test to fail on this, so we have to remove te file
# ids from the table names.

# Note: Has been commented out, since file ids should always be the same, since we always
# start out with an empty database

#def replace(matchobj):
#    return 'alignments_%si'% (matchobj.group(1) if matchobj.group(1) else '')
#p = re.compile('alignments_(extra_)?\d+')
#out = (re.subn(p, replace, out))[0]

out, err = c.communicate()
sys.stdout.write(out)
sys.stderr.write(err)


# All that is left to do is delete the temporary SAM/BAM files
for (file_id, dbschema) in to_query:
    os.remove(tmp_output(file_id))
    #os.remove(tmp_output(file_id, sam=False))