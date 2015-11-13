import os
import bam

# We will write temporary files that can be passed to the bam_loader_files function

# Define file paths
files_path = os.path.join(bam.SRCDIR, "files.lst")
files_qname_path = os.path.join(bam.SRCDIR, "files_qname.lst")
files_many_path = os.path.join(bam.SRCDIR, "files_many.lst")

# And open them
files = open(files_path, "w")
files_qname = open(files_qname_path, "w")
files_many = open(files_many_path, "w")

# Write files to them
files.write("%s\n"% os.path.join(bam.SRCDIR, "files", "file1.bam"))
files.write("%s\n"% os.path.join(bam.SRCDIR, "files", "file2.sam"))
files_qname.write("%s\n"% os.path.join(bam.SRCDIR, "files", "queryname", "file1.bam"))
files_qname.write("%s\n"% os.path.join(bam.SRCDIR, "files", "queryname", "file2.sam"))

# Write many files to the many file list
for i in range(0, 30):
    files_many.write("%s\n"% os.path.join(bam.SRCDIR, "files", "file1.bam"))
    files_many.write("%s\n"% os.path.join(bam.SRCDIR, "files", "file2.sam"))

# Close them
files.close();
files_qname.close();
files_many.close();



# Ok, we have our files in place, call SQL file
bam.exec_sql_file("bam_loader_files.sql", {'PWD': bam.SRCDIR})


# And clean up our garbage
os.remove(files_path)
os.remove(files_qname_path)
os.remove(files_many_path)
