import os
import bam

# We will write temporary files that can be passed to the bam_loader_files function

# Define file paths
files_path = os.path.join(bam.SRCDIR, "files.txt")
files_qname_path = os.path.join(bam.SRCDIR, "files_qname.txt")

# And open them
files = open(files_path, "w")
files_qname = open(files_qname_path, "w")

# Write files to them
files.write("%s\n"% os.path.join(bam.SRCDIR, "files", "file1.bam"));
files.write("%s\n"% os.path.join(bam.SRCDIR, "files", "file2.sam"));
files_qname.write("%s\n"% os.path.join(bam.SRCDIR, "files", "queryname", "file1.bam"));
files_qname.write("%s\n"% os.path.join(bam.SRCDIR, "files", "queryname", "file2.sam"));

# Close them
files.close();
files_qname.close();



# Ok, we have our files in place, call SQL file
bam.exec_sql_file("bam_loader_files.sql", {'PWD': bam.SRCDIR})


# And clean up our garbage
os.remove(files_path);
os.remove(files_qname_path);