import os
import bam

# Define output paths
output_1 = os.path.join(bam.SRCDIR, "files", "out_sequential.sam")
output_2 = os.path.join(bam.SRCDIR, "files", "out_pairwise.sam")

# Run test with these paths
bam.exec_sql_file("sam_export.sql", {'OUTPUT_1': output_1, 'OUTPUT_2': output_2})

# And remove our garbage
os.remove(output_1)
os.remove(output_2)
