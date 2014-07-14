import os
import bam

# Define output paths
output_1 = os.path.join(bam.SRCDIR, "files", "out_sequential.bam")
output_2 = os.path.join(bam.SRCDIR, "files", "out_pairwise.bam")

# Run test with these paths
bam.exec_sql_file("bam_export.sql", {'OUTPUT_1': output_1, 'OUTPUT_2': output_2})

# And remove our garbage
os.remove(output_1)
os.remove(output_2)
