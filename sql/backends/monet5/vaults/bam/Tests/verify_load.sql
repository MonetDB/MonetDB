# Verify the contents of the first file and the third file
# Contents should be the same but first is loaded in sequential and third is loaded
# in pairwise storage schema
# AND verify extra info from just the first file
SELECT *
FROM bam.alignments_3;

SELECT *
FROM bam.alignments_extra_1;

SELECT *
FROM bam.paired_primary_alignments_3;

SELECT *
FROM bam.paired_secondary_alignments_3;

SELECT *
FROM bam.unpaired_alignments_3;
