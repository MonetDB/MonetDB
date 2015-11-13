# Populate the export table with a sequentially stored file
INSERT INTO bam.export (
	SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
	FROM bam.alignments_1
);

# Export data to SAM file
CALL bam.sam_export('OUTPUT_1');

# Load exported data back into a sequential table
CALL bam.bam_loader_file('OUTPUT_1', 0);

# Data inside original table should be exactly the same as the newly imported file
SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_1
EXCEPT
SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_73;

# Verify that the export table is now empty
SELECT * FROM bam.export;



# Populate the export table with a pairwise stored file
INSERT INTO bam.export (
	SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
	FROM bam.unpaired_all_alignments_3
);

# Export data to SAM file
CALL bam.sam_export('OUTPUT_2');

# Load exported data back into a sequential table
CALL bam.bam_loader_file('OUTPUT_2', 0);

# Data inside original table should be exactly the same as the newly imported file
SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_3
EXCEPT
SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_74;

# Verify that the export table is now empty
SELECT * FROM bam.export;
