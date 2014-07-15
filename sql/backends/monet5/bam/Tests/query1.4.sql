SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_1
WHERE qname = 'sim_22_1_1'
ORDER BY rname, pos;

SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_3
WHERE qname = 'sim_22_1_1'
ORDER BY rname, pos;
