SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_i
WHERE rname = rname_1_3
  AND pos >= pos_1_3_1
  AND pos <= pos_1_3_2
ORDER BY pos;
