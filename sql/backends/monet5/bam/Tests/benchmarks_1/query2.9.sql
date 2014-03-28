SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_i
WHERE rname = rname_2_9
  AND pos_2_9 >= pos
  AND pos_2_9 < pos + seq_length(cigar)
ORDER BY pos;
