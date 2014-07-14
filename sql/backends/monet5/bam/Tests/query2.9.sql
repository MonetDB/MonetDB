SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_1
WHERE rname = 'chr22'
  AND 39996433 >= pos
  AND 39996433 < pos + bam.seq_length(cigar)
ORDER BY pos;

SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_3
WHERE rname = 'chr22'
  AND 39996433 >= pos
  AND 39996433 < pos + bam.seq_length(cigar)
ORDER BY pos;
