SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_1
WHERE rname = 'chr22'
  AND pos >= 1000000
  AND pos <= 20000000
ORDER BY pos;

SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.unpaired_all_alignments_3
WHERE rname = 'chr22'
  AND pos >= 1000000
  AND pos <= 20000000
ORDER BY pos;
