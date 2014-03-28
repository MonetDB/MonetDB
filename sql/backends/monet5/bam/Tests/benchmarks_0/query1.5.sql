SELECT qname, flag, rname, pos, mapq, cigar, rnext, pnext, tlen, seq, qual
FROM bam.alignments_i
WHERE mapq > mapq_1_5
ORDER BY mapq DESC;
