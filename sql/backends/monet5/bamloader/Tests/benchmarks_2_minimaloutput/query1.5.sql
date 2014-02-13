SELECT mapq, virtual_offset
FROM bam.intern_alignments_i
WHERE mapq > mapq_1_5
ORDER BY mapq DESC;