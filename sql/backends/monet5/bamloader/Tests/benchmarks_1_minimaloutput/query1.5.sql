SELECT mapq, virtual_offset
FROM bam.unpaired_all_alignments_i
WHERE mapq > mapq_1_5
ORDER BY mapq DESC;