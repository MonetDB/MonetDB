SELECT pos, virtual_offset
FROM bam.alignments_i
WHERE rname = rname_1_3
  AND pos >= pos_1_3_1
  AND pos <= pos_1_3_2
ORDER BY pos;