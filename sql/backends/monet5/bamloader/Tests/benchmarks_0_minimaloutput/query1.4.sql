SELECT rname, pos, virtual_offset
FROM bam.alignments_i
WHERE qname = qname_1_4
ORDER BY rname, pos;