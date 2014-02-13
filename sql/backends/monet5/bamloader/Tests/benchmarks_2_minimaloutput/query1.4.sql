SELECT rname, pos, virtual_offset
FROM bam.intern_alignments_i
WHERE qname = qname_1_4
ORDER BY rname, pos;