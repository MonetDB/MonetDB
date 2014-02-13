SELECT rname, pos, virtual_offset
FROM bam.unpaired_all_alignments_i
WHERE qname = qname_1_4
ORDER BY rname, pos;