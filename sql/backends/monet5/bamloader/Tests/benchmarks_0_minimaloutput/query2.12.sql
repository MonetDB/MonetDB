WITH alig AS (
    SELECT qname, flag, rname, pos, cigar, virtual_offset
    FROM bam.alignments_i
    WHERE bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm')
      AND bam_flag(flag, 'seco_alig') = True
      AND rname <> '*'
      AND pos > 0
      AND rnext = '*'
      AND pnext = 0
)
SELECT rname, distance, l_virtual_offset, r_virtual_offset
FROM (
    SELECT l.rname AS rname, 
        CASE WHEN l.pos < r.pos 
             THEN r.pos - (l.pos + seq_length(l.cigar))
             ELSE l.pos - (r.pos + seq_length(r.cigar))
        END AS distance,
        l.virtual_offset AS l_virtual_offset, r.virtual_offset AS r_virtual_offset
    FROM (
        SELECT qname, rname, pos, cigar, virtual_offset
        FROM alig
        WHERE bam_flag(flag, 'firs_segm') = True
    ) AS l JOIN (
        SELECT qname, rname, pos, cigar, virtual_offset
        FROM alig
        WHERE bam_flag(flag, 'last_segm') = True
    ) AS r 
        ON  l.qname = r.qname
        AND l.rname = r.rname
) AS alig_joined
WHERE distance > 0
  AND distance < distance_2_12
ORDER BY rname;