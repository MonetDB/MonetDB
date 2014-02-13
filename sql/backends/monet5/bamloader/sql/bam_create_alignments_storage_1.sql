CREATE TABLE bam.paired_primary_alignments_i (
    l_virtual_offset              BIGINT      NOT NULL,
    r_virtual_offset              BIGINT      NOT NULL,
    qname                         STRING      NOT NULL,
    l_flag                        SMALLINT    NOT NULL,
    l_rname                       STRING      NOT NULL,
    l_pos                         INT         NOT NULL,
    l_mapq                        SMALLINT    NOT NULL,
    l_cigar                       STRING      NOT NULL,
    l_rnext                       STRING      NOT NULL,
    l_pnext                       INT         NOT NULL,
    l_tlen                        INT         NOT NULL,
    l_seq                         STRING      NOT NULL,
    l_qual                        STRING      NOT NULL,
    r_flag                        SMALLINT    NOT NULL,
    r_rname                       STRING      NOT NULL,
    r_pos                         INT         NOT NULL,
    r_mapq                        SMALLINT    NOT NULL,
    r_cigar                       STRING      NOT NULL,
    r_rnext                       STRING      NOT NULL,
    r_pnext                       INT         NOT NULL,
    r_tlen                        INT         NOT NULL,
    r_seq                         STRING      NOT NULL,
    r_qual                        STRING      NOT NULL,
    CONSTRAINT paired_primary_alignments_i_pkey_l_virtual_offset_r_virtual_offset PRIMARY KEY (l_virtual_offset, r_virtual_offset)
);

CREATE TABLE bam.paired_secondary_alignments_i (
    l_virtual_offset              BIGINT      NOT NULL,
    r_virtual_offset              BIGINT      NOT NULL,
    qname                         STRING      NOT NULL,
    l_flag                        SMALLINT    NOT NULL,
    l_rname                       STRING      NOT NULL,
    l_pos                         INT         NOT NULL,
    l_mapq                        SMALLINT    NOT NULL,
    l_cigar                       STRING      NOT NULL,
    l_rnext                       STRING      NOT NULL,
    l_pnext                       INT         NOT NULL,
    l_tlen                        INT         NOT NULL,
    l_seq                         STRING      NOT NULL,
    l_qual                        STRING      NOT NULL,
    r_flag                        SMALLINT    NOT NULL,
    r_rname                       STRING      NOT NULL,
    r_pos                         INT         NOT NULL,
    r_mapq                        SMALLINT    NOT NULL,
    r_cigar                       STRING      NOT NULL,
    r_rnext                       STRING      NOT NULL,
    r_pnext                       INT         NOT NULL,
    r_tlen                        INT         NOT NULL,
    r_seq                         STRING      NOT NULL,
    r_qual                        STRING      NOT NULL,
    CONSTRAINT paired_secondary_alignments_i_pkey_l_virtual_offset_r_virtual_offset PRIMARY KEY (l_virtual_offset, r_virtual_offset)
);

CREATE TABLE bam.unpaired_alignments_i (
    virtual_offset                BIGINT      NOT NULL,
    qname                         STRING      NOT NULL,
    flag                          SMALLINT    NOT NULL,
    rname                         STRING      NOT NULL,
    pos                           INT         NOT NULL,
    mapq                          SMALLINT    NOT NULL,
    cigar                         STRING      NOT NULL,
    rnext                         STRING      NOT NULL,
    pnext                         INT         NOT NULL,
    tlen                          INT         NOT NULL,
    seq                           STRING      NOT NULL,
    qual                          STRING      NOT NULL,
    CONSTRAINT unpaired_alignments_i_pkey_virtual_offset PRIMARY KEY (virtual_offset)
);

CREATE TABLE bam.alignments_extra_i (
    tag                           CHAR(2)     NOT NULL,
    virtual_offset                BIGINT      NOT NULL,
    type                          CHAR(1)     NOT NULL,
    value                         STRING,
    CONSTRAINT alignments_extra_i_pkey_tag_virtual_offset PRIMARY KEY (tag, virtual_offset)
);

CREATE VIEW bam.unpaired_primary_alignments_i AS
    SELECT l_virtual_offset AS virtual_offset, qname, l_flag AS flag, l_rname AS rname, l_pos AS pos, l_mapq AS mapq, l_cigar AS cigar, l_rnext AS rnext, l_pnext AS pnext, l_tlen AS tlen, l_seq AS seq, l_qual AS qual
    FROM bam.paired_primary_alignments_i
    UNION ALL
    SELECT r_virtual_offset AS virtual_offset, qname, r_flag AS flag, r_rname AS rname, r_pos AS pos, r_mapq AS mapq, r_cigar AS cigar, r_rnext AS rnext, r_pnext AS pnext, r_tlen AS tlen, r_seq AS seq, r_qual AS qual
    FROM bam.paired_primary_alignments_i;

CREATE VIEW bam.unpaired_all_primary_alignments_i AS
    SELECT *
    FROM bam.unpaired_primary_alignments_i
    UNION ALL
    SELECT *
    FROM bam.unpaired_alignments_i
    WHERE bam_flag(flag, 'seco_alig') = False
      AND bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm');

CREATE VIEW bam.unpaired_secondary_alignments_i AS
    SELECT l_virtual_offset AS virtual_offset, qname, l_flag AS flag, l_rname AS rname, l_pos AS pos, l_mapq AS mapq, l_cigar AS cigar, l_rnext AS rnext, l_pnext AS pnext, l_tlen AS tlen, l_seq AS seq, l_qual AS qual
    FROM bam.paired_secondary_alignments_i
    UNION ALL
    SELECT r_virtual_offset AS virtual_offset, qname, r_flag AS flag, r_rname AS rname, r_pos AS pos, r_mapq AS mapq, r_cigar AS cigar, r_rnext AS rnext, r_pnext AS pnext, r_tlen AS tlen, r_seq AS seq, r_qual AS qual
    FROM bam.paired_secondary_alignments_i;
    
CREATE VIEW bam.unpaired_all_secondary_alignments_i AS
    SELECT *
    FROM bam.unpaired_secondary_alignments_i
    UNION ALL
    SELECT *
    FROM bam.unpaired_alignments_i
    WHERE bam_flag(flag, 'seco_alig') = True
      AND bam_flag(flag, 'firs_segm') <> bam_flag(flag, 'last_segm');

CREATE VIEW bam.unpaired_all_alignments_i AS
SELECT *
FROM bam.unpaired_primary_alignments_i
UNION ALL
SELECT *
FROM bam.unpaired_secondary_alignments_i
UNION ALL
SELECT *
FROM bam.unpaired_alignments_i;
