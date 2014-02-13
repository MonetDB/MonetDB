               CREATE TABLE bam.alignments_i (
                    virtual_offset                BIGINT      NOT NULL,
if mask[0]          qname                         STRING      NOT NULL,
if mask[1]          flag                          SMALLINT    NOT NULL,
if mask[2]          rname                         STRING      NOT NULL,
if mask[3]          pos                           INT         NOT NULL,
if mask[4]          mapq                          SMALLINT    NOT NULL,
if mask[5]          cigar                         STRING      NOT NULL,
if mask[6]          rnext                         STRING      NOT NULL,
if mask[7]          pnext                         INT         NOT NULL,
if mask[8]          tlen                          INT         NOT NULL,
if mask[9]          seq                           STRING      NOT NULL,
if mask[10]         qual                          STRING      NOT NULL,
                    CONSTRAINT alignments_i_pkey_virtual_offset PRIMARY KEY (virtual_offset)
               );

if mask[11]    CREATE TABLE bam.alignments_extra_i (
if mask[11]         tag                           CHAR(2)     NOT NULL,
if mask[11]         virtual_offset                BIGINT      NOT NULL,
if mask[11]         type                          CHAR(1)     NOT NULL,
if mask[11]         value                         STRING,
if mask[11]         CONSTRAINT alignments_extra_i_pkey_tag_virtual_offset PRIMARY KEY (tag, virtual_offset),
if mask[11]         CONSTRAINT alignments_extra_i_fkey_virtual_offset FOREIGN KEY (virtual_offset) REFERENCES bam.alignments_i (virtual_offset)
if mask[11]    );