--
--  Copyright (c) 2001, University of Amsterdam, The Netherlands.
--  All rights reserved.
--
--  Author(s):
--  Marc Navarro (mnavarro@wins.uva.nl)
--

-- This script creates the tables for VxSegmentations
--
--  USAGE
--    SQL> START makeTables.sql
--

-- PROMPT Creating table VxSegmentation_tab...

CREATE TABLE VxSegmentation_tab (
    Id                NUMBER         NOT NULL,
    Name              VARCHAR(40)   NOT NULL,
    VideoName         VARCHAR(40)   NOT NULL,
    Description       VARCHAR(80),
  CONSTRAINT VxSegmentation_pk   PRIMARY KEY(id),
  CONSTRAINT VxSegmentation_uniq UNIQUE(Name, VideoName)
);


-- PROMPT Creating table VxSegment_tab...

CREATE TABLE VxSegment_tab (
    Id                NUMBER         NOT NULL,
    SegIndex          NUMBER         NOT NULL,
    SegmentationId    NUMBER         NOT NULL,
    StartPos          NUMBER         NOT NULL,
    EndPos            NUMBER         NOT NULL,
  CONSTRAINT VxSegment_pk   PRIMARY KEY(Id),
  CONSTRAINT VxSegment_uniq UNIQUE(SegmentationId, SegIndex),
  CONSTRAINT VxSegment_fk   FOREIGN KEY(SegmentationId)
      REFERENCES VxSegmentation_tab ON DELETE CASCADE
);


-- PROMPT Creating table VxSegmentStringFeature_tab...

CREATE TABLE VxSegmentStringFeature_tab (
    SegmentId         NUMBER         NOT NULL,
    FieldName         VARCHAR(20)   NOT NULL,
    Value             VARCHAR(1024),
  CONSTRAINT VxSegmentStringFeat_pk PRIMARY KEY(SegmentId, FieldName),
  CONSTRAINT VxSegmentStringFeat_fk FOREIGN KEY(SegmentId)
      REFERENCES VxSegment_tab ON DELETE CASCADE
);


-- PROMPT Creating table VxSegmentDoubleFeature_tab...

CREATE TABLE VxSegmentDoubleFeature_tab (
    SegmentId         NUMBER         NOT NULL,
    FieldName         VARCHAR(20)   NOT NULL,
    Value             NUMBER         NOT NULL,
  CONSTRAINT VxSegmentDoubleFeat_pk PRIMARY KEY(SegmentId, FieldName),
  CONSTRAINT VxSegmentDoubleFeat_fk FOREIGN KEY(SegmentId)
      REFERENCES VxSegment_tab ON DELETE CASCADE
);


-- PROMPT Creating table VxSegmentIntFeature_tab...

CREATE TABLE VxSegmentIntFeature_tab (
    SegmentId         NUMBER         NOT NULL,
    FieldName         VARCHAR(20)   NOT NULL,
    Value             NUMBER         NOT NULL,
  CONSTRAINT VxSegmentIntFeat_pk PRIMARY KEY(SegmentId, FieldName),
  CONSTRAINT VxSegmentIntFeat_fk FOREIGN KEY(SegmentId)
      REFERENCES VxSegment_tab ON DELETE CASCADE
);


-- PROMPT Creating sequence VxSegmentation_id...

-- CREATE SEQUENCE VxSegmentation_id INCREMENT BY 1 START WITH 1;


-- PROMPT Creating sequence VxSegment_id...

-- CREATE SEQUENCE VxSegment_id      INCREMENT BY 1 START WITH 1; 

