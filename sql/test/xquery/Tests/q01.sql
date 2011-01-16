-- set optimizer='off';
-- !! START SCHEMA INFORMATION ** DO NOT EDIT THESE LINES !!
-- document-relation: document
-- document-pre: pre
-- document-size: size
-- document-level: level
-- document-kind: kind
-- document-value: value
-- document-name: name
-- document-tag: tag
-- result-relation: result
-- result-pos_nat: pos_nat
-- result-item_pre: item_pre
-- !! END SCHEMA INFORMATION ** DO NOT EDIT THESE LINES !!
WITH
-- Rel: ScjRel
-- Binding due to: dirty node
a0000(item1_pre, iter_nat) AS
  (SELECT DISTINCT c0004.pre, 1 AS iter_nat
     FROM xmldoc AS c0004,
          xmldoc AS c0003,
          xmldoc AS c0002,
          xmldoc AS c0001
    WHERE ((c0004.level = (c0003.level + 1)) AND (((c0003.pre + c0003.size) >=
          c0004.pre) AND ((c0004.pre > c0003.pre) AND ((c0004.kind = 1) AND
          ((c0004.name = 'person') AND ((c0003.level = (c0002.level + 1)) AND
          (((c0002.pre + c0002.size) >= c0003.pre) AND ((c0003.pre > c0002.pre)
          AND ((c0003.kind = 1) AND ((c0003.name = 'people') AND ((c0002.level =
          (c0001.level + 1)) AND (((c0001.pre + c0001.size) >= c0002.pre) AND
          ((c0002.pre > c0001.pre) AND ((c0002.kind = 1) AND ((c0002.name =
          'site') AND ((c0001.value = 'auctionG.xml') AND (c0001.kind =
          6)))))))))))))))))),

-- Rel: rownum(Rel)
-- Binding due to: dirty node
a0001(pos_nat, item1_pre) AS
  (SELECT  1 + ROW_NUMBER () OVER (ORDER BY c0005.item1_pre ASC) AS pos_nat,
          c0005.item1_pre
     FROM a0000 AS c0005),

-- Rel: number(Rel)
-- Binding due to: refctr > 1 and dirty node
a0002(iter_nat, pos_nat, item1_pre) AS
  (SELECT  1 + ROW_NUMBER () OVER () AS iter_nat, c0006.pos_nat, c0006.item1_pre
     FROM a0001 AS c0006),

-- Rel: project(Rel)
-- Binding due to: dirty node
a0003(iter1_nat) AS
  (SELECT DISTINCT c0008.iter_nat AS iter1_nat
     FROM xmldoc AS c0011,
          xmldoc AS c0009,
          xmldoc AS c0010,
          a0002 AS c0008
    WHERE ((c0011.value = 'person0') AND ((c0011.kind = 2) AND ((c0009.pre =
          c0011.pre) AND ((c0009.level = (c0010.level + 1)) AND (((c0010.pre +
          c0010.size) >= c0009.pre) AND ((c0009.pre > c0010.pre) AND
          ((c0009.kind = 2) AND ((c0009.name = 'id') AND (c0010.pre =
          c0008.item1_pre)))))))))),

-- Rel: rownum(Rel)
-- Binding due to: dirty node
a0004(pos1_nat, pos_nat, item1_pre) AS
  (SELECT  1 + ROW_NUMBER () OVER (ORDER BY c0008.pos_nat ASC) AS pos1_nat,
          c0008.pos_nat, c0008.item1_pre
     FROM a0003 AS c0012,
          a0002 AS c0008
    WHERE (c0008.iter_nat = c0012.iter1_nat)),

-- Rel: number(Rel)
-- Binding due to: refctr > 1 and dirty node
a0005(iter_nat, pos1_nat, item1_pre) AS
  (SELECT  1 + ROW_NUMBER () OVER () AS iter_nat, c0013.pos1_nat, c0013.item1_pre
     FROM a0004 AS c0013),

-- Rel: ScjRel
-- Binding due to: dirty node
a0006(item_pre, iter1_nat) AS
  (SELECT DISTINCT c0019.pre, c0016.iter_nat AS iter1_nat
     FROM xmldoc AS c0019,
          xmldoc AS c0017,
          xmldoc AS c0018,
          a0005 AS c0016
    WHERE ((c0019.level = (c0017.level + 1)) AND (((c0017.pre + c0017.size) >=
          c0019.pre) AND ((c0019.pre > c0017.pre) AND ((c0019.kind = 3) AND
          ((c0017.level = (c0018.level + 1)) AND (((c0018.pre + c0018.size) >=
          c0017.pre) AND ((c0017.pre > c0018.pre) AND ((c0017.kind = 1) AND
          ((c0017.name = 'name') AND (c0018.pre = c0016.item1_pre))))))))))),

-- Rel: rownum(Rel)
-- Binding due to: dirty node
a0007(sort_nat, item_pre, iter_nat, pos1_nat) AS
  (SELECT  1 + ROW_NUMBER () OVER
          (PARTITION BY c0015.iter_nat ORDER BY c0020.item_pre ASC) AS sort_nat,
          c0020.item_pre, c0015.iter_nat, c0015.pos1_nat
     FROM a0006 AS c0020,
          a0005 AS c0015
    WHERE (c0015.iter_nat = c0020.iter1_nat)),

-- Rel: rownum(Rel)
-- Binding due to: dirty node
a0008(pos_nat, sort_nat, item_pre, pos1_nat) AS
  (SELECT  1 + ROW_NUMBER () OVER (ORDER BY c0021.pos1_nat ASC, c0021.sort_nat ASC)
          AS pos_nat, c0021.sort_nat, c0021.item_pre, c0021.pos1_nat
     FROM a0007 AS c0021),

-- Rel: project(Rel)
-- Binding due to: dirty node
a0009(pos_nat, item_pre) AS
  (SELECT c0022.pos_nat, c0022.item_pre
     FROM a0008 AS c0022),

-- ====================
-- = RESULT RELATIONS =
-- ====================
result(pos_nat, item_pre) AS
  (SELECT c0023.pos_nat, c0023.item_pre
     FROM a0009 AS c0023),

document(pre, size, level, kind, value, tag, name) AS
  (SELECT pre, size, level, kind, value, name, name
     FROM xmldoc AS c0024)
select * from result; 
