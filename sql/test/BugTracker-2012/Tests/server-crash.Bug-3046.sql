START TRANSACTION;

CREATE TABLE node (
	id               integer                  NOT NULL,
	text_ref         integer                  NOT NULL,
	corpus_ref       integer                  NOT NULL,
	namespace        character varying(100),
	name             character varying(100)   NOT NULL,
	"left"           integer                  NOT NULL,
	"right"          integer                  NOT NULL,
	token_index      integer,
	continuous       boolean,
	span             character varying(2000),
	toplevel_corpus  integer                  NOT NULL,
	left_token       integer,
	right_token      integer
);
CREATE TABLE rank (
	pre            integer        NOT NULL,
	post           integer        NOT NULL,
	node_ref       integer        NOT NULL,
	component_ref  integer        NOT NULL,
	parent         integer,
	root           boolean,
	level          integer        NOT NULL
);
CREATE TABLE component (
	id         integer                 NOT NULL,
	type       character(1),
	namespace  character varying(255),
	name       character varying(255)
);
ALTER TABLE node ADD CONSTRAINT "PK_node" PRIMARY KEY (id);
ALTER TABLE component ADD CONSTRAINT "PK_component" PRIMARY KEY (id);
ALTER TABLE rank ADD CONSTRAINT "PK_rank" PRIMARY KEY (pre);
ALTER TABLE rank ADD CONSTRAINT "UNIQ_rank_pre" UNIQUE (pre);
ALTER TABLE rank ADD CONSTRAINT "UNIQ_rank_post" UNIQUE (post);
ALTER TABLE rank ADD CONSTRAINT "FK_rank_parent" FOREIGN KEY (parent) REFERENCES rank (pre);
ALTER TABLE rank ADD CONSTRAINT "FK_rank_node" FOREIGN KEY (node_ref) REFERENCES node (id);
ALTER TABLE rank ADD CONSTRAINT "FK_rank_component" FOREIGN KEY (component_ref) REFERENCES component (id);

SELECT
  count(*)
FROM
  (
    SELECT DISTINCT
      node1.id AS id1, node2.id AS id2, node1.toplevel_corpus
    FROM
      node AS node1, rank AS rank1, component AS component1,
      node AS node2, rank AS rank2, component AS component2
    WHERE
      component1.id = component2.id AND
      component1.name IS NULL AND
      component1.type = 'd' AND
      component2.name IS NULL AND
      component2.type = 'd' AND
      rank1.component_ref = component1.id AND
      rank1.node_ref = node1.id AND
      rank1.pre = rank2.parent AND
      rank2.component_ref = component2.id AND
      rank2.node_ref = node2.id
  ) AS solutions;

ROLLBACK;
