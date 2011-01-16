-- Generated on Mon Jul 27 16:45:08 CEST 2009

-- subject_term_result = subject_term
CREATE VIEW subject_term_result AS 
	SELECT ALL a1,
		 prob 
	FROM subject_term;

-- doc_2_DATA_docDict = docDict
CREATE VIEW doc_2_DATA_docDict AS 
	SELECT ALL docID AS a1,
		 doc AS a2,
		 type AS a3,
		 prob 
	FROM docDict;

-- doc_2_DATA_neDict = neDict
CREATE VIEW doc_2_DATA_neDict AS 
	SELECT ALL neID AS a1,
		 ne AS a2,
		 type AS a3,
		 prob 
	FROM neDict;

-- doc_2_DATA_termDict = termDict
CREATE VIEW doc_2_DATA_termDict AS 
	SELECT ALL termID AS a1,
		 term AS a2,
		 prob 
	FROM termDict;

-- doc_2_DATA_doc_string = doc_string
CREATE VIEW doc_2_DATA_doc_string AS 
	SELECT ALL docID AS a1,
		 attribute AS a2,
		 value AS a3,
		 prob 
	FROM doc_string;

-- doc_2_DATA_ne_string = ne_string
CREATE VIEW doc_2_DATA_ne_string AS 
	SELECT ALL neID AS a1,
		 attribute AS a2,
		 value AS a3,
		 prob 
	FROM ne_string;

-- doc_2_DATA_doc_doc = doc_doc
CREATE VIEW doc_2_DATA_doc_doc AS 
	SELECT ALL docID1 AS a1,
		 predicate AS a2,
		 docID2 AS a3,
		 prob 
	FROM doc_doc;

-- doc_2_DATA_ne_doc = ne_doc
CREATE VIEW doc_2_DATA_ne_doc AS 
	SELECT ALL neID AS a1,
		 predicate AS a2,
		 docID AS a3,
		 prob 
	FROM ne_doc;

-- doc_2_DATA_ne_ne = ne_ne
CREATE VIEW doc_2_DATA_ne_ne AS 
	SELECT ALL neID1 AS a1,
		 predicate AS a2,
		 neID2 AS a3,
		 prob 
	FROM ne_ne;

-- doc_2_DATA_tf = tf_sum
CREATE VIEW doc_2_DATA_tf AS 
	SELECT ALL termID AS a1,
		 docID AS a2,
		 prob 
	FROM tf_sum;

-- doc_2_DATA_idf_bm25 = idf_bm25
CREATE VIEW doc_2_DATA_idf_bm25 AS 
	SELECT ALL termID AS a1,
		 prob 
	FROM idf_bm25;

-- doc_2_DATA_result = PROJECT[$1](doc_2_DATA_docDict)
CREATE VIEW doc_2_DATA_result AS 
	SELECT ALL a1,
		 prob 
	FROM doc_2_DATA_docDict;

-- rank_DOC_TF_IDF_9_SOURCE_result = doc_2_DATA_result
CREATE VIEW rank_DOC_TF_IDF_9_SOURCE_result AS 
	SELECT ALL a1,
		 prob 
	FROM doc_2_DATA_result;

-- rank_DOC_TF_IDF_9_QTERMS_result = subject_term_result
CREATE VIEW rank_DOC_TF_IDF_9_QTERMS_result AS 
	SELECT ALL a1,
		 prob 
	FROM subject_term_result;

-- rank_DOC_TF_IDF_9_qterm_1 = JOIN[$2=$1](doc_2_DATA_termDict,rank_DOC_TF_IDF_9_QTERMS_result)
CREATE VIEW rank_DOC_TF_IDF_9_qterm_1 AS 
	SELECT ALL doc_2_DATA_termDict.a1 AS a1,
		 doc_2_DATA_termDict.a2 AS a2,
		 rank_DOC_TF_IDF_9_QTERMS_result.a1 AS a3,
		 doc_2_DATA_termDict.prob * rank_DOC_TF_IDF_9_QTERMS_result.prob AS prob 
	FROM doc_2_DATA_termDict, rank_DOC_TF_IDF_9_QTERMS_result 
	WHERE doc_2_DATA_termDict.a2=rank_DOC_TF_IDF_9_QTERMS_result.a1;

-- rank_DOC_TF_IDF_9_qterm = PROJECT[$1](rank_DOC_TF_IDF_9_qterm_1)
CREATE VIEW rank_DOC_TF_IDF_9_qterm AS 
	SELECT ALL a1,
		 prob 
	FROM rank_DOC_TF_IDF_9_qterm_1;

-- rank_DOC_TF_IDF_9_idf = doc_2_DATA_idf
CREATE VIEW rank_DOC_TF_IDF_9_idf AS 
	SELECT ALL a1,
		 prob 
	FROM doc_2_DATA_idf_bm25;

-- rank_DOC_TF_IDF_9_weighted_qterm_1 = JOIN[$1=$1](rank_DOC_TF_IDF_9_qterm,rank_DOC_TF_IDF_9_idf)
CREATE VIEW rank_DOC_TF_IDF_9_weighted_qterm_1 AS 
	SELECT ALL rank_DOC_TF_IDF_9_qterm.a1 AS a1,
		 rank_DOC_TF_IDF_9_idf.a1 AS a2,
		 rank_DOC_TF_IDF_9_qterm.prob * rank_DOC_TF_IDF_9_idf.prob AS prob 
	FROM rank_DOC_TF_IDF_9_qterm, rank_DOC_TF_IDF_9_idf 
	WHERE rank_DOC_TF_IDF_9_qterm.a1=rank_DOC_TF_IDF_9_idf.a1;

-- rank_DOC_TF_IDF_9_weighted_qterm = PROJECT[$1](rank_DOC_TF_IDF_9_weighted_qterm_1)
CREATE VIEW rank_DOC_TF_IDF_9_weighted_qterm AS 
	SELECT ALL a1,
		 prob 
	FROM rank_DOC_TF_IDF_9_weighted_qterm_1;

-- rank_DOC_TF_IDF_9_norm_weighted_qterm_L = PROJECT[$1](rank_DOC_TF_IDF_9_weighted_qterm)
CREATE VIEW rank_DOC_TF_IDF_9_norm_weighted_qterm_L AS 
	SELECT ALL a1,
		 prob 
	FROM rank_DOC_TF_IDF_9_weighted_qterm;

-- rank_DOC_TF_IDF_9_norm_weighted_qterm_R_2 = PROJECT[$1](rank_DOC_TF_IDF_9_weighted_qterm)
CREATE VIEW rank_DOC_TF_IDF_9_norm_weighted_qterm_R_2 AS 
	SELECT ALL a1,
		 prob 
	FROM rank_DOC_TF_IDF_9_weighted_qterm;

-- rank_DOC_TF_IDF_9_norm_weighted_qterm_R_1 = PROJECT DISJOINT[$1](rank_DOC_TF_IDF_9_norm_weighted_qterm_R_2)
CREATE VIEW rank_DOC_TF_IDF_9_norm_weighted_qterm_R_1 AS 
	SELECT ALL a1,
		 sum(prob) AS prob 
	FROM rank_DOC_TF_IDF_9_norm_weighted_qterm_R_2 
	GROUP BY a1;

-- rank_DOC_TF_IDF_9_norm_weighted_qterm_R = PROJECT INVERSE(rank_DOC_TF_IDF_9_norm_weighted_qterm_R_1)
CREATE VIEW rank_DOC_TF_IDF_9_norm_weighted_qterm_R AS 
	SELECT ALL a1,
		 1/prob AS prob 
	FROM rank_DOC_TF_IDF_9_norm_weighted_qterm_R_1;

-- rank_DOC_TF_IDF_9_norm_weighted_qterm_1 = JOIN[$1=$1](rank_DOC_TF_IDF_9_norm_weighted_qterm_L,rank_DOC_TF_IDF_9_norm_weighted_qterm_R)
CREATE VIEW rank_DOC_TF_IDF_9_norm_weighted_qterm_1 AS 
	SELECT ALL rank_DOC_TF_IDF_9_norm_weighted_qterm_L.a1 AS a1,
		 rank_DOC_TF_IDF_9_norm_weighted_qterm_R.a1 AS a2,
		 rank_DOC_TF_IDF_9_norm_weighted_qterm_L.prob * rank_DOC_TF_IDF_9_norm_weighted_qterm_R.prob AS prob 
	FROM rank_DOC_TF_IDF_9_norm_weighted_qterm_L, rank_DOC_TF_IDF_9_norm_weighted_qterm_R 
	WHERE rank_DOC_TF_IDF_9_norm_weighted_qterm_L.a1=rank_DOC_TF_IDF_9_norm_weighted_qterm_R.a1;

-- rank_DOC_TF_IDF_9_norm_weighted_qterm = PROJECT[$1](rank_DOC_TF_IDF_9_norm_weighted_qterm_1)
CREATE VIEW rank_DOC_TF_IDF_9_norm_weighted_qterm AS 
	SELECT ALL a1,
		 prob 
	FROM rank_DOC_TF_IDF_9_norm_weighted_qterm_1;

-- rank_DOC_TF_IDF_9_tf = doc_2_DATA_tf
CREATE VIEW rank_DOC_TF_IDF_9_tf AS 
	SELECT ALL a1,
		 a2,
		 prob 
	FROM doc_2_DATA_tf;

-- rank_DOC_TF_IDF_9_RETRIEVE_result_1 = JOIN[$1=$1](rank_DOC_TF_IDF_9_norm_weighted_qterm,rank_DOC_TF_IDF_9_tf)
CREATE VIEW rank_DOC_TF_IDF_9_RETRIEVE_result_1 AS 
	SELECT ALL rank_DOC_TF_IDF_9_norm_weighted_qterm.a1 AS a1,
		 rank_DOC_TF_IDF_9_tf.a1 AS a2,
		 rank_DOC_TF_IDF_9_tf.a2 AS a3,
		 rank_DOC_TF_IDF_9_norm_weighted_qterm.prob * rank_DOC_TF_IDF_9_tf.prob AS prob 
	FROM rank_DOC_TF_IDF_9_norm_weighted_qterm, rank_DOC_TF_IDF_9_tf 
	WHERE rank_DOC_TF_IDF_9_norm_weighted_qterm.a1=rank_DOC_TF_IDF_9_tf.a1;

-- rank_DOC_TF_IDF_9_RETRIEVE_result = PROJECT SUM[$3](rank_DOC_TF_IDF_9_RETRIEVE_result_1)
CREATE VIEW rank_DOC_TF_IDF_9_RETRIEVE_result AS 
	SELECT ALL a3 AS a1,
		 sum(prob) AS prob 
	FROM rank_DOC_TF_IDF_9_RETRIEVE_result_1 
	GROUP BY a3;

-- find_NE_from_DOC_3_SOURCE_result = doc_2_DATA_result
CREATE VIEW find_NE_from_DOC_3_SOURCE_result AS 
	SELECT ALL a1,
		 prob 
	FROM doc_2_DATA_result;

-- find_NE_from_DOC_3_selected_nes_R_1 = SELECT[$2="assignee_of"](doc_2_DATA_ne_doc)
CREATE VIEW find_NE_from_DOC_3_selected_nes_R_1 AS 
	SELECT ALL a1,
		 a2,
		 a3,
		 prob 
	FROM doc_2_DATA_ne_doc 
	WHERE doc_2_DATA_ne_doc.a2='assignee_of';

-- find_NE_from_DOC_3_selected_nes_R = PROJECT[$1,$3](find_NE_from_DOC_3_selected_nes_R_1)
CREATE VIEW find_NE_from_DOC_3_selected_nes_R AS 
	SELECT ALL a1,
		 a3 AS a2,
		 prob 
	FROM find_NE_from_DOC_3_selected_nes_R_1;

-- find_NE_from_DOC_3_selected_nes_1 = JOIN[$1=$2](find_NE_from_DOC_3_SOURCE_result,find_NE_from_DOC_3_selected_nes_R)
CREATE VIEW find_NE_from_DOC_3_selected_nes_1 AS 
	SELECT ALL find_NE_from_DOC_3_SOURCE_result.a1 AS a1,
		 find_NE_from_DOC_3_selected_nes_R.a1 AS a2,
		 find_NE_from_DOC_3_selected_nes_R.a2 AS a3,
		 find_NE_from_DOC_3_SOURCE_result.prob * find_NE_from_DOC_3_selected_nes_R.prob AS prob 
	FROM find_NE_from_DOC_3_SOURCE_result, find_NE_from_DOC_3_selected_nes_R 
	WHERE find_NE_from_DOC_3_SOURCE_result.a1=find_NE_from_DOC_3_selected_nes_R.a2;

-- find_NE_from_DOC_3_selected_nes = PROJECT[$2,$3](find_NE_from_DOC_3_selected_nes_1)
CREATE VIEW find_NE_from_DOC_3_selected_nes AS 
	SELECT ALL a2 AS a1,
		 a3 AS a2,
		 prob 
	FROM find_NE_from_DOC_3_selected_nes_1;

-- find_NE_from_DOC_3_RESULT_result = PROJECT DISTINCT[$1](find_NE_from_DOC_3_selected_nes)
CREATE VIEW find_NE_from_DOC_3_RESULT_result AS 
	SELECT ALL a1,
		 1-prod(1-prob) AS prob 
	FROM find_NE_from_DOC_3_selected_nes 
	GROUP BY a1;

-- assignee_1_SOURCE_result = find_NE_from_DOC_3_RESULT_result
CREATE VIEW assignee_1_SOURCE_result AS 
	SELECT ALL a1,
		 prob 
	FROM find_NE_from_DOC_3_RESULT_result;

-- assignee_1_attributes_1 = JOIN[$1=$1](assignee_1_SOURCE_result,doc_2_DATA_ne_string)
CREATE VIEW assignee_1_attributes_1 AS 
	SELECT ALL assignee_1_SOURCE_result.a1 AS a1,
		 doc_2_DATA_ne_string.a1 AS a2,
		 doc_2_DATA_ne_string.a2 AS a3,
		 doc_2_DATA_ne_string.a3 AS a4,
		 assignee_1_SOURCE_result.prob * doc_2_DATA_ne_string.prob AS prob 
	FROM assignee_1_SOURCE_result, doc_2_DATA_ne_string 
	WHERE assignee_1_SOURCE_result.a1=doc_2_DATA_ne_string.a1;

-- assignee_1_attributes = PROJECT[$2,$3,$4](assignee_1_attributes_1)
CREATE VIEW assignee_1_attributes AS 
	SELECT ALL a2 AS a1,
		 a3 AS a2,
		 a4 AS a3,
		 prob 
	FROM assignee_1_attributes_1;

-- assignee_1_selected_attr_1 = SELECT[$2="name",$3=~"INC"](assignee_1_attributes)
CREATE VIEW assignee_1_selected_attr_1 AS 
	SELECT ALL a1,
		 a2,
		 a3,
		 prob 
	FROM assignee_1_attributes 
	WHERE assignee_1_attributes.a2='name' AND assignee_1_attributes.a3 LIKE '%INC%';

-- assignee_1_selected_attr = PROJECT[$1](assignee_1_selected_attr_1)
CREATE VIEW assignee_1_selected_attr AS 
	SELECT ALL a1,
		 prob 
	FROM assignee_1_selected_attr_1;

-- assignee_1_RETRIEVE_result = PROJECT DISTINCT[$1](assignee_1_selected_attr)
CREATE VIEW assignee_1_RETRIEVE_result AS 
	SELECT ALL a1,
		 1-prod(1-prob) AS prob 
	FROM assignee_1_selected_attr 
	GROUP BY a1;

-- assignee_2_SOURCE_result = find_NE_from_DOC_3_RESULT_result
CREATE VIEW assignee_2_SOURCE_result AS 
	SELECT ALL a1,
		 prob 
	FROM find_NE_from_DOC_3_RESULT_result;

-- assignee_2_attributes_1 = JOIN[$1=$1](assignee_2_SOURCE_result,doc_2_DATA_ne_string)
CREATE VIEW assignee_2_attributes_1 AS 
	SELECT ALL assignee_2_SOURCE_result.a1 AS a1,
		 doc_2_DATA_ne_string.a1 AS a2,
		 doc_2_DATA_ne_string.a2 AS a3,
		 doc_2_DATA_ne_string.a3 AS a4,
		 assignee_2_SOURCE_result.prob * doc_2_DATA_ne_string.prob AS prob 
	FROM assignee_2_SOURCE_result, doc_2_DATA_ne_string 
	WHERE assignee_2_SOURCE_result.a1=doc_2_DATA_ne_string.a1;

-- assignee_2_attributes = PROJECT[$2,$3,$4](assignee_2_attributes_1)
CREATE VIEW assignee_2_attributes AS 
	SELECT ALL a2 AS a1,
		 a3 AS a2,
		 a4 AS a3,
		 prob 
	FROM assignee_2_attributes_1;

-- assignee_2_selected_attr_1 = SELECT[$2="name",$3=~"CO"](assignee_2_attributes)
CREATE VIEW assignee_2_selected_attr_1 AS 
	SELECT ALL a1,
		 a2,
		 a3,
		 prob 
	FROM assignee_2_attributes 
	WHERE assignee_2_attributes.a2='name' AND assignee_2_attributes.a3 LIKE '%CO%';

-- assignee_2_selected_attr = PROJECT[$1](assignee_2_selected_attr_1)
CREATE VIEW assignee_2_selected_attr AS 
	SELECT ALL a1,
		 prob 
	FROM assignee_2_selected_attr_1;

-- assignee_2_RETRIEVE_result = PROJECT DISTINCT[$1](assignee_2_selected_attr)
CREATE VIEW assignee_2_RETRIEVE_result AS 
	SELECT ALL a1,
		 1-prod(1-prob) AS prob 
	FROM assignee_2_selected_attr 
	GROUP BY a1;

-- mix_assignees_SOURCE1_result = assignee_1_RETRIEVE_result
CREATE VIEW mix_assignees_SOURCE1_result AS 
	SELECT ALL a1,
		 prob 
	FROM assignee_1_RETRIEVE_result;

-- mix_assignees_SOURCE2_result = assignee_2_RETRIEVE_result
CREATE VIEW mix_assignees_SOURCE2_result AS 
	SELECT ALL a1,
		 prob 
	FROM assignee_2_RETRIEVE_result;

CREATE TABLE mixture1(a1 VARCHAR(1000), prob DOUBLE);
INSERT INTO mixture1 VALUES ('mix1', 0.600000 );
CREATE TABLE mixture2(a1 VARCHAR(1000), prob DOUBLE);
INSERT INTO mixture2 VALUES ('mix2', 0.400000 );
-- mix_assignees_or1 = JOIN[](mix_assignees_SOURCE1_result,mixture1)
CREATE VIEW mix_assignees_or1 AS 
	SELECT ALL mix_assignees_SOURCE1_result.a1 AS a1,
		 mixture1.a1 AS a2,
		 mix_assignees_SOURCE1_result.prob * mixture1.prob AS prob 
	FROM mix_assignees_SOURCE1_result, mixture1;

-- mix_assignees_or2 = JOIN[](mix_assignees_SOURCE2_result,mixture2)
CREATE VIEW mix_assignees_or2 AS 
	SELECT ALL mix_assignees_SOURCE2_result.a1 AS a1,
		 mixture2.a1 AS a2,
		 mix_assignees_SOURCE2_result.prob * mixture2.prob AS prob 
	FROM mix_assignees_SOURCE2_result, mixture2;

-- mix_assignees_RETRIEVE_result_1 = UNITE ALL(mix_assignees_or1,mix_assignees_or2)
CREATE VIEW mix_assignees_RETRIEVE_result_1 AS 
	SELECT ALL pra2sql_tmp.a1,
		 pra2sql_tmp.a2,
		 pra2sql_tmp.prob 
	FROM (SELECT ALL a1, a2, prob FROM mix_assignees_or1 UNION ALL SELECT ALL a1, a2, prob FROM mix_assignees_or2) AS pra2sql_tmp;

-- mix_assignees_RETRIEVE_result = PROJECT SUM[$1](mix_assignees_RETRIEVE_result_1)
CREATE VIEW mix_assignees_RETRIEVE_result AS 
	SELECT ALL a1,
		 sum(prob) AS prob 
	FROM mix_assignees_RETRIEVE_result_1 
	GROUP BY a1;

-- filter_DOC_with_NE_8_SOURCE_result = doc_2_DATA_result
CREATE VIEW filter_DOC_with_NE_8_SOURCE_result AS 
	SELECT ALL a1,
		 prob 
	FROM doc_2_DATA_result;

-- filter_DOC_with_NE_8_SUBJECTS_result = mix_assignees_RETRIEVE_result
CREATE VIEW filter_DOC_with_NE_8_SUBJECTS_result AS 
	SELECT ALL a1,
		 prob 
	FROM mix_assignees_RETRIEVE_result;

-- filter_DOC_with_NE_8_pred_1 = SELECT[$2="assignee_of"](doc_2_DATA_ne_doc)
CREATE VIEW filter_DOC_with_NE_8_pred_1 AS 
	SELECT ALL a1,
		 a2,
		 a3,
		 prob 
	FROM doc_2_DATA_ne_doc 
	WHERE doc_2_DATA_ne_doc.a2='assignee_of';

-- filter_DOC_with_NE_8_pred = PROJECT[$1,$3](filter_DOC_with_NE_8_pred_1)
CREATE VIEW filter_DOC_with_NE_8_pred AS 
	SELECT ALL a1,
		 a3 AS a2,
		 prob 
	FROM filter_DOC_with_NE_8_pred_1;

-- filter_DOC_with_NE_8_nes_R_1 = SELECT[$2="assignee_of"](doc_2_DATA_ne_doc)
CREATE VIEW filter_DOC_with_NE_8_nes_R_1 AS 
	SELECT ALL a1,
		 a2,
		 a3,
		 prob 
	FROM doc_2_DATA_ne_doc 
	WHERE doc_2_DATA_ne_doc.a2='assignee_of';

-- filter_DOC_with_NE_8_nes_R = PROJECT[$1,$3](filter_DOC_with_NE_8_nes_R_1)
CREATE VIEW filter_DOC_with_NE_8_nes_R AS 
	SELECT ALL a1,
		 a3 AS a2,
		 prob 
	FROM filter_DOC_with_NE_8_nes_R_1;

-- filter_DOC_with_NE_8_nes_1 = JOIN[$1=$2](filter_DOC_with_NE_8_SOURCE_result,filter_DOC_with_NE_8_nes_R)
CREATE VIEW filter_DOC_with_NE_8_nes_1 AS 
	SELECT ALL filter_DOC_with_NE_8_SOURCE_result.a1 AS a1,
		 filter_DOC_with_NE_8_nes_R.a1 AS a2,
		 filter_DOC_with_NE_8_nes_R.a2 AS a3,
		 filter_DOC_with_NE_8_SOURCE_result.prob * filter_DOC_with_NE_8_nes_R.prob AS prob 
	FROM filter_DOC_with_NE_8_SOURCE_result, filter_DOC_with_NE_8_nes_R 
	WHERE filter_DOC_with_NE_8_SOURCE_result.a1=filter_DOC_with_NE_8_nes_R.a2;

-- filter_DOC_with_NE_8_nes = PROJECT[$2,$3](filter_DOC_with_NE_8_nes_1)
CREATE VIEW filter_DOC_with_NE_8_nes AS 
	SELECT ALL a2 AS a1,
		 a3 AS a2,
		 prob 
	FROM filter_DOC_with_NE_8_nes_1;

-- filter_DOC_with_NE_8_RETRIEVE_result_1 = JOIN[$1=$1](filter_DOC_with_NE_8_SUBJECTS_result,filter_DOC_with_NE_8_nes)
CREATE VIEW filter_DOC_with_NE_8_RETRIEVE_result_1 AS 
	SELECT ALL filter_DOC_with_NE_8_SUBJECTS_result.a1 AS a1,
		 filter_DOC_with_NE_8_nes.a1 AS a2,
		 filter_DOC_with_NE_8_nes.a2 AS a3,
		 filter_DOC_with_NE_8_SUBJECTS_result.prob * filter_DOC_with_NE_8_nes.prob AS prob 
	FROM filter_DOC_with_NE_8_SUBJECTS_result, filter_DOC_with_NE_8_nes 
	WHERE filter_DOC_with_NE_8_SUBJECTS_result.a1=filter_DOC_with_NE_8_nes.a1;

-- filter_DOC_with_NE_8_RETRIEVE_result = PROJECT DISTINCT[$3](filter_DOC_with_NE_8_RETRIEVE_result_1)
CREATE VIEW filter_DOC_with_NE_8_RETRIEVE_result AS 
	SELECT ALL a3 AS a1,
		 1-prod(1-prob) AS prob 
	FROM filter_DOC_with_NE_8_RETRIEVE_result_1 
	GROUP BY a3;

-- find_NE_from_DOC_11_SOURCE_result = filter_DOC_with_NE_8_RETRIEVE_result
CREATE VIEW find_NE_from_DOC_11_SOURCE_result AS 
	SELECT ALL a1,
		 prob 
	FROM filter_DOC_with_NE_8_RETRIEVE_result;

-- find_NE_from_DOC_11_selected_nes_R_1 = SELECT[$2="inventor_of"](doc_2_DATA_ne_doc)
CREATE VIEW find_NE_from_DOC_11_selected_nes_R_1 AS 
	SELECT ALL a1,
		 a2,
		 a3,
		 prob 
	FROM doc_2_DATA_ne_doc 
	WHERE doc_2_DATA_ne_doc.a2='inventor_of';

-- find_NE_from_DOC_11_selected_nes_R = PROJECT[$1,$3](find_NE_from_DOC_11_selected_nes_R_1)
CREATE VIEW find_NE_from_DOC_11_selected_nes_R AS 
	SELECT ALL a1,
		 a3 AS a2,
		 prob 
	FROM find_NE_from_DOC_11_selected_nes_R_1;

-- find_NE_from_DOC_11_selected_nes_1 = JOIN[$1=$2](find_NE_from_DOC_11_SOURCE_result,find_NE_from_DOC_11_selected_nes_R)
CREATE VIEW find_NE_from_DOC_11_selected_nes_1 AS 
	SELECT ALL find_NE_from_DOC_11_SOURCE_result.a1 AS a1,
		 find_NE_from_DOC_11_selected_nes_R.a1 AS a2,
		 find_NE_from_DOC_11_selected_nes_R.a2 AS a3,
		 find_NE_from_DOC_11_SOURCE_result.prob * find_NE_from_DOC_11_selected_nes_R.prob AS prob 
	FROM find_NE_from_DOC_11_SOURCE_result, find_NE_from_DOC_11_selected_nes_R 
	WHERE find_NE_from_DOC_11_SOURCE_result.a1=find_NE_from_DOC_11_selected_nes_R.a2;

-- find_NE_from_DOC_11_selected_nes = PROJECT[$2,$3](find_NE_from_DOC_11_selected_nes_1)
CREATE VIEW find_NE_from_DOC_11_selected_nes AS 
	SELECT ALL a2 AS a1,
		 a3 AS a2,
		 prob 
	FROM find_NE_from_DOC_11_selected_nes_1;

-- find_NE_from_DOC_11_RESULT_result = PROJECT DISTINCT[$1](find_NE_from_DOC_11_selected_nes)
CREATE VIEW find_NE_from_DOC_11_RESULT_result AS 
	SELECT ALL a1,
		 1-prod(1-prob) AS prob 
	FROM find_NE_from_DOC_11_selected_nes 
	GROUP BY a1;

-- filter_NE_with_DOC_12_SOURCE_result = find_NE_from_DOC_11_RESULT_result
CREATE VIEW filter_NE_with_DOC_12_SOURCE_result AS 
	SELECT ALL a1,
		 prob 
	FROM find_NE_from_DOC_11_RESULT_result;

-- filter_NE_with_DOC_12_SUBJECTS_result = rank_DOC_TF_IDF_9_RETRIEVE_result
CREATE VIEW filter_NE_with_DOC_12_SUBJECTS_result AS 
	SELECT ALL a1,
		 prob 
	FROM rank_DOC_TF_IDF_9_RETRIEVE_result;

-- filter_NE_with_DOC_12_pred_1 = SELECT[$2="inventor_of"](doc_2_DATA_ne_doc)
CREATE VIEW filter_NE_with_DOC_12_pred_1 AS 
	SELECT ALL a1,
		 a2,
		 a3,
		 prob 
	FROM doc_2_DATA_ne_doc 
	WHERE doc_2_DATA_ne_doc.a2='inventor_of';

-- filter_NE_with_DOC_12_pred = PROJECT[$1,$3](filter_NE_with_DOC_12_pred_1)
CREATE VIEW filter_NE_with_DOC_12_pred AS 
	SELECT ALL a1,
		 a3 AS a2,
		 prob 
	FROM filter_NE_with_DOC_12_pred_1;

-- filter_NE_with_DOC_12_nes_1 = JOIN[$1=$1](filter_NE_with_DOC_12_SOURCE_result,filter_NE_with_DOC_12_pred)
CREATE VIEW filter_NE_with_DOC_12_nes_1 AS 
	SELECT ALL filter_NE_with_DOC_12_SOURCE_result.a1 AS a1,
		 filter_NE_with_DOC_12_pred.a1 AS a2,
		 filter_NE_with_DOC_12_pred.a2 AS a3,
		 filter_NE_with_DOC_12_SOURCE_result.prob * filter_NE_with_DOC_12_pred.prob AS prob 
	FROM filter_NE_with_DOC_12_SOURCE_result, filter_NE_with_DOC_12_pred 
	WHERE filter_NE_with_DOC_12_SOURCE_result.a1=filter_NE_with_DOC_12_pred.a1;

-- filter_NE_with_DOC_12_nes = PROJECT[$2,$3](filter_NE_with_DOC_12_nes_1)
CREATE VIEW filter_NE_with_DOC_12_nes AS 
	SELECT ALL a2 AS a1,
		 a3 AS a2,
		 prob 
	FROM filter_NE_with_DOC_12_nes_1;

-- filter_NE_with_DOC_12_RETRIEVE_result_1 = JOIN[$1=$2](filter_NE_with_DOC_12_SUBJECTS_result,filter_NE_with_DOC_12_nes)
CREATE VIEW filter_NE_with_DOC_12_RETRIEVE_result_1 AS 
	SELECT ALL filter_NE_with_DOC_12_SUBJECTS_result.a1 AS a1,
		 filter_NE_with_DOC_12_nes.a1 AS a2,
		 filter_NE_with_DOC_12_nes.a2 AS a3,
		 filter_NE_with_DOC_12_SUBJECTS_result.prob * filter_NE_with_DOC_12_nes.prob AS prob 
	FROM filter_NE_with_DOC_12_SUBJECTS_result, filter_NE_with_DOC_12_nes 
	WHERE filter_NE_with_DOC_12_SUBJECTS_result.a1=filter_NE_with_DOC_12_nes.a2;

-- filter_NE_with_DOC_12_RETRIEVE_result = PROJECT DISTINCT[$2](filter_NE_with_DOC_12_RETRIEVE_result_1)
CREATE VIEW filter_NE_with_DOC_12_RETRIEVE_result AS 
	SELECT ALL a2 AS a1,
		 1-prod(1-prob) AS prob 
	FROM filter_NE_with_DOC_12_RETRIEVE_result_1 
	GROUP BY a2;

-- temporary_request1_result = filter_NE_with_DOC_12_RETRIEVE_result
CREATE VIEW temporary_request1_result AS 
	SELECT ALL a1,
		 prob 
	FROM filter_NE_with_DOC_12_RETRIEVE_result;


-- End of generated SQL
