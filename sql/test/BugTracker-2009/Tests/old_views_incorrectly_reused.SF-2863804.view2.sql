START TRANSACTION;

CREATE VIEW DATA_docDict AS SELECT ALL docID AS a1, doc AS a2, type AS a3, prob FROM docDict;
CREATE VIEW DATA_doc_string AS SELECT ALL docID AS a1, attribute AS a2, value AS a3, prob FROM doc_string;
CREATE VIEW DATA_result AS SELECT ALL a1, prob FROM DATA_docDict;

CREATE VIEW find_TERM_from_DOC_attribute_1_RESULT_result_3 AS 
	SELECT ALL DATA_doc_string.a1 AS a1, DATA_doc_string.a2 AS a2, 
		DATA_doc_string.a3 AS a3, DATA_result.a1 AS a4, 
		DATA_doc_string.prob * DATA_result.prob AS prob 
	FROM DATA_doc_string, DATA_result 
	WHERE DATA_doc_string.a1=DATA_result.a1;

CREATE VIEW find_TERM_from_DOC_attribute_1_RESULT_result_2 AS 
	SELECT ALL a1, a2, a3, a4, prob 
	FROM find_TERM_from_DOC_attribute_1_RESULT_result_3 
	WHERE find_TERM_from_DOC_attribute_1_RESULT_result_3.a2='date';

CREATE VIEW find_TERM_from_DOC_attribute_1_RESULT_result_1 AS 
	SELECT ALL a3 AS a1, prob FROM find_TERM_from_DOC_attribute_1_RESULT_result_2;

CREATE VIEW find_TERM_from_DOC_attribute_1_RESULT_result AS 
	SELECT ALL a1, 1-prod(1-prob) AS prob FROM find_TERM_from_DOC_attribute_1_RESULT_result_1 GROUP BY a1;

SELECT a.a1 as value, 'term' as type, a.prob FROM find_TERM_from_DOC_attribute_1_RESULT_result as a ORDER BY "prob" DESC, value LIMIT 50 OFFSET 0;

ROLLBACK;
