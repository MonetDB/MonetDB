SELECT b.value, a.prob FROM temporary_request1_result as a 
	LEFT JOIN ne_string as b ON a.a1 = b.neid WHERE b."attribute"='name' ORDER BY a."prob" DESC LIMIT 10; 
