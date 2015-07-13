SELECT json.isvalid('[0,1]');
SELECT json.isvalid('[0,,1]');

SELECT json.isValid(null); 	-- false
SELECT json.isValid(''); 	-- false
SELECT json.isValid('null'); 	-- TRUE, must be false 
SELECT json.isValid('"'); 	-- false
SELECT json.isValid('""'); 	-- false
SELECT json.isValid('"""'); 	-- TRUE, must be false
SELECT json.isValid('"\""'); 	-- TRUE, must be false
SELECT json.isValid('""""'); 	-- false
SELECT json.isValid('"\"\""'); 	-- false

SELECT json.isValid('[]'); 	-- true
SELECT json.isValid('[null]'); 	-- true
SELECT json.isValid('[""]'); 	-- FALSE, must be true
SELECT json.isValid('["""]'); 	-- TRUE, must be false
SELECT json.isValid('["\""]'); 	-- true
SELECT json.isValid('[""""]'); 	-- false
SELECT json.isValid('["\"\""]'); 	-- FALSE, must be true

SELECT json.isValid('{}'); 	-- true
SELECT json.isValid('{"test":null}'); 	-- true
SELECT json.isValid('{"test":""}'); 	-- FALSE, must be true
SELECT json.isValid('{"test":"""}'); 	-- TRUE, must be false
SELECT json.isValid('{"test":"\""}'); 	-- true
SELECT json.isValid('{"test":""""}'); 	-- false
SELECT json.isValid('{"test":"\"\""}'); 	-- FALSE, must be true
