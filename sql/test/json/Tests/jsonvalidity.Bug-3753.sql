SELECT json.isvalid('[0,1]');
SELECT json.isvalid('[0,,1]');

SELECT json.isValid(null); 		-- false
SELECT json.isValid(''); 		-- false
SELECT json.isValid('null'); 		-- false 
SELECT json.isValid('"'); 		-- false
SELECT json.isValid('""'); 		-- false
SELECT json.isValid('"""'); 		-- false
SELECT json.isValid(E'"\\""'); 		-- false
SELECT json.isValid('""""'); 		-- false
SELECT json.isValid(E'"\\"\\""');	-- false

SELECT json.isValid('[]'); 		-- true
SELECT json.isValid('[null]'); 		-- true
SELECT json.isValid('[""]'); 		-- true
SELECT json.isValid('["""]'); 		-- false
SELECT json.isValid(E'["\\""]');	-- true
SELECT json.isValid('[""""]'); 		-- false
SELECT json.isValid(E'["\\"\\""]'); 	-- true

SELECT json.isValid('{}'); 		-- true
SELECT json.isValid('{"test":null}'); 	-- true
SELECT json.isValid('{"test":""}'); 	-- true
SELECT json.isValid('{"test":"""}'); 	-- false
SELECT json.isValid(E'{"test":"\\""}');	-- true
SELECT json.isValid('{"test":""""}'); 	-- false
SELECT json.isValid(E'{"test":"\\"\\""}'); -- true
