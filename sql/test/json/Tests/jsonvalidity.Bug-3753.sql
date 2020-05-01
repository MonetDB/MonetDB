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

SELECT json.isValid(R'"\u003c\""');	-- true

SELECT json.isValid(R'01');             -- false
SELECT json.isValid(R'[01]');           -- false

SELECT json.isValid(r'0.001');          -- true
SELECT json.isValid(r'-0.001');         -- true

SELECT json.isValid(r'0.001e12');        -- true
SELECT json.isValid(r'-0.001e-12');      -- true

SELECT json.isValid(r'{"foo": 90}');     -- true
SELECT json.isValid(r'{"foo": 9}');      -- true
