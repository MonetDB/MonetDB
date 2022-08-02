SELECT json.isValid('"abc":"abc"');           -- false
SELECT json.isValid('{"abc":"abc":"abc"}');   -- false
