SELECT json.isValid('[0.1e12]');
SELECT json.isValid('[1e12]');
SELECT json.isValid('{"foo":1e12}');
SELECT json.number(json.filter('{"foo":1e3}', '$.foo'));
