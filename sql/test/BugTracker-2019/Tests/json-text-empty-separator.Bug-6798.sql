select json.text('"abc"', '');   -- Error in branch Nov2019, fixed in branch json
select json.text('["abc", "def"]', '');
select json.text('{"a":"abc", "b":"def"}', '');
