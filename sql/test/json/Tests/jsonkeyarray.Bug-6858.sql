select json.keyarray(json '{ "":0 }');
	-- [""]
select json.isvalid(json '{ "":0 }');
	-- true
select json.isobject(json '{ "":0 }');
	-- true

select json.keyarray(json '{ "":"" }');
	-- [""]
select json.isvalid(json '{ "":"" }');
	-- true
select json.isobject(json '{ "":"" }');
	-- true

select json.keyarray(json '{ "a":0 }');
	-- ["a"]
select json.isvalid(json '{ "a":0 }');
	-- true
select json.isobject(json '{ "a":0 }');
	-- true
