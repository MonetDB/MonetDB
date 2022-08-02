select json.text(json '[1,2,3,4]', 'a');
	-- '1a2a3a4'
select json.text(json '[1,2,3,4]', 'ax');
	-- '1ax2ax3ax4'
select json.text(json '[1,2,3,4]', 'test');
	-- '1test2test3test4'
