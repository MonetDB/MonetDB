        # As a starting point, we provide some standard tzones
	#
	const RULE_MAR := rule("first sunday from end of march@02:00");
	const RULE_OCT := rule("first sunday from end of october@02:00");
	const TIMEZONES := new(str, tzone).col_name("monettime_tzones"); 
	
	TIMEZONES.insert("Wake Island",          tzone( 12*60));
	TIMEZONES.insert("Melbourne/Australia",  tzone( 11*60));
	TIMEZONES.insert("Brisbane/Australia",   tzone( 10*60));
	TIMEZONES.insert("Japan",                tzone( 09*60));
	TIMEZONES.insert("Singapore",            tzone( 08*60));
	TIMEZONES.insert("Thailand",             tzone( 07*60));
	TIMEZONES.insert("Kazakhstan",           tzone( 06*60, RULE_MAR, RULE_OCT)); 
	TIMEZONES.insert("Pakistan",             tzone( 05*60));
	TIMEZONES.insert("United Arab Emirates", tzone( 04*60)); 
	TIMEZONES.insert("Moscow/Russia",        tzone( 03*60, RULE_MAR, RULE_OCT)); 
	TIMEZONES.insert("East/Europe",          tzone( 02*60, RULE_MAR, RULE_OCT)); 
	TIMEZONES.insert("West/Europe",          tzone( 01*60, RULE_MAR, RULE_OCT)); 
	TIMEZONES.insert("GMT",                  tzone( 00*00)); 
	TIMEZONES.insert("UK",                   tzone( 00*00, RULE_MAR, RULE_OCT)); 
	TIMEZONES.insert("Azore Islands",        tzone(-01*60)); 
	TIMEZONES.insert("Eastern/Brazil",       tzone(-02*60, RULE_OCT, RULE_MAR)); 
	TIMEZONES.insert("Western/Brazil",       tzone(-03*60, RULE_OCT, RULE_MAR)); 
	TIMEZONES.insert("Andes/Brazil",         tzone(-04*60, RULE_OCT, RULE_MAR)); 
	TIMEZONES.insert("East/USA",             tzone(-05*60, RULE_MAR, RULE_OCT)); 
	TIMEZONES.insert("Central/USA",          tzone(-06*60, RULE_MAR, RULE_OCT)); 
	TIMEZONES.insert("Mountain/USA",         tzone(-07*60, RULE_MAR, RULE_OCT)); 
	TIMEZONES.insert("Alaska/USA",           tzone(-09*60, RULE_MAR, RULE_OCT)); 
	TIMEZONES.insert("Hawaii/USA",           tzone(-10*60)); 
	TIMEZONES.insert("American Samoa",       tzone(-11*60)); 

	tzone_local(TIMEZONE("West/Europe"));


