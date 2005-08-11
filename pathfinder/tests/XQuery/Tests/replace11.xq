(: multi-line input string, _without_ multi-line replace :)
fn:replace("
abcd
abcd
abcd
abcd", "(^a)", "*", "")
