(: Check for correct duplicate elimination :)

                                    (: attribute step :)

("&#13; 1 ", attribute line { "1"}, doc("foo.xml")//(c,c)/@*

                                    (: self step :)

,"&#13; 2 ", attribute line { "2"}, doc("foo.xml")//(text(),text())/self::node()

                                    (: starting from attribute context nodes :)

,"&#13; 3 ", attribute line { "3"}, doc("foo.xml")//(@foo2,@foo2)/self::node()
,"&#13; 4 ", attribute line { "4"}, doc("foo.xml")//(@foo2,@foo2)/ancestor-or-self::node()
,"&#13; 5 ", attribute line { "5"}, doc("foo.xml")//(@foo2,@foo2)/descendant-or-self::node()
,"&#13; 6 ", attribute line { "6"}, doc("foo.xml")//(@foo2,@foo2)/parent::*

                                    (: starting from mixed context sequences :)

,"&#13; 7 ", attribute line { "7"}, doc("foo.xml")//(@foo2,@foo2,text(),text())/self::node()
,"&#13; 8 ", attribute line { "8"}, doc("foo.xml")//(@foo2,@foo2,text(),text())/descendant-or-self::node()
,"&#13; 9 ", attribute line { "9"}, doc("foo.xml")//(@foo2,@foo2,text(),text())/parent::node()
,"&#13;10 ", attribute line {"10"}, doc("foo.xml")//(@foo2,@foo2,text(),text())/ancestor-or-self::node()
,"&#13;11 ", attribute line {"11"}, doc("foo.xml")//(@foo2,@foo2,text(),text())/ancestor::node()
,"&#13;")
