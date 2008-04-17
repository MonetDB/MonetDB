(: Check for correct namtests :)

                                    (: Owners :)

("&#13; 1 ", attribute line { "1"}, doc("foo.xml")//(@foo2)/parent::b
,"&#13; 2 ", attribute line { "2"}, doc("foo.xml")//(@foo2)/parent::c
,"&#13; 3 ", attribute line { "3"}, doc("foo.xml")//(@foo2|text())/parent::a
,"&#13; 4 ", attribute line { "4"}, doc("foo.xml")//(@foo2|text())/parent::b
,"&#13; 5 ", attribute line { "5"}, doc("foo.xml")//(@foo2|text())/parent::c
,"&#13; 6 ", attribute line { "6"}, doc("foo.xml")//(@foo2|text())/ancestor::a
,"&#13; 7 ", attribute line { "7"}, doc("foo.xml")//(@foo2|text())/ancestor::b
,"&#13; 8 ", attribute line { "8"}, doc("foo.xml")//(@foo2|text())/ancestor::c

                                      (: *self steps :)

,"&#13; 9 ", attribute line { "9"}, doc("foo.xml")//@foo2/self::attribute(b)
,"&#13;10 ", attribute line {"10"}, doc("foo.xml")//@foo2/self::attribute(foo2)
,"&#13;11 ", attribute line {"11"}, doc("foo.xml")//@foo2/ancestor-or-self::attribute(b)
,"&#13;12 ", attribute line {"12"}, doc("foo.xml")//@foo2/ancestor-or-self::attribute(foo2)
,"&#13;13 ", attribute line {"13"}, doc("foo.xml")//@foo2/descendant-or-self::attribute(b)
,"&#13;14 ", attribute line {"14"}, doc("foo.xml")//@foo2/descendant-or-self::attribute(foo2)
,"&#13;15 ", attribute line {"15"}, doc("foo.xml")//(b|@foo2|text())/self::attribute(b)
,"&#13;16 ", attribute line {"16"}, doc("foo.xml")//(@foo2|text())/self::attribute(foo2)
,"&#13;17 ", attribute line {"17"}, doc("foo.xml")//(b|@foo2|text())/ancestor-or-self::attribute(b)
,"&#13;18 ", attribute line {"18"}, doc("foo.xml")//(@foo2|text())/ancestor-or-self::attribute(foo2)
,"&#13;19 ", attribute line {"19"}, doc("foo.xml")//(b|@foo2|text())/descendant-or-self::attribute(b)
,"&#13;20 ", attribute line {"20"}, doc("foo.xml")//(@foo2|text())/descendant-or-self::attribute(foo2)
,"&#13;")
