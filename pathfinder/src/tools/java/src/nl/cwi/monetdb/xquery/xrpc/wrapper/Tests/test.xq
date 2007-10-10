import module namespace t = "xrpcwrapper-testfunctions"
        at "/tmp/functions.xq";

(: Test 1. individual functions :)
(:
execute at {"localhost:50002"} {t:echoVoid()}
execute at {"localhost:50002"} {t:echoInteger(234)}
execute at {"localhost:50002"} {t:echoDouble(234.43)}
execute at {"localhost:50002"} {t:echoParam((23.4, 45.1),
                                            (<hello><world/></hello>,
                                            <foo><bar/></foo>))}
execute at {"localhost:50002"} {t:echoString("Hello")}
execute at {"localhost:50002"} { t:getPerson("persons.xml",
                                             "person2544")}
execute at {"localhost:50002"} {t:getDoc("bib.xml")}
execute at {"localhost:50002"} {t:Q_B1()}
execute at {"localhost:50002"} {t:Q_B2()}
execute at {"localhost:50002"} {t:Q_B3("person2153")}
:)

(: Test 2. for-loop :)
(:
for $i in 1 to 3
return (
    execute at {"localhost:50002"} {t:echoVoid()},
    execute at {"localhost:50002"} {t:echoInteger(234)},
    execute at {"localhost:50002"} {t:echoDouble(234.43)},
    execute at {"localhost:50002"} {t:echoParam((23.4, 45.1),
                                                (<hello><world/></hello>,
                                                <foo><bar/></foo>))},
    execute at {"localhost:50002"} {t:echoString("Hello")},
    execute at {"localhost:50002"} { t:getPerson("persons.xml",
                                                 "person2544")},
    execute at {"localhost:50002"} {t:getDoc("bib.xml")},
    execute at {"localhost:50002"} {t:Q_B1()},
    execute at {"localhost:50002"} {t:Q_B2()},
    execute at {"localhost:50002"} {t:Q_B3("person2153")}
)
:)
(:  Test 3. large output :)
for $i in 1 to 50
return
    execute at {"localhost:50002"} {t:Q_B2()}
