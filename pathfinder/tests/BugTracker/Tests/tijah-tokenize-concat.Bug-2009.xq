let $record := <Bijschrift_NL>foo</Bijschrift_NL>
return <item> {tijah:tokenize(concat($record/Bijschrift_NL/text(), " Hallo wereld!"))} </item>
