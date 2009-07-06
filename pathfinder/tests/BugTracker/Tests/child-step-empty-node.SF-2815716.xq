let $doc := <root><field/></root>
return <foo>{$doc//field/string()}</foo>/*
