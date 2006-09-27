declare function xmltopxml($node as node()?)
{
typeswitch($node)
case $n as element() return <foo>{$n}</foo>
default return <foo/>
};
xmltopxml(<a/>)
