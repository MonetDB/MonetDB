child::para
--
child::*
--
child::text()
--
child::node()
--
attribute::name
--
attribute::*
--
descendant::para
--
descendant-or-self::para
--
self::para
--
child::chapter/descendant::para
--
child::*/child::para
--
/descendant::para
--
/descendant::list/child::member
--
child::para[position() = 1]
--
child::para[position() = last()]
--
child::para[position() = last()-1]
--
child::para[position() > 1]
--
/descendant::figure[position() = 42]
--
child::doc/child::chapter[position() = 5]/child::section[position() = 2]
--
child::para[attribute::type = 'warning'][position() = 5]
--
child::para[position() = 5][attribute::type = 'warning']
--
child::chapter[child::title = 'Introduction']
--
child::chapter[child::title]
--
child::*[self::chapter or self::appendix]
--
child::*[self::chapter or self::appendix][position() = last()]
--
foo//bar
--
//foo/bar
--
foo/@bar
--
foo/../bar
--
../foo
--
if/for
--
foo:bar/foobar
--
if/descendant::else:return/child
--
/child::child:parent
--
/foo/comment()
--
/foo/text()
--
/foo/node()
--
/foo/processing-instruction()
--
/foo/processing-instruction("foobar")
