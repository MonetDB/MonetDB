"foo" eq "bar"
--
foo = bar
--
(1, 2) eq (2, 3)
--
(1, 2) = (2, 3)
--
"42" lt 43
--
5 >= "3"
--
4 != 7
--
book/author eq "Kennedy"
--
{-- This should result in true --}
<a>5</a> eq <a>5</a>
--
{-- This should result in false --}
<a>5</a> is <a>5</a>
--
{-- This should result in true (?) --}
<a>5</a> eq <b>7</b>
--
//book[order_no = "1234"] is //book[isbn = "1-2345-5678-9"]
--
foo >> bar
--
//open_auction//description << //open_auction//bidder
--
{-- This is true --}
foo isnot <a>7</a>
--
{-- This is implementation defined --}
foo << <a>7</a>
