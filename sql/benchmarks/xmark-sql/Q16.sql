select p1250.tail
from   hidx p1267, hidx p1270, hidx p1277, hidx p1278, hidx p1324,
       hidx p1325, hidx p1326, hidx p1332, hidx p1356, hidx p1357, attx p1358,
       hidx p1249, attx p1250
where  p1267.tblid = 1267
and    p1270.tblid = 1270
and    p1277.tblid = 1277
and    p1278.tblid = 1278
and    p1324.tblid = 1324
and    p1325.tblid = 1325
and    p1326.tblid = 1326
and    p1332.tblid = 1332
and    p1356.tblid = 1356
and    p1357.tblid = 1357
and    p1358.tblid = 1358
and    p1249.tblid = 1249
and    p1250.tblid = 1250
and    p1267.tail = p1270.head
and    p1270.tail = p1277.head
and    p1277.tail = p1278.head
and    p1278.tail = p1324.head
and    p1324.tail = p1325.head
and    p1325.tail = p1326.head
and    p1326.tail = p1332.head
and    p1332.tail = p1356.head
and    p1356.tail = p1357.head
and    p1357.tail = p1358.head
and    p1249.tblid = 1249
and    p1250.tblid = 1250
and    p1249.tail = p1250.head
and    p1249.head = p1267.head;
