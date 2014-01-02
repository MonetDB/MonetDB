-- query 16 

select X01250.tail
from   X01267, X01270, X01277, X01278, X01324,
       X01325, X01326, X01332, X01356, X01357, X01358,
       X01249, X01250
where  X01267.tail = X01270.head
and    X01270.tail = X01277.head
and    X01277.tail = X01278.head
and    X01278.tail = X01324.head
and    X01324.tail = X01325.head
and    X01325.tail = X01326.head
and    X01326.tail = X01332.head
and    X01332.tail = X01356.head
and    X01356.tail = X01357.head
and    X01357.tail = X01358.head
and    X01249.tail = X01250.head
and    X01249.head = X01267.head;

