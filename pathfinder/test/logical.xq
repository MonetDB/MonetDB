{-- true --}
1 = 1 and 2 = 2
--
{-- true --}
1 = 1 or 2 = 3
--
{-- Either false or the error value (implementation defined) --}
1 = 2 and 3 div 0 = 47
--
{-- Either true or the error value (implementation defined) --}
1 = 1 or 3 div 0 = 47
--
{-- Always the error value --}
1 = 1 and 3 div 0 = 47
