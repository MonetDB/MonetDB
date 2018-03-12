from __future__ import print_function

print('create table histogram256_tab (')
for i in range(1,256):
    print('\tBIN%d NUMBER NOT NULL,' % i)

print('\tBIN256 NUMBER NOT NULL')
print(');')
