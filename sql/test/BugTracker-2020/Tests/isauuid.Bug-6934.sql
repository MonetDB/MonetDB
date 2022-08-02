select sys.isauuid('e31960fb-dc8b-452d-ab30-b342723e7565XYZ') as fals;
select sys.isauuid('e31960fbdc8b452dab30b342723e7565XYZ') as fals;
select sys.isauuid('00000000-0000-0000-0000-000000000000XYZ') as fals;
select sys.isauuid('FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFFXYZ') as fals;
select sys.isauuid('00000000000000000000000000000000XYZ') as fals;
select sys.isauuid('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFXYZ') as fals;
select sys.isauuid('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF %$#@!') as fals;

select cast('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF %$#@!' as uuid) as uuid_val;
select convert('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF %$#@!', uuid) as uuid_val;
select uuid 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF %$#@!' as uuid_val;
