select isauuid('this is not a uuid'); -- false
select isauuid(''); -- false
select isauuid(4); -- false
select isauuid('XYZaee1a538-aca1-381b-d9f4-8c29ef3f5'); -- false

select isauuid(uuid()); -- true
select isauuid('aee1a538-aca1-381b-d9f4-8c29ef3f5f34'); -- true
select isauuid('AEE1A538-ACA1-381B-D9F4-8C29EF3F5F34'); -- true
select isauuid(null); -- false
select cast('aee1a538-aca1-381b-d9f4-8c29ef3f5f34' as uuid); -- true
select cast('AEE1A538-ACA1-381B-D9F4-8C29EF3F5F34' as uuid); -- true

select cast('XYZaee1a538-aca1-381b-d9f4-8c29ef3f5' as uuid); -- error: Not a UUID
