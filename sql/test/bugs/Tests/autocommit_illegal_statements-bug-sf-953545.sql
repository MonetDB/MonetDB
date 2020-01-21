-- the following statements should all fail
ROLLBACK;

COMMIT;

SAVEPOINT failingsavepoint;

-- this one is incorrect since the savepoint should not exist
-- however, the server might answer with an error about auto_commit
-- for that might be cheaper
RELEASE SAVEPOINT failingsavepoint;
