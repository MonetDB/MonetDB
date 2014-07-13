Examples from

  Developing time-oriented database applications in SQL
  Richard T. Snodgrass
  ISBN 1-55860-436-7
  Morgan Kaufmann Publishers
  <http://www.cs.arizona.edu/people/rts/tdbbook.pdf>

  Chapter 3: Instants and Intervals

----------------------------------------------------------------------

  Boolean results should be true!

  ROLLBACK: expected error (not always thrown by MonetDB)
  rollback: MonetDB error

Separate .sql files assume an auto commit client.  Since the SQL statements
are commented, it makes sense to run them with echoing such that a dialog
appears, such as with JdbcClient's -e option.
