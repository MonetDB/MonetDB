import random

nr_of_values = 10000
nr_of_different_values = 10000 # this is just a rough estimate. There are most likely to be duplicates after a batch uniform samples.

values = "{}\n".format(random.randint(0, nr_of_different_values))

for record in range(nr_of_values):
    values += ",{}\n".format(random.randint(0,nr_of_different_values))

select_in_value_list_statement = "select * from foo where i in ({values});".format(values=values)

print(select_in_value_list_statement)
