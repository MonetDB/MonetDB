import random

nr_of_records = 10000000
nr_of_different_records = 100000 # this is just a rough estimate. There are most likely to be duplicates after a batch uniform samples.

records = ""

for record in range(nr_of_records):
    records += "{0},'aa{0}aa'\n".format(random.randint(0, nr_of_different_records))

create_table_statement = "copy {nr_of_records} records into foo from stdin delimiters ',', '\n', '''';\n{records}".format(nr_of_records=nr_of_records, records=records)

print(create_table_statement)
