import sys
import re
import bam

# This script drops the files with file IDs [5,12]
# Unfortunately, this functionality is not that clever yet and we have to
# provide the dbschema to the drop function
# To do that, we first get the dbschemas from the database and we create a mapping
# of file id to dbschema that we can pass to the bam.exec_sql_file function


# Run select query to get file info from files table
c = bam.new_client()
c.stdin.write("SELECT file_id, dbschema FROM bam.files;")
out, err = c.communicate()

# Parse this outcome into a dictionary {DBSCHEMA_file_id -> dbschema}
p = re.compile("^\s*\[\s*(\d+)\s*,\s*(\d)\s*\]\s*$", re.MULTILINE) # Parses raw DB output
mapping = {}
c = bam.new_client()
for match in p.finditer(out):
    fileId = int(match.group(1))
    dbschema = int(match.group(2))
    if fileId < 5:
        continue

    c.stdin.write("CALL bam.bam_drop_file(%d, %d);"% (fileId, dbschema))

out, err = c.communicate()
sys.stdout.write(out)
sys.stdout.write(err)
