import bam

bam.exec_sql_file("bam_loader_repos.sql", {'PWD': bam.SRCDIR})
