# first need to make sure connector is installed:
#  pip install mysql-connector-python

import mysql.connector

print ("Loading VideoGame data")

mydb = mysql.connector.connect(
  user='root',
  passwd='iheartsql',  # the password for that use
  database='comp3421',   # the database to connect to
  host='127.0.0.1',   # localhost
  allow_local_infile=1  # needed so can load local files
)

myc = mydb.cursor()   # myc name short for "my cursor"

# We need to reset the variable that allows loading of local files 
myc.execute('set global local_infile = 1') 

myc.execute("drop table if exists vg_csv;")

myc.execute("create table vg_csv (ranking TEXT, name TEXT, platform TEXT, year TEXT, genre TEXT, publisher TEXT, na_sales TEXT, eu_sales TEXT, jp_sales TEXT, other_sales TEXT, global_sales TEXT);");

myc.execute("load data local infile 'vgsales.csv' into table vg_csv fields terminated by ',' enclosed by '\"' lines terminated by '\\n' ignore 1 rows;")
            

print(myc.rowcount, " tuples were inserted")

print("done")

# MUST commit the changes made  (if you did any inserts, deletes, updates, 
#                               load data. )
mydb.commit()
mydb.close() 

