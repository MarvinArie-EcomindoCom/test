import mysql.connector
from datetime import datetime
connection = mysql.connector.connect(host='localhost',
                                    database='btpn',
                                    user='root',
                                    password='root')
date1=datetime.now()
date1=date1.strftime('%Y-%m-%d %H:%M:%S')
s='failed'
query=("insert into status values('"+date1+"','"+s+"')")
cursor = connection.cursor()
cursor.execute(query)
connection.commit()
print(cursor.rowcount, "Record inserted successfully into Laptop table")
cursor.close()
connection.close()