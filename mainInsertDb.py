import cx_Oracle
import psycopg2
import pandas as pd
from time import strftime, localtime

cx_Oracle.init_oracle_client(
    lib_dir=r"C:\Users\samrat.alam\Downloads\instantclient-basic-windows.x64-21.7.0.0.0dbru\instantclient_21_7")
cx_Oracle.clientversion()


print(f'Starting Time: {strftime("%Y-%m-%d %H:%M:%S", localtime())}')

con = psycopg2.connect(host="localhost", port=5432,
                       database="demo", user="postgres", password="1234")

# oracle test db
con = cx_Oracle.connect('SIEBELTST/DCrm@654321@dbcx8-scan:2021/RMDTST12')


create_table_sql = 'CREATE TABLE CBS_MSISDN_DATADUMP_PREPAID_TEMP(ID NUMBER(38) GENERATED ALWAYS AS IDENTITY CACHE 10 ORDER,SUBSCRIBERNO VARCHAR2(200),BRANDID VARCHAR2(200),PAIDMODE VARCHAR2(200),LIFECYCLESTATE VARCHAR2(200),GENDER VARCHAR2(200))'

#s = "insert into CBS_MSISDN_DATADUMP_PREPAID_NEW (ID, MSISDN, OPERATOR, CONNECTION_TYPE, STATUS) values (%s, %s, %s , %s, %s)"
# oracle
insert_sql = "insert into CBS_MSISDN_DATADUMP_PREPAID_TEMP (SUBSCRIBERNO,BRANDID,PAIDMODE,LIFECYCLESTATE,GENDER) values (:1,:2,:3,:4,:5)"


cur = con.cursor()
cur.execute(create_table_sql)

# prepaid_30000000

df = pd.read_csv(filepath_or_buffer='prepaid_30000000.txt', delimiter="|",
                 header=None, names=['a', 'b', 'c', 'd', 'e'], encoding='utf-8-sig')
df.fillna(value=300000, inplace=True)
df = df.astype('int')
df.replace(to_replace=300000, value='Null', inplace=True)
df = df.astype('str')

batch_size = 1000000

records = []

i = 1
for index, row in df.iterrows():
    records.extend([[row['a'], row['b'], row['c'], row['d'], row['e']]])
    # print(records)

    if len(records) % batch_size == 0:
        cur.executemany(insert_sql, records)
        records = []
        print(f'Chunk No: {i} and Time: {strftime("%H:%M:%S", localtime())}')
        i = i+1
if records:
    cur.executemany(insert_sql, records)
    records = []
    print(f'chunk no: {i} and Time: {strftime("%H:%M:%S", localtime())}')


con.commit()
cur.close()
con.close()

print(f'Ending Time: {strftime("%Y-%m-%d %H:%M:%S", localtime())}')
