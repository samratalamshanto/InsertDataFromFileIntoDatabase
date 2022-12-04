
import cx_Oracle
import psycopg2
con = psycopg2.connect(host="localhost", port=5432,
                       database="demo", user="postgres", password="1234")


# oracle test db
cx_Oracle.init_oracle_client(
    lib_dir=r"C:\Users\samrat.alam\Downloads\instantclient-basic-windows.x64-21.7.0.0.0dbru\instantclient_21_7")

cx_Oracle.clientversion()
# oracle test db
con = cx_Oracle.connect('SIEBELTST/DCrm@654321@RDMTEST-SCAN:1524/RMDTST12')


q = 'CREATE TABLE CBS_MSISDN_DATADUMP_PREPAID_NEW (ID NUMERIC(38) , MSISDN NUMERIC(38), OPERATOR varchar(255), CONNECTION_TYPE varchar(255), STATUS varchar(255))'
#s = "insert into CBS_MSISDN_DATADUMP_PREPAID_NEW (ID, MSISDN, OPERATOR, CONNECTION_TYPE, STATUS) values (%s, %s, %s , %s, %s)"

# oracle
s = "insert into CBS_MSISDN_DATADUMP_PREPAID_NEW (ID, MSISDN, OPERATOR, CONNECTION_TYPE, STATUS) values (:0,:1,:2,:3,:4)"

cur = con.cursor()
cur.execute(q)
records = []
#file = open("C:/Users/samrat.alam/Desktop/MD_MSISDN_DETAILS_DUMP_1.txt")

file = open("C:/Users/samrat.alam/Desktop/textfile1.txt")

print("Running....")

row = 0
for i in file.readlines():
    if (row == 0):
        row = row+1
        continue
    a = i.split(",")
    sub_records = []
    for j in a:
        sub_records.append(j.replace("\n", ""))
    records.append(sub_records)
# print(records)
for i in records:
    cur.executemany(s, records)
    records = []
con.commit()
print("End")
