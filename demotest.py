import cx_Oracle

# C:\Users\samrat.alam\Downloads\instantclient-basic-windows.x64-21.7.0.0.0dbru\instantclient_21_7
cx_Oracle.init_oracle_client(
    lib_dir=r"C:\Users\samrat.alam\Downloads\instantclient-basic-windows.x64-21.7.0.0.0dbru\instantclient_21_7")

cx_Oracle.clientversion()
# oracle test db
con = cx_Oracle.connect('SIEBELTST/DCrm@654321@RDMTEST-SCAN:1524/RMDTST12')


# import psycopg2
# con = psycopg2.connect(host="localhost", port=5432,
#                        database="demo", user="postgres", password="1234")


q = 'CREATE TABLE CBS_MSISDN_DATADUMP_PREPAID(SubscriberNo NUMERIC(38) , BrandID NUMERIC(38), PaidMode NUMERIC(38), LifeCycleState NUMERIC(38), Gender NUMERIC(38))'
# s = "insert into CBS_MSISDN_DATADUMP_PREPAID (SubscriberNo,BrandID,PaidMode,LifeCycleState,Gender) values  (%s, %s, %s , %s, %s)" #postgresql
# oracle
s = "insert into CBS_MSISDN_DATADUMP_PREPAID (SubscriberNo,BrandID,PaidMode,LifeCycleState,Gender) values  (:0,:1,:2,:3,:4)"

cur = con.cursor()
cur.execute(q)
records = []
print("Running....")
file = open("C:/Users/samrat.alam/Desktop/textfile.txt")
row = 0
for i in file.readlines():
    if (row == 0):
        row = row+1
        continue

    a = i.split("|")
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
