import cx_Oracle
# Test to see if the cx_Oracle is recognized
print(cx_Oracle.version)   # this returns 8.0.1 for me

# C:\Users\samrat.alam\Downloads\instantclient-basic-windows.x64-21.7.0.0.0dbru\instantclient_21_7
cx_Oracle.init_oracle_client(
    lib_dir=r"C:\Users\samrat.alam\Downloads\instantclient-basic-windows.x64-21.7.0.0.0dbru\instantclient_21_7")

cx_Oracle.clientversion()
