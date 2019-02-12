import os, sys, time

sys.path.append(os.getcwd())
from oracle_wrapper import *

#
# CONNECT TO DATABASE
#
tns = {
  'DPP' : {
    'user'    : '',
    'pwd'     : '',
    'server'  : '',
    'sid'     : '',
    'service' : '',
  },
}
db = Oracle(tns[sys.argv[1]])

#
# EXECUTE QUERY
#
db.execute("ALTER SESSION SET PLSQL_OPTIMIZE_LEVEL = 1");
db.execute("ALTER SESSION SET PLSQL_CODE_TYPE = 'INTERPRETED'");
db.execute("ALTER SESSION SET PLSCOPE_SETTINGS = 'IDENTIFIERS:ALL'");

#
# FETCH QUERY RESULTS
#
data, cols, desc = db.fetch('SELECT * FROM error_log ORDER BY error_id DESC', limit = 10)
print(cols, '\n')
for row in data:
  print(row[cols.msg_id], row[cols.msg_type], row[cols.loc], row[cols.msg])
print()

data, cols, desc = db.fetch('SELECT * FROM session_context ORDER BY attribute')
for row in data:
  print(row[cols.attribute].ljust(16), row[cols.value])
print()

