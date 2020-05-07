# coding: utf-8
import os, sys, pickle

sys.path.append(os.getcwd())
from oracle_wrapper import Oracle



# load settings
db_conf = [
  #target_dir + '/db.conf',
  os.path.dirname(os.path.realpath(__file__)) + '/db.conf'
]
tns = {}
for file in db_conf:
  try:
    with open(file, 'rb') as f:
      tns = pickle.load(f)
      print('--\n{}@{}/{}'.format(tns['user'], tns['host'], tns['sid']))
    break
  except IOError:
    pass



# connect to database
ora = Oracle(tns)
#
ora.execute("ALTER SESSION SET PLSQL_OPTIMIZE_LEVEL=1")
ora.execute("ALTER SESSION SET PLSQL_CODE_TYPE=INTERPRETED")
ora.execute("ALTER SESSION SET PLSCOPE_SETTINGS='IDENTIFIERS:ALL, STATEMENTS:ALL'")  # this screws recompile
ora.execute("ALTER SESSION SET PLSCOPE_SETTINGS='IDENTIFIERS:NONE'")  # this fix it
ora.execute("""
BEGIN
    DBMS_SESSION.SET_NLS('NLS_LANGUAGE',  '''ENGLISH''');
    DBMS_SESSION.SET_NLS('NLS_TERRITORY', '''CZECH REPUBLIC''');
END;""")



# fetch simple value
data = ora.fetch("SELECT COUNT(*) FROM DUAL")
print('COUNT =', data[0][0])  # data[row][col]
print()



# fetch multiple columns by name
data = ora.execute("BEGIN ctx.set_session(in_user_id => :user_id); END;", user_id = 'JKVETINA')
#
data = ora.fetch_assoc("""
SELECT namespace, attribute, value
FROM session_context
WHERE namespace LIKE :namespace
ORDER BY 1, 2
""",
  namespace = '%'
)
print(' | '.join(ora.cols))
for row in data:
  print(' | '.join(row))  # row.attribute.ljust(16), row.value
print()

