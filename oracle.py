import os, sys

sys.path.append(os.getcwd())
from oracle_wrapper import Oracle



#
# CONNECT TO DATABASE
#
ora = Oracle({
  'user'    : 'FGK',
  'pwd'     : 'fgk.',
  'host'    : '127.0.0.1',
  #'sid'     : 'orcl',
  'service' : 'orcl',
})

ora.execute("ALTER SESSION SET PLSQL_OPTIMIZE_LEVEL=1")
ora.execute("ALTER SESSION SET PLSQL_CODE_TYPE=INTERPRETED")
ora.execute("ALTER SESSION SET PLSCOPE_SETTINGS='IDENTIFIERS:ALL, STATEMENTS:ALL'")  # this screws recompile
ora.execute("ALTER SESSION SET PLSCOPE_SETTINGS='IDENTIFIERS:NONE'")  # this fix it

data, cols = ora.fetch("SELECT COUNT(*) FROM DUAL")
print(data[0][0])  # data[row][col]

data = ora.fetch_assoc("""
SELECT attribute, value
FROM session_context
WHERE namespace LIKE :namespace
ORDER BY attribute
""",
  namespace = 'FGK'
)
for row in data:
  print(row.attribute.ljust(16), row.value)
print()

