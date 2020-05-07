# Oracle wrapper for Python

## Connect to database

#### Connect to database with SID or service name
```python
ora = Oracle({
  'user'    : 'USER_NAME',
  'pwd'     : 'PASSWORD',
  'host'    : 'SERVER_NAME',  # default port 1521
  'sid'     : 'SID',
  #'service' : 'SERVICE_NAME',  # use sid or service_name
})
```


## Queries

#### Execute query (without fetching results)
```python
ora.execute("ALTER SESSION SET PLSQL_OPTIMIZE_LEVEL = 1");  # cant bind values to DDL queries
ora.execute("ALTER SESSION SET PLSQL_CODE_TYPE = 'INTERPRETED'");
ora.execute("ALTER SESSION SET PLSCOPE_SETTINGS = 'IDENTIFIERS:ALL'");
```

#### Binding variables (also avaiable in fetch)
```python
ora.execute("BEGIN ctx.player_id(player.get_id(:player_name)); END;", player_name = 'DOBBY')
ora.commit();
```

#### Fetch query results
You dont want to access columns by numeric indexes probably, except maybe when you do siple COUNT(\*).
By default cx_Oracle will return rows as tuples without column names and for large sets of data it is not appropriate to convert these sets to namedtuples.

```python
data = ora.fetch_assoc("""
SELECT attribute, value
FROM session_context
WHERE namespace LIKE :namespace
ORDER BY attribute
""",
  namespace = 'FGK',  # bind variables
  limit = 100         # limit number of output rows
)
for row in data:
  #print(row[0].ljust(16), row[1])  # access columns by their index
  print(row.attribute.ljust(16), row.value)  # access by column names
print()
```

