# Oracle wrapper for Python

## Connect to database

#### Connect to database with SID
```python
tns = {
  'user'    : 'USER_NAME',
  'pwd'     : 'PASSWORD',
  'host'    : 'SERVER_NAME',  # default port 1521
  'sid'     : 'SID',
}
ora = Oracle(tns)
```

#### Connect to database with Service Name
```python
tns = {
  'user'    : 'USER_NAME',
  'pwd'     : 'PASSWORD',
  'host'    : 'SERVER_NAME',
  'service' : 'SERVICE_NAME',
}
ora = Oracle(tns)
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
By default cx_Oracle will return rows as tuples without column names and for large sets of data it is not appropriate to convert these sets to dicts. Thats where cols.column_name trick comes in place. It just converts column_name to proper index. And you can still use indexes if you like.

```python
data, cols = ora.fetch('SELECT * FROM session_context ORDER BY attribute')
print(cols, '\n')
for row in data:
  print(row[cols.attribute].ljust(16), row[cols.value])
print()
```

#### Fetch query results with binded variables and limit rows
```python
data, cols = ora.fetch('SELECT * FROM session_context WHERE attribute LIKE :condition', condition = '%_ID', limit = 100)
print(cols, '\n')
for row in data:
  print(row[cols.attribute].ljust(16), row[cols.value])
print()
```

