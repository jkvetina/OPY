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
db = Oracle(tns)
```

#### Connect to database with Service Name
```python
tns = {
  'user'    : 'USER_NAME',
  'pwd'     : 'PASSWORD',
  'host'    : 'SERVER_NAME',
  'service' : 'SERVICE_NAME',
}
db = Oracle(tns)
```


## Queries

#### Execute query (without fetching results)
```python
db.execute("ALTER SESSION SET PLSQL_OPTIMIZE_LEVEL = 1");  # cant bind values to DDL queries
db.execute("ALTER SESSION SET PLSQL_CODE_TYPE = 'INTERPRETED'");
db.execute("ALTER SESSION SET PLSCOPE_SETTINGS = 'IDENTIFIERS:ALL'");
```

#### Binding variables (also avaiable in fetch)
```python
db.execute("BEGIN ctx.player_id(player.get_id(:player_name)); END;", player_name = 'DOBBY')
db.commit();
```

#### Fetch query results
```python
data, cols = db.fetch('SELECT * FROM session_context ORDER BY attribute')
print(cols, '\n')
for row in data:
  print(row[cols.attribute].ljust(16), row[cols.value])
print()
```

#### Limit output lines
```python
data, cols = db.fetch('SELECT * FROM session_context ORDER BY attribute', limit = 100)
```
