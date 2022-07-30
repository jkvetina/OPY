# Oracle wrapper for Python

## Exporting database objects and applications

- execute queries in Oracle database
- export Oracle database objects
- export APEX applications

Read this for more info:
http://www.oneoracledeveloper.com/2022/07/database-versioning-with-data-apex-apps.html

<br />

### Typical commands

#### Exporting

Export database objects since today midnight
python export.py -n PROJECT -r 1

Export database objects since yesteray midnight
python export.py -n PROJECT -r 2

Export database objects since tomorrow (to get objects overview, but dont export anything)
python export.py -n PROJECT -r 0

Export all database objects
python export.py -n PROJECT

Export all tables (or other object types - PACKAGE, VIEW...)
python export.py -n PROJECT -t TABLE

Export all tables changed today
python export.py -n PROJECT -t TABLE -r 1

Export data from all tables listed in data/ as CSV files
python export.py -n PROJECT -csv

Export all database objects and APEX application 100
python export.py -n PROJECT -a 100

Export just the APEX application 100
python export.py -n PROJECT -r 0 -a 100

Export APEX application 100, but just changes made today
python export.py -n PROJECT -r 1 -a 100

#### Patching

Create patch from changed files (against rollout.log)
python export.py -n PROJECT -patch

Create patch as a feature branch from changed files (against rollout.log)
python export.py -n PROJECT -feature

Mark recently created patch as executed (merge files in patch to rollout.log)
python export.py -n PROJECT -rollout

<br />

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

