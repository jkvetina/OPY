# Oracle wrapper for Python

## Exporting database objects and applications

- execute queries in Oracle database
- export Oracle database objects
- export APEX applications

Read this for more info:
- http://www.oneoracledeveloper.com/2022/07/database-versioning-with-data-apex-apps.html
- http://www.oneoracledeveloper.com/2022/07/converting-csv-data-to-merge-statements.html
- http://www.oneoracledeveloper.com/2022/09/database-versioning-for-feature-branches.html

<br />

### Typical commands

#### Exporting database objects

Export database objects since today midnight\
```python export.py -n PROJECT -r 1```

Export database objects since today midnight and show each object name\
```python export.py -n PROJECT -r 1 -v```

Export database objects since today midnight and show flags against last PROD release\
```python export.py -n PROJECT -r 1 -v PROD```

Export database objects since yesteray midnight\
```python export.py -n PROJECT -r 2```

Export database objects since tomorrow (to get objects overview, but dont export anything)\
```python export.py -n PROJECT -r 0```

Export all database objects\
```python export.py -n PROJECT```

Export all tables (or other object types - PACKAGE, VIEW...)\
```python export.py -n PROJECT -t TABLE```

Export all tables changed today and show each table name\
```python export.py -n PROJECT -t TABLE -r 1 -v```

Export all object types starting with ABC\
```python export.py -n PROJECT -t % ABC%```

Export all object and show object status against latest PROD patch \
```python export.py -n PROJECT -e PROD```

#### Exporting CSV

Export data from all tables listed in data/ as CSV files\
```python export.py -n PROJECT -r 0 -csv```

Export data as CSV from table NAVIGATION\
```python export.py -n PROJECT -r 0 -csv NAVIGATION```

Export data as CSV from all tables starting with USER or ROLE\
```python export.py -n PROJECT -r 0 -csv USER% ROLE% -v```

Export data from all tables listed in data/ as CSV files and show more details\
```python export.py -n PROJECT -r 0 -csv -v```

Export data from all tables listed in data/ as CSV files and show flags against last PROD release\
```python export.py -n PROJECT -r 0 -csv -v PROD```

#### Exporting APEX

Export all database objects and APEX applications listed in apex/ folder\
```python export.py -n PROJECT -a```

Export all database objects and APEX applications 100, 110\
```python export.py -n PROJECT -a 100 110```

Export just the APEX application 100 without db objects\
```python export.py -n PROJECT -r 0 -a 100```

Export APEX application 100, and show changes made today\
```python export.py -n PROJECT -r 1 -a 100```

Export APEX application 100 with application and workspace files\
```python export.py -n PROJECT -a 100 -f```

#### Exporting locked objects

Mark all current files as locked (create locked.log, then export just these objects)\
```python export.py -n PROJECT -lock```

Add new objects compiled today, then run just -lock to actually add them to locked.log\
```python export.py -n PROJECT -add -r 1```

Add new objects compiled today starting with XXX\
```python export.py -n PROJECT -add XXX -r 1```

Allow to export new objects to files when -lock(ed)\
```python export.py -n PROJECT -lock -add```

Delete all files not listed on locked.log list\
```python export.py -n PROJECT -lock -delete```

Export only locked objects (when locked.log file exists) changed today\
```python export.py -n PROJECT -v -r 1```

Export only locked objects changed today and show flags against last PROD release\
```python export.py -n PROJECT -v PROD -r 1```

#### Patching

Create patch for PROD environment from changed files (against rollout.PROD.log)\
```python export.py -n PROJECT -patch PROD```

Create patch named as patches/CARD_NUMBER.sql for PROD environment...\
```python export.py -n PROJECT -patch PROD CARD_NUMBER```

Mark recently created PROD patch as executed (merge files in patch to rollout.PROD.log)\
```python export.py -n PROJECT -rollout PROD```

Mark recently created patch as executed + keep just existing files in log\
```python export.py -n PROJECT -rollout PROD -delete```

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

