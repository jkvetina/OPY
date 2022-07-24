import sys, re, traceback
from export_queries import *



def get_object(conn, object_type, object_name):
  try:
    # get object from database
    if object_type == 'JOB':
      desc = conn.fetch(query_describe_job, object_name = object_name)
    else:
      desc = conn.fetch(query_describe_object, object_type = object_type, object_name = object_name)
    #
    if len(desc) > 0:
      return re.sub('\t', '    ', str(desc[0][0]).strip())  # replace tabs with 4 spaces
    return
  except Exception:
    print()
    print('#')
    print('# OBJECT_EXPORT_FAILED:', object_type, object_name)
    print('#')
    print(traceback.format_exc())
    print(sys.exc_info()[2])



def get_object_comments(conn, object_name):
  try:
    lines = ['\n--']
    data = conn.fetch_assoc(query_table_comments, table_name = object_name)
    for row in data:
      lines.append('COMMENT ON TABLE {} IS \'{}\';'.format(object_name.lower(), (row.comments or '').replace('\'', '')))
    #
    data = conn.fetch_assoc(query_column_comments, table_name = object_name)
    if len(data) > 0:
      lines.append('--')
    for row in data:
      lines.append('COMMENT ON COLUMN {} IS \'{}\';'.format(row.column_name, (row.comments or '').replace('\'', '')))
    #
    return '\n'.join(lines)
  except Exception:
    print()
    print('#')
    print('# OBJECT_COMMENT_FAILED:', object_name)
    print('#')
    print(traceback.format_exc())
    print(sys.exc_info()[2])



def replace(subject, pattern, replacement, flags = 0):
  return re.compile(pattern, flags).sub(replacement, subject)



def get_lines(obj):
  if obj == None:
    return []
  #
  lines = [s.rstrip() for s in obj.split('\n')]
  lines[0] = lines[0].lstrip()
  #
  return lines



def fix_simple_name(obj, schema):
  obj = obj.replace('"{}".'.format(schema), '')  # remove current schema
  #obj = replace(obj, '("[A-Z0-9_$#]+")\.', '')
  obj = re.sub(r'"([A-Z0-9_$#]+)"', lambda x : x.group(1).lower(), obj)
  #
  return obj



def fix_next_sequence(obj):
  obj = re.sub(r'"([A-Z0-9_$#]+)"\."([A-Z0-9_$#]+)"."NEXTVAL"', lambda x : x.group(2).lower() + '.NEXTVAL', obj)
  obj = re.sub(r'"([A-Z0-9_$#]+)"', lambda x : x.group(1).lower(), obj)
  #
  return obj



def clean_table(lines, schema):
  lines[0] = fix_simple_name(lines[0], schema) + ' ('
  lines[1] = lines[1].lstrip().lstrip('(').lstrip()  # fix fisrt column

  # throw away some distrators
  for (i, line) in enumerate(lines):
    if line.startswith('  STORAGE') or\
      line.startswith('  PCTINCREASE') or\
      line.startswith('  BUFFER_POOL') or\
      line.startswith('  USING'):
      lines[i] = ''
    else:
      lines[i] = lines[i].replace(' ENABLE', '').strip()
      lines[i] = lines[i].replace(' COLLATE "USING_NLS_COMP"', '')
      lines[i] = lines[i].replace(' DEFAULT COLLATION "USING_NLS_COMP"', '')
      lines[i] = lines[i].replace(' SEGMENT CREATION IMMEDIATE', '')
      lines[i] = lines[i].replace(' SEGMENT CREATION DEFERRED', '')
      lines[i] = lines[i].replace('(PARTITION', '(\n    PARTITION')
      lines[i] = lines[i].replace('" )', '"\n)')
      lines[i] = lines[i].replace('TIMESTAMP\' ', 'TIMESTAMP \'')

  # fix column alignment
  lines = '\n'.join(lines).split('\n')
  for (i, line) in enumerate(lines):
    # fic column name
    if line.startswith('"'):
      columns = lines[i].replace(' (', '(').strip().split(' ', 3)

      # fix constraint name or NEXTVAL sequence as default
      extra   = ' '.join(columns[2:])
      if extra.startswith('DEFAULT'):
        extra = fix_next_sequence(extra)
      elif extra.startswith('CONSTRAINT'):
        extra = fix_simple_name(extra, schema)

      # format line
      lines[i] = '    {:<30}  {:<16}{}'.format(
        fix_simple_name(columns[0], schema),
        columns[1].replace('NUMBER(*,0)', 'INTEGER') if len(columns) > 1 else '',
        extra if len(columns) > 2 else ''
      ).rstrip()
    #
    if line.startswith('PARTITION'):
      lines[i] = fix_simple_name(lines[i], schema)
    #
    if line.startswith(')  PCTFREE') or line.startswith(') PCTFREE'):
      lines[i] = ')'
    #
    if line.startswith(')  DEFAULT COLLATION "USING_NLS_COMP" PCTFREE'):
      lines[i] = ')'
    #
    if line.startswith('NOCACHE'):
      lines[i] = ''
    #
    if line.startswith('TABLESPACE') or\
      line.startswith('PCTFREE') or\
      line.startswith('PCTTHRESHOLD') or\
      line.startswith('NOCOMPRESS LOGGING') or\
      line.startswith('NOCACHE LOGGING') or\
      line.startswith('CACHE') or\
      line.startswith('STORAGE IN') or\
      line.startswith('LOB ("'):
      lines[i] = ''
    #
    if line.startswith('CONSTRAINT'):
      lines[i] = '    --\n    ' + fix_simple_name(lines[i], schema)
      lines[i] = lines[i].replace(' CHECK (', '\n        CHECK (')
      lines[i] = lines[i].replace(' PRIMARY KEY (', '\n        PRIMARY KEY (')
      lines[i] = lines[i].replace(' FOREIGN KEY (', '\n        FOREIGN KEY (')
      lines[i] = lines[i].replace(' UNIQUE (', '\n        UNIQUE (')
    #
    if line.startswith('REFERENCES'):
      lines[i] = '        ' + fix_simple_name(lines[i], schema)
    #
    lines[i] = lines[i].replace(' DEFERRABLE', '\n        DEFERRABLE')

    # fix some strange PK/index combinations
    if i > 1 and lines[i].startswith('CREATE'):
      lines[i] = fix_simple_name(lines[i], schema).rstrip() + ';'
      lines[i - 1] += ';'  # fix new lines later
    #
    if i > 1 and lines[i].startswith('ALTER TABLE'):
      lines[i] = fix_simple_name(lines[i], schema)

    # fix nameless keys
    if lines[i].startswith('PRIMARY KEY') or lines[i].startswith('FOREIGN KEY') or lines[i].startswith('UNIQUE') or lines[i].startswith('CHECK'):
      lines[i] = '    --\n    ' + fix_simple_name(lines[i], schema)
      #
      #print()
      #print('  NAMELESS CONSTRAINT', lines[i])
      #print()

    # fix XMLTYPE
    if lines[i].startswith('XMLTYPE'):
      lines[i] = ''

    # fix IOT tables
    if lines[i].startswith('ORGANIZATION INDEX'):
      lines[i] = 'ORGANIZATION INDEX'

    # fix temp tables
    if lines[i].startswith(') ON COMMIT'):
      lines[i] = lines[i].replace(') ON COMMIT', ')\nON COMMIT')

  # remove empty lines
  lines = list(filter(None, lines))
  lines = '\n'.join(lines)

  # fix missing ;
  lines = lines.replace('\n)\n;\nCREATE', '\n);\n--\nCREATE')  # fix new lines

  # fix missing comma
  lines = lines.replace(')\n    --', '),\n    --')

  # return as array
  lines = lines.split('\n')
  lines[len(lines) - 1] = lines[len(lines) - 1].rstrip() + ';'
  return lines



def clean_view(lines, schema):
  lines[0] = lines[0].replace(' DEFAULT COLLATION "USING_NLS_COMP"', '')
  lines[0] = lines[0].replace(' EDITIONABLE', '')
  lines[0] = replace(lines[0], r'\s*\([^)]+\)\s*AS', ' AS')                 # remove columns
  lines[0] = fix_simple_name(lines[0], schema)
  lines[1] = lines[1].lstrip()
  lines[len(lines) - 1] += ';'
  #
  return lines



def clean_materialized_view(lines, schema):
  lines[0] = replace(lines[0], r'\s*\([^)]+\)', '')                         # remove columns
  lines[0] = fix_simple_name(lines[0], schema)
  lines[0] = lines[0].replace('CREATE', '-- DROP') + ';\n' + lines[0]

  # found query start
  splitter = 0
  for (i, line) in enumerate(lines):
    # search for line where real query starts
    if line.startswith('  AS '):
      lines[i] = line.replace('  AS ', 'AS\n')
      splitter = i
      break

    # throw away some distrators
    if line.startswith(' NOCOMPRESS') or\
      line.startswith('  DEFAULT COLLATION') or\
      line.startswith('  ORGANIZATION') or\
      line.startswith('  STORAGE') or\
      line.startswith('  TABLESPACE') or\
      line.startswith('  PCTINCREASE') or\
      line.startswith('  BUFFER_POOL') or\
      line.startswith('  USING'):
      lines[i] = ''
    else:
      lines[i] = lines[i].lstrip()

  # remove empty lines
  lines[len(lines) - 1] += ';'
  lines = list(filter(None, lines[0:splitter])) + lines[splitter:]
  #
  return lines



def clean_package(lines, schema):
  lines = clean_procedure(lines, schema)

  # remove body
  for (i, line) in enumerate(lines):
    if line.replace(' EDITIONABLE', '').startswith('CREATE OR REPLACE PACKAGE BODY'):
      lines = lines[0:i]
      lines[len(lines) - 1] += '\n/'
      break
  #
  return lines



def clean_package_body(lines, schema):
  return clean_procedure(lines, schema)



def clean_procedure(lines, schema):
  lines[0] = fix_simple_name(lines[0], schema)
  lines[0] = lines[0].replace(' EDITIONABLE', '')
  lines[len(lines) - 1] += '\n/'
  return lines



def clean_function(lines, schema):
  return clean_procedure(lines, schema)



def clean_sequence(lines, schema):
  lines[0] = lines[0].replace(' MAXVALUE 9999999999999999999999999999', '')
  lines[0] = lines[0].replace(' INCREMENT BY 1', '')
  lines[0] = lines[0].replace(' NOORDER', '')
  lines[0] = lines[0].replace(' NOCYCLE', '')
  lines[0] = lines[0].replace(' NOKEEP', '')
  lines[0] = lines[0].replace(' NOSCALE', '')
  lines[0] = lines[0].replace(' GLOBAL', '')
  lines[0] = lines[0].replace(' GLOBAL', '')
  #
  lines[0] = fix_simple_name(lines[0], schema)
  lines[0] = replace(lines[0], '\s+', ' ').strip() + ';'
  #
  lines[0] = lines[0].replace(' MINVALUE', '\n    MINVALUE')
  lines[0] = lines[0].replace(' START', '\n    START')
  lines[0] = lines[0].replace(' CACHE', '\n    CACHE')
  #
  lines = '\n'.join(lines).split('\n')
  lines[0] = lines[0].replace('CREATE', '-- DROP') + ';\n' + lines[0]
  #
  for (i, line) in enumerate(lines):
    if line.startswith('    START WITH'):
      lines[i] = ''
  #
  lines = list(filter(None, lines))
  return lines



def clean_trigger(lines, schema):
  lines[0] = fix_simple_name(lines[0], schema)
  lines[0] = lines[0].replace(' EDITIONABLE', '')

  # fix enable/disable trigger
  found_slash = False
  for (i, line) in enumerate(lines):
    if line.startswith('ALTER TRIGGER'):
      lines[i] = replace(line, 'ALTER TRIGGER "[^"]+"."[^"]+" ENABLE', '');
      if '" DISABLE' in line:
        lines[i] = fix_simple_name(line.replace(' DISABLE', ' DISABLE;'), schema);
        lines[i - 1] = '/\n';
        found_slash = True

  # fix missing slash
  if not found_slash:
    if len(lines[len(lines) - 2]) == 0:
      lines[len(lines) - 2] = '/';
    else:
      lines[len(lines) - 1] = '/';
  #
  return lines



def clean_index(lines, schema):
  for (i, line) in enumerate(lines):
    # throw away some distrators
    if line.startswith('  STORAGE') or\
      line.startswith('  PCTFREE') or\
      line.startswith('  PCTINCREASE') or\
      line.startswith('  BUFFER_POOL'):
      lines[i] = ''
    else:
      lines[i] = lines[i].lstrip()
      lines[i] = lines[i].replace('TABLESPACE', '    COMPUTE STATISTICS\n    TABLESPACE')
  #
  lines[0] = fix_simple_name(lines[0], schema).replace(' ON ', '\n    ON ')
  lines = list(filter(None, lines))
  lines[len(lines) - 1] += ';'
  #
  return lines



def clean_synonym(lines, schema):
  lines[0] = lines[0].replace(' EDITIONABLE', '')
  lines[0] = fix_simple_name(lines[0], schema)
  lines[len(lines) - 1] += ';'
  #
  return lines



def clean_job(lines, schema):
  for (i, line) in enumerate(lines):
    #if line.startswith('sys.dbms_scheduler.set_attribute(') or\
    #  line.startswith('COMMIT;') or\
    #  line.startswith('END;'):
    #  lines[i] = ''
    if line.startswith('start_date=>'):
      lines[i] = replace(lines[i], r'start_date=>TO_TIMESTAMP_TZ[^)]*[)]', 'start_date=>SYSDATE')
    if line.lstrip().startswith('sys.dbms_scheduler.set_attribute(') and 'NLS_ENV' in line:
      lines[i] = ''
    if line.startswith(');'):
      lines = replace(' '.join(lines[2:i]), r'\s+', ' ')  # everything to 1 line
      lines = lines.replace('end_date=>NULL,', '')
      lines = lines.replace('job_class=>\'"DEFAULT_JOB_CLASS"\',', '')
      break
  #
  lines = ['job_name=>in_job_name,'] + replace(lines, r'\s*,\s*([a-z_]+)\s*=>\s*', r',\n\1=>').split('\n')
  for (i, line) in enumerate(lines):
    line = line.split('=>')
    line = '        {:<20}=> {}'.format(line[0], '=>'.join(line[1:]))
    lines[i] = line
  #
  return lines



def get_job_fixed(object_name, obj, conn):
  # fix priority and status
  data = conn.fetch_assoc(query_describe_job_details, job_name = object_name)
  job_priority  = data[0].job_priority
  job_enabled   = '--' if data[0].enabled == 'FALSE' else ''

  # fix arguments
  args = ''
  data = conn.fetch_assoc(query_describe_job_args, job_name = object_name)
  for row in data:
    kind  = 'position'
    name  = row.argument_position
    value = row.value
    #
    if row.argument_name:
      kind  = 'name'
      name = '\'{}\''.format(row.argument_name)
    #
    args += '\n    DBMS_SCHEDULER.SET_JOB_ARGUMENT_VALUE(in_job_name, argument_{} => {}, argument_value => {});'.format(kind, name, value)
  #
  if len(args) > 0:
    args += '\n    --'
  #
  return job_template.format(object_name, obj, args, job_priority, job_enabled)


