import sys, os, re, traceback, glob, csv, hashlib, datetime, collections
from export_queries import *



def get_file_details(object_type, object_name, file, cfg, hashed_old):
  obj = {
    'type'      : object_type or '',
    'name'      : object_name or '',
    'shortcut'  : '',
    'file'      : file or '',
    'folder'    : '',
    'group'     : '',
    'hash_old'  : '',
    'hash_new'  : ''
  }
  #
  if not (obj['type'] in cfg.folders):   # unsupported object type
    return {}

  # missing filename
  obj_folder = cfg.folders[obj['type']]
  if obj['file'] == '':
    obj['file'] = os.path.normpath(obj_folder[0] + obj['name'].lower() + obj_folder[1])
  obj['folder'] = obj_folder[0]

  # missing object name
  if obj['name'] == '':
    obj['name'] = os.path.basename(obj['file']).split('.')[0]
  obj['name'] = obj['name'].upper()

  # get short file use in all log files
  obj['shortcut'] = obj['file'].replace(cfg.git_root, '').replace('\\', '/').lstrip('/').strip()
  obj['hash_old'] = hashed_old.get(obj['shortcut'], '')
  obj['hash_new'] = ''

  # calculate new file hash
  if os.path.exists(obj['file']):
    obj['hash_new']  = hashlib.md5(open(obj['file'], 'rb').read()).hexdigest()
  #
  return collections.namedtuple('OBJ', obj.keys())(*obj.values())  # convert to named tuple



def get_files(object_type, cfg, sort):
  folder  = cfg.folders[object_type]
  files   = glob.glob(folder[0] + '/*' + folder[1])
  return files if not sort else sorted(files)



def get_fixed_path(value, root):
  if isinstance(value, str):
    if '#ROOT#' in value:
      value = value.replace('#ROOT#', root)
      if '\\' in value:
        value = value.replace('/', '\\')  # fix slashes for Windows
    #
    if '#TODAY#' in value:
      value = value.replace('#TODAY#', datetime.datetime.today().strftime('%Y-%m-%d'))  # YYYY-MM-DD
  return value



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



def clean_table(object_name, lines, schema):
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

    # remove auto sequence
    if ' AS IDENTITY' in lines[i]:
      lines[i] = lines[i].replace(' NOORDER', '')
      lines[i] = lines[i].replace(' NOCYCLE', '')
      lines[i] = lines[i].replace(' NOCACHE', '')
      lines[i] = lines[i].replace(' MINVALUE 1', '')
      lines[i] = lines[i].replace(' MAXVALUE 9999999999999999999999999999', '')
      lines[i] = lines[i].replace(' INCREMENT BY 1', '')
      lines[i] = lines[i].replace(' START WITH 1', '')
      lines[i] = lines[i].replace('  ', ' ').replace('  ', ' ')

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



def clean_view(object_name, lines, schema):
  lines[0] = lines[0].replace(' DEFAULT COLLATION "USING_NLS_COMP"', '')
  lines[0] = lines[0].replace(' EDITIONABLE', '')
  lines[0] = replace(lines[0], r'\s*\([^)]+\)\s*AS', ' AS')                 # remove columns
  lines[0] = replace(lines[0], r'\s*\([^)]+\)\s*BEQUEATH', ' BEQUEATH')     # remove columns
  lines[0] = lines[0].replace(' ()  AS', ' AS')                             # fix some views
  lines[0] = lines[0].replace('  ', ' ')
  lines[0] = fix_simple_name(lines[0], schema)
  lines[1] = lines[1].lstrip()

  # fix SELECT *, convert it to columns each on one line
  for (i, line) in enumerate(lines):
    #line = replace(line, r'([^\.]\.)?["]([^"]+)["](,?)', r'\n    \1\2\3')
    check_line = (replace(replace(line.strip(), 'SELECT\s*', '', re.I), '\s+FROM', '\nFROM', re.I) + '\n').split('\n')[0]
    if not (' ' in check_line):
      new_line = re.sub(r'"([A-Z0-9_$#]+)"', lambda x : x.group(1).lower(), line)   # fix uppercased names
      if new_line != line:
        line = replace(new_line, r'([^\.]\.)?([^,]+)(,?)', r'\n    \1\2\3')         # split to lines
        lines[i] = '    ' + line.lstrip()
        if '    SELECT ' in lines[i].upper():
          lines[i] = replace(lines[i], '    (SELECT) ', r'\1\n    ', re.I)  # fix SELECT t.* on same line
  #
  lines[len(lines) - 1] += ';'
  #
  return lines



def clean_materialized_view(object_name, lines, schema):
  lines[0] = replace(lines[0], r'\s*\([^)]+\)', '')                         # remove columns
  lines[0] = fix_simple_name(lines[0], schema)
  lines[0] = template_mvw_drop.lstrip().format(view_name = object_name) + lines[0]
  #lines[0] = lines[0].replace('CREATE', 'DROP') + ';\n--\n' + lines[0]

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



def clean_package(object_name, lines, schema):
  lines = clean_procedure(object_name, lines, schema)

  # remove body
  for (i, line) in enumerate(lines):
    if line.replace(' EDITIONABLE', '').startswith('CREATE OR REPLACE PACKAGE BODY'):
      lines = lines[0:i]
      lines[len(lines) - 1] += '\n/'
      break
  #
  return lines



def clean_package_body(object_name, lines, schema):
  return clean_procedure(object_name, lines, schema)



def clean_procedure(object_name, lines, schema):
  lines[0] = fix_simple_name(lines[0], schema)
  lines[0] = lines[0].replace(' EDITIONABLE', '')
  lines[len(lines) - 1] += '\n/'
  return lines



def clean_function(object_name, lines, schema):
  return clean_procedure(object_name, lines, schema)



def clean_sequence(object_name, lines, schema):
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



def clean_trigger(object_name, lines, schema):
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



def clean_index(object_name, lines, schema):
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



def clean_synonym(object_name, lines, schema):
  lines[0] = lines[0].replace(' EDITIONABLE', '')
  lines[0] = fix_simple_name(lines[0], schema)
  lines[len(lines) - 1] += ';'
  #
  return lines



def clean_job(object_name, lines, schema):
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



def clean_apex_files(app_id, folder, apex_replacements, default_authentication):
  # remove timestamps from all apex files (related to the exported app)
  path  = '{}f{}/**/*.sql'.format(folder, app_id)
  files = sorted(glob.glob(path, recursive = True))
  files.append(path.replace('/**/*', ''))   # add full export
  #
  for file in files:
    if os.path.exists(file):
      # get current file content
      old_content = ''
      with open(file, 'r') as h:
        old_content = h.read()

      # change page attributes to make changes in Git minimal
      new_content = old_content
      new_content = re.sub(r",p_last_updated_by=>'([^']+)'", ",p_last_updated_by=>'DEV'", new_content)
      new_content = re.sub(r",p_last_upd_yyyymmddhh24miss=>'(\d+)'", ",p_last_upd_yyyymmddhh24miss=>'20220101000000'", new_content)

      # replace default authentication
      if default_authentication > 0:
        new_content = re.sub(r",p_authentication_id=>wwv_flow_api.id[(]([\d]+)[)]", ',p_authentication_id=>wwv_flow_api.id({})'.format(default_authentication), new_content)

      # convert component id to names
      for (type, components) in apex_replacements.items():
        for (component_id, component_name) in components.items():
          search      = '.id({})\n'.format(component_id)
          new_content = new_content.replace(search, '{}  -- {}\n'.format(search.strip(), component_name))

      # store new content in the same file
      if new_content != old_content:  # close the file first
        with open(file, 'w') as z:
          z.write(new_content)



def get_merge_from_csv(csv_file, conn, skip_update, skip_delete):
  table_name  = os.path.basename(csv_file).split('.')[0].lower()
  columns     = []
  csv_select  = []
  all_rows    = []
  update_cols = []
  csv_rows    = 0

  # parse CSV file and create WITH table
  with open(csv_file, mode = 'r', encoding = 'utf-8') as csv_file:
    csv_reader = csv.DictReader(csv_file, delimiter = ';', lineterminator = '\n', quoting = csv.QUOTE_NONNUMERIC)
    for row in csv_reader:
      csv_rows += 1
      all_rows.append(row)
      #
      cols = []
      for col_name, col_value in row.items():
        if not isinstance(col_value, (int, float)):
          col_value = '\'{}\''.format(col_value.replace('\'', '\'\''))
        cols.append('{} AS {}'.format(col_value, col_name))
      csv_select.append('SELECT {} FROM DUAL'.format(', '.join(cols)))
      #
      if not len(columns):
        columns = list(row.keys())
  csv_select = ' UNION ALL\n    '.join(csv_select)

  # ignore empty files
  if csv_rows == 0:
    return

  # get primary key cols for merge
  primary_cols = conn.fetch_value(query_csv_primary_columns, table_name = table_name)
  if primary_cols == None:
    return
  #
  primary_cols = primary_cols.lower().split(',')
  primary_cols_set = []
  for col in primary_cols:
    primary_cols_set.append('t.{} = s.{}'.format(col, col))
  primary_cols_set = '\n    ' + '\n    AND '.join(primary_cols_set) + '\n'

  # get other columns
  for col in columns:
    if not (col in primary_cols):
      update_cols.append('t.{} = s.{}'.format(col, col))
  update_cols = ',\n{}        '.format(skip_update).join(update_cols)
  #
  all_cols    = 't.' + ',\n        t.'.join(columns)
  all_values  = 's.' + ',\n        s.'.join(columns)
  query       = template_csv_merge.lstrip().format (
    table_name            = table_name,
    primary_cols_set      = primary_cols_set,
    csv_content_query     = csv_select,
    non_primary_cols_set  = update_cols,
    all_cols              = all_cols,
    all_values            = all_values,
    skip_update           = skip_update,
    skip_delete           = skip_delete
  )
  #
  return query

