import sys, re
from export_queries import *



def get_object(conn, object_type, object_name):
  # get object from database
  if object_type == 'JOB':
    desc = conn.fetch(query_describe_job, object_name = object_name)
  else:
    desc = conn.fetch(query_describe_object, object_type = object_type, object_name = object_name)
  #
  return re.sub('\t', '    ', str(desc[0][0]).strip())  # replace tabs with 4 spaces



def replace(subject, pattern, replacement, flags = 0):
  return re.compile(pattern, flags).sub(replacement, subject)



def get_lines(obj):
  lines = [s.rstrip() for s in obj.split('\n')]
  lines[0] = lines[0].lstrip()
  #
  return lines



def fix_simple_name(obj):
  obj = replace(obj, '("[A-Z0-9_$#]+")\.', '')
  obj = re.sub(r'"([A-Z0-9_$#]+)"', lambda x : x.group(1).lower(), obj)
  #
  return obj



def clean_table(lines):
  lines[0] = fix_simple_name(lines[0]) + ' ('
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
      lines[i] = lines[i].replace('(PARTITION', '(\n    PARTITION')
      lines[i] = lines[i].replace('" )', '"\n)')

  # fix column alignment
  lines = '\n'.join(lines).split('\n')
  for (i, line) in enumerate(lines):
    # fic column name
    if line.startswith('"'):
      columns = lines[i].strip().split(' ', 3)
      lines[i] = '    {:<30}  {:<16}{}'.format(
        fix_simple_name(columns[0]),
        columns[1].replace('NUMBER(*,0)', 'INTEGER') if len(columns) > 1 else '',
        fix_simple_name(' '.join(columns[2:])) if len(columns) > 2 else ''
      ).rstrip()
    #
    if line.startswith(')  DEFAULT COLLATION "USING_NLS_COMP" PCTFREE'):
      lines[i] = ')'
    #
    if line.startswith('TABLESPACE') or\
      line.startswith('PCTFREE') or\
      line.startswith('NOCOMPRESS LOGGING'):
      lines[i] = ''
    #
    if line.startswith('CONSTRAINT'):
      lines[i] = '    --\n    ' + fix_simple_name(lines[i])
      lines[i] = lines[i].replace(' CHECK (', '\n        CHECK (')
      lines[i] = lines[i].replace(' PRIMARY KEY (', '\n        PRIMARY KEY (')
      lines[i] = lines[i].replace(' FOREIGN KEY (', '\n        FOREIGN KEY (')
      lines[i] = lines[i].replace(' UNIQUE (', '\n        UNIQUE (')
    #
    if line.startswith('REFERENCES'):
      lines[i] = '        ' + fix_simple_name(lines[i])

  # remove empty lines
  lines = list(filter(None, lines))
  lines[len(lines) - 1] += ';'
  return lines



def clean_view(lines):
  lines[0] = lines[0].replace(' DEFAULT COLLATION "USING_NLS_COMP"', '')
  lines[0] = lines[0].replace(' EDITIONABLE', '')
  lines[0] = replace(lines[0], r'\s*\([^)]+\)\s*AS', ' AS')                 # remove columns
  lines[0] = fix_simple_name(lines[0])
  lines[1] = lines[1].lstrip()
  lines[len(lines) - 1] += ';'
  #
  # @TODO: add comments (view + columns) -> might not execute if view is not valid
  #
  return lines



