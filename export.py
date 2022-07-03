# coding: utf-8
import sys, os, argparse, pickle, timeit
from oracle_wrapper import Oracle
from export_fn import *

# args
parser = argparse.ArgumentParser()
parser.add_argument('target',           help = 'Target folder (Git root)')
parser.add_argument('-n', '--name',     help = 'Connection name')
parser.add_argument('-t', '--type',     help = 'Filter specific object type', default = '')
parser.add_argument('-r', '--recent',   help = 'Filter objects compiled since SYSDATE - $recent')
parser.add_argument('-a', '--app',      help = 'APEX application')
parser.add_argument('-p', '--page',     help = 'APEX page')
parser.add_argument('-c', '--csv',      help = '', nargs = '?', default = False, const = True)
parser.add_argument('-v', '--verbose',  help = '', nargs = '?', default = False, const = True)
parser.add_argument('-d', '--debug',    help = '', nargs = '?', default = False, const = True)
#
args = vars(parser.parse_args())

# check args
if args['debug']:
  print('ARGS:\n-----')
  for (key, value) in args.items():
    if not (key in ('pwd', 'wallet_pwd')):
      print('{:>8} = {}'.format(key, value))
  print('')

# current dir
root = os.path.dirname(os.path.realpath(__file__))
conn_dir = '/conn'



# connect to database
db_conf = args['target'] + 'python/db.conf'
if args['name']:
  db_conf = '{}{}/{}.conf'.format(root, conn_dir, args['name'])
#
print('CONNECTING:\n-----------\n  {}\n'.format(db_conf))
conn = None
with open(db_conf, 'rb') as f:
  conn = Oracle(pickle.load(f))



# export objects
print('EXPORTING OBJECTS:\n------------------')
data_objects = conn.fetch_assoc(query_objects, object_type = args['type'].upper(), recent = args['recent'])
summary = {}
for row in data_objects:
  if not (row.object_type) in summary:
    summary[row.object_type] = 0
  summary[row.object_type] += 1
#
all_objects = conn.fetch_assoc(query_all_objects)
for row in all_objects:
  print('{:>20} | {:>4} | {:>4}'.format(row.object_type, summary.get(row.object_type, ''), row.count_))
print()
#
print('  CONSTRAINTS:\n  ------------')
data_constraints = conn.fetch_assoc(query_constraints)
for row in data_constraints:
  print('{:>8} | {}'.format(row.constraint_type, row.count_))
print()



# target folders by object types
target = args['target'] + '/database/'
folders = {
  'TABLE'             : target + 'tables/',
  'VIEW'              : target + 'views/',
  'MATERIALIZED VIEW' : target + 'mviews/',
  'TRIGGER'           : target + 'triggers/',
  'INDEX'             : target + 'indexes/',
  'SEQUENCE'          : target + 'sequences/',
  'PROCEDURE'         : target + 'procedures/',
  'FUNCTION'          : target + 'functions/',
  'PACKAGE'           : target + 'packages/',
  'PACKAGE BODY'      : target + 'packages/',
  'JOB'               : target + 'jobs/',
  'DATA'              : target + 'data/',
  'GRANT'             : target + 'grants/',
  'APEX'              : target + 'apex/',
}

# export objects
print('EXPORTING:\n----------')
start = timeit.default_timer()
for row in data_objects:
  object_type, object_name = row.object_type, row.object_name

  # make sure we have target folders ready
  folder = folders[object_type]
  if not (os.path.isdir(folder)):
    os.makedirs(folder)
  #
  extra = ''
  if object_type == 'PACKAGE':
    extra = '.spec'
  #
  obj   = get_object(conn, object_type, object_name)
  file  = '{}{}{}.sql'.format(folder, object_name.lower(), extra)
  #
  print('{:>20} | {:<30} {:>8}'.format(object_type, object_name, len(obj)))
  #
  lines = get_lines(obj)
  lines = getattr(sys.modules[__name__], 'clean_' + object_type.replace(' ', '_').lower())(lines)
  obj   = '\n'.join(lines) + '\n\n'

  # write object to file
  with open(file, 'w', encoding = 'utf-8') as h:
    h.write(obj)
print()
print('\nTIME:', round(timeit.default_timer() - start, 2))
print('\n')

