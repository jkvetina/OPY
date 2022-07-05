# coding: utf-8
import sys, os, argparse, pickle, timeit, glob, csv, subprocess, datetime, shutil, zipfile, hashlib
from oracle_wrapper import Oracle
from export_fn import *

#
# ARGS
#
parser = argparse.ArgumentParser()
parser.add_argument('target',           help = 'Target folder (Git root)')
parser.add_argument('-n', '--name',     help = 'Connection name')
parser.add_argument('-t', '--type',     help = 'Filter specific object type', default = '')
parser.add_argument('-r', '--recent',   help = 'Filter objects compiled since SYSDATE - $recent')
parser.add_argument('-a', '--app',      help = 'APEX application')
parser.add_argument('-p', '--page',     help = 'APEX page')
parser.add_argument('-c', '--csv',      help = 'Export tables in data/ dor to CSV files', nargs = '?', default = False, const = True)
parser.add_argument('-v', '--verbose',  help = 'Show object names during export', nargs = '?', default = False, const = True)
parser.add_argument('-d', '--debug',    help = '', nargs = '?', default = False, const = True)
parser.add_argument('-z', '--patch',    help = 'Prepare patch', nargs = '?', default = False, const = True)
#
args = vars(parser.parse_args())

# check args
if args['debug']:
  print('ARGS:')
  print('-----')
  for (key, value) in args.items():
    if not (key in ('pwd', 'wallet_pwd')):
      print('{:>8} = {}'.format(key, value))
  print('')

# current dir
root          = os.path.dirname(os.path.realpath(__file__))
conn_dir      = '/conn'
rollout_dir   = root + '/patches'
rollout_done  = root + '/patches_done'
rolldirs      = ['41_sequences', '42_functions', '43_procedures', '45_views', '44_packages', '48_triggers', '49_indexes']
rolldir_obj   = rollout_dir + '/40_objects---LIVE'
rolldir_man   = rollout_dir + '/20_diffs---MANUALLY'
rolldir_apex  = rollout_dir + '/90_apex_app---LIVE'
today         = datetime.datetime.today().strftime('%Y-%m-%d')
rollout_log   = '{}/{}'.format(rollout_done, 'rollout.log')
patch_file    = '{}/{}.sql'.format(rollout_done, today)

# target folders by object types
git_target = args['target'] + '/database/'
folders = {
  'TABLE'             : git_target + 'tables/',
  'VIEW'              : git_target + 'views/',
  'MATERIALIZED VIEW' : git_target + 'mviews/',
  'TRIGGER'           : git_target + 'triggers/',
  'INDEX'             : git_target + 'indexes/',
  'SEQUENCE'          : git_target + 'sequences/',
  'PROCEDURE'         : git_target + 'procedures/',
  'FUNCTION'          : git_target + 'functions/',
  'PACKAGE'           : git_target + 'packages/',
  'PACKAGE BODY'      : git_target + 'packages/',
  'JOB'               : git_target + 'jobs/',
  'DATA'              : git_target + 'data/',
  'GRANT'             : git_target + 'grants/',
  'APEX'              : git_target + 'apex/',
}


#
# CONNECT TO DATABASE
#
start = timeit.default_timer()
db_conf = args['target'] + 'python/db.conf'
if args['name']:
  db_conf = '{}{}/{}.conf'.format(root, conn_dir, args['name'])
#
print('CONNECTING:')
print('-----------\n  {}\n'.format(db_conf))
conn = None
with open(db_conf, 'rb') as f:
  conn_bak  = pickle.load(f)
  conn      = Oracle(conn_bak)



#
# PREVIEW OBJECTS
#
if args['recent'] == None or int(args['recent']) > 0:
  print('OBJECTS PREVIEW:')
  print('----------------')
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
  print('                          ^')
  print('    CONSTRAINTS:')
  print('    ------------')
  data_constraints = conn.fetch_assoc(query_constraints)
  for row in data_constraints:
    print('{:>8} | {}'.format(row.constraint_type, row.count_))
  print()



#
# EXPORT OBJECTS
#
if args['recent'] == None or int(args['recent']) > 0:
  print('EXPORTING:', '\n----------' if args['verbose'] else '')
  for (i, row) in enumerate(data_objects):
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
    if args['verbose']:
      print('{:>20} | {:<30} {:>8}'.format(object_type, object_name, len(obj)))
    else:
      perc = (i + 1) / len(data_objects)
      dots = int(70 * perc)
      sys.stdout.write('\r' + ('.' * dots) + ' ' + str(int(perc * 100)) + '%')
      sys.stdout.flush()
    #
    lines = get_lines(obj)
    lines = getattr(sys.modules[__name__], 'clean_' + object_type.replace(' ', '_').lower())(lines)
    obj   = '\n'.join(lines)

    # append comments
    if object_type in ('TABLE', 'VIEW', 'MATERIALIZED VIEW'):
      obj += get_object_comments(conn, object_name)

    # fill in job template
    if object_type in ('JOB'):
      obj = get_job_fixed(object_name, obj, conn)

    # write object to file
    with open(file, 'w', encoding = 'utf-8') as h:
      h.write(obj + '\n\n')
  #
  if not args['verbose']:
    print()
  print()



#
# EXPORT DATA
#
if args['csv']:
  files = [os.path.splitext(os.path.basename(file))[0] for file in glob.glob(folders['DATA'] + '*.csv')]
  ignore_columns = ['updated_at', 'updated_by', 'created_at', 'created_by', 'calculated_at']
  #
  print('EXPORT TABLES DATA:', len(files))
  print('-------------------')
  #
  for table_name in sorted(files):
    try:
      table_exists = conn.fetch('SELECT * FROM {} WHERE ROWNUM = 1'.format(table_name))
    except:
      continue
    #
    file        = '{}{}.csv'.format(folders['DATA'], table_name)
    csv_file    = open(file, 'w')
    writer      = csv.writer(csv_file, delimiter = ';', lineterminator = '\n', quoting = csv.QUOTE_NONNUMERIC)
    columns     = [col for col in conn.cols if not (col in ignore_columns)]
    order_by    = ', '.join([str(i) for i in range(1, min(len(columns), 5) + 1)])
    data        = conn.fetch('SELECT {} FROM {} ORDER BY {}'.format(', '.join(columns), table_name, order_by))
    #
    writer.writerow(conn.cols)  # headers
    print('  {:30} {:>8}'.format(table_name, len(data)))
    for row in data:
      writer.writerow(row)
    csv_file.close()
  print()



#
# EXPORT APEX APP
#
apex_dir = folders['APEX']
apex_tmp = './export.tmp'
#
if 'app' in args and int(args['app'] or 0) > 0:
  print('EXPORTING APEX APP:')
  print('-------------------')
  print('       APP |', args['app'])
  print('    FOLDER |', apex_dir)
  #
  content = ''
  if 'wallet' in conn_bak:
    content += 'set cloudconfig ../../../conn/Wallet_{}.zip\n'.format(conn_bak['name'])
    content += 'connect {}/"{}"@{}\n'.format(conn_bak['user'], conn_bak['pwd'], conn_bak['service'])
  else:
    content += 'connect {}/"{}"@{}:{}/{}\n'.format(conn_bak['user'], conn_bak['pwd'], conn_bak['host'], conn_bak['port'], conn_bak['sid'])
  #
  content += 'apex export -applicationid {} -skipExportDate -expComments -expTranslations -split\n'.format(args['app'])
  content += 'apex export -applicationid {} -skipExportDate -expComments -expTranslations\n'.format(args['app'])
  #content  = 'apex export -applicationid {} -split -skipExportDate -expComments -expTranslations -expType APPLICATION_SOURCE,READABLE_YAML \n'
  #content  = 'apex export -applicationid {} -split -skipExportDate -expComments -expTranslations -expType READABLE_YAML \n'

  # change current dir so we have extracts in correct path
  if not os.path.exists(apex_dir):
    os.makedirs(apex_dir)
  os.chdir(apex_dir)
  #
  with open(apex_tmp, 'w+') as f:
    f.write(content)
    f.close()
  #
  process = 'sql /nolog < {}'.format(apex_tmp)
  result  = subprocess.run(process, shell = True, capture_output = True, text = True)
  output  = result.stdout.strip()
  print()

# remove temp file
os.chdir(root)
if os.path.exists(apex_dir + apex_tmp):
  os.remove(apex_dir + apex_tmp)



#
# REMOVE TIMESTAMPS FROM ALL APEX FILES
#
apex_dir = folders['APEX']
files = glob.glob(apex_dir + '/**/*.sql', recursive = True)
#
for file in files:
  content = ''
  with open(file, 'r') as h:
    content = h.read()
    content = re.sub(r",p_last_updated_by=>'([^']+)'", ",p_last_updated_by=>'DEV'", content)
    content = re.sub(r",p_last_upd_yyyymmddhh24miss=>'(\d+)'", ",p_last_upd_yyyymmddhh24miss=>'20220101000000'", content)
  #
  with open(file, 'w') as h:
    h.write(content)



#
# PREPARE PATCH
#
if args['patch']:
  print('PREPARING PATCH:', patch_file.replace(root, ''))
  print('----------------')
  #
  if os.path.exists(patch_file):
    os.remove(patch_file)
  #
  if not os.path.exists(rollout_dir):
    os.makedirs(rollout_dir)
  if not os.path.exists(rollout_done):
    os.makedirs(rollout_done)
  #
  manual_file = '{}/{}.sql'.format(rolldir_man, today)
  if not os.path.exists(manual_file):
    with open(manual_file, 'w') as f:
      f.write('')
  #
  for d in rolldirs:
    files_mask  = '{}/{}/*.sql'.format(git_target, re.sub('\d+[_]', '', d))
    files       = sorted(glob.glob(files_mask))
    out_file    = '{}/{}.sql'.format(rolldir_obj, d)
    #
    if not (os.path.isdir(os.path.dirname(out_file))):
      os.makedirs(os.path.dirname(out_file))
    #
    with open(out_file, 'wb') as z:
      for file in files:
        with open(file, 'rb') as h:
          z.write(h.read())
          z.write('/\n\n'.encode('utf-8'))

  # refresh current apps
  files = glob.glob(rolldir_apex + '/*.sql')
  for file in files:
    apex_file = folders['APEX'] + os.path.basename(file)
    if os.path.exists(apex_file):
      shutil.copyfile(apex_file, file)

  # join all files in a folder to a single file
  files = sorted(glob.glob(rollout_dir + '/**/*.sql', recursive = True))
  last_dir = ''
  for file in files:
    if not ('---SKIP.sql' in file):
      flag = ''
      if 'MANUALLY' in file and not (today in file):
        flag = ' <- CHECK'
      #
      with open(patch_file, 'ab') as z:
        #z.write((file + '\n').encode('utf-8'))
        with open(file, 'rb') as h:
          z.write(h.read())
          (curr_dir, short) = file.replace(rollout_dir, '').lstrip('/').split('/')
          print('    {:>20} | {:<24} {:>10}{}'.format(curr_dir if curr_dir != last_dir else '', short, os.path.getsize(file), flag))
          last_dir = curr_dir
  print('    {:<48}{:>10}'.format('', os.path.getsize(patch_file)))
  print()

  # create binary to whatever purpose
  with zipfile.ZipFile(patch_file + '.zip', 'w') as myzip:
    myzip.write(patch_file)
  print()

print('TIME:', round(timeit.default_timer() - start, 2))
print('\n')


