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
parser.add_argument('-csv',             help = 'Export tables in data/ dor to CSV files',   nargs = '?', default = False, const = True)
parser.add_argument('-verbose',         help = 'Show object names during export',           nargs = '?', default = False, const = True)
parser.add_argument('-debug',           help = '',                                          nargs = '?', default = False, const = True)
parser.add_argument('-patch',           help = 'Prepare patch',                             nargs = '?', default = False, const = True)
parser.add_argument('-rollout',         help = 'Mark rollout as done',                      nargs = '?', default = False, const = True)
parser.add_argument('-zip',             help = 'Patch as ZIP',                              nargs = '?', default = False, const = True)
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

# current dir
root          = os.path.dirname(os.path.realpath(__file__))
conn_dir      = root + '/conn'
rollout_dir   = git_target + '../patches'
rollout_done  = git_target + '../patches_done'
rolldirs      = ['41_sequences', '42_functions', '43_procedures', '45_views', '44_packages', '48_triggers', '49_indexes']
rolldir_obj   = rollout_dir + '/40_objects---LIVE'
rolldir_man   = rollout_dir + '/20_diffs---MANUALLY'
rolldir_apex  = rollout_dir + '/90_apex_app---LIVE'
today         = datetime.datetime.today().strftime('%Y-%m-%d')
rollout_log   = '{}/{}'.format(rollout_done, 'rollout.log')
patch_file    = '{}/{}.sql'.format(rollout_done, today)
zip_file      = '{}/{}.zip'.format(rollout_done, today)

# primary connection file
db_conf = args['target'] + 'documentation/db.conf'



#
# CONNECT TO DATABASE
#
start = timeit.default_timer()
if args['name']:
  db_conf = '{}/{}.conf'.format(conn_dir, args['name'])
#
common  = os.path.commonprefix([db_conf, git_target])
conn    = None
with open(db_conf, 'rb') as f:
  conn_bak  = pickle.load(f)
  conn      = Oracle(conn_bak)

# find wallet
wallet_file = ''
if 'name' in conn_bak:
  wallet_file = '{}/Wallet_{}.zip'.format(os.path.abspath(os.path.dirname(db_conf)), conn_bak['name'])
  if not os.path.exists(wallet_file):
    wallet_file = '{}/Wallet_{}.zip'.format(os.path.abspath(conn_dir), conn_bak['name'])
    if not os.path.exists(wallet_file):
      wallet_file = ''
#
print('CONNECTING:')
print('-----------')
print('    SOURCE | {}@{}/{}{}'.format(
  conn_bak['user'],
  conn_bak.get('host', ''),
  conn_bak.get('service', ''),
  conn_bak.get('sid', '')))
#
if wallet_file != '':
  print('    WALLET | {}'.format(conn_bak['wallet'].replace(common, '~ ')))
#
print('           | {}'.format(db_conf.replace(common, '~ ')))
print('    TARGET | {}'.format(git_target.replace(common, '~ ')))
print()



#
# PREVIEW OBJECTS
#
print('OBJECTS OVERVIEW:                            CONSTRAINTS:')
print('-----------------                            ------------')
data_objects = conn.fetch_assoc(query_objects, object_type = args['type'].upper(), recent = args['recent'])
summary = {}
for row in data_objects:
  if not (row.object_type) in summary:
    summary[row.object_type] = 0
  summary[row.object_type] += 1
#
all_objects = conn.fetch_assoc(query_summary)
for row in all_objects:
  print('{:>20} | {:>4} | {:>6} {:>12}{}{:>4}'.format(row.object_type, summary.get(row.object_type, ''), row.object_count, row.constraint_type or '', ' | ' if row.constraint_type else '', row.constraint_count or ''))
#
if args['recent'] == None or int(args['recent']) > 0:
  print('                          ^')  # to highlight affected objects
print()



#
# EXPORT OBJECTS
#
if args['recent'] == None or int(args['recent']) > 0:
  print('EXPORTING OBJECTS: ({}){}'.format(len(data_objects), '\n------------------' if args['verbose'] else ''))
  #
  recent_type = ''
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
      print('{:>20} | {:<30} {:>8}'.format(object_type if object_type != recent_type else '', object_name, len(obj)))
      recent_type = object_type
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
  print('    FOLDER |', apex_dir.replace(common, '~ '))
  #
  content = ''
  if wallet_file != '' and 'wallet' in conn_bak:
    content += 'set cloudconfig {}\n'.format(wallet_file)
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

# get old hashes
hashed_old = {}
f = open(rollout_log, 'r')
for line in f.readlines():
  (file, hash) = line.split('|')
  hashed_old[file.strip()] = hash.strip()



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
  print('PREPARING PATCH:', patch_file.replace(root, ''), '+ .zip' if args['zip'] else '')
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
  for dir in rolldirs:
    out_file    = '{}/{}.sql'.format(rolldir_obj, dir)
    files_mask  = '{}{}/*.sql'.format(git_target, re.sub('\d+[_]', '', dir))
    files       = sorted(glob.glob(files_mask))
    #
    if not (os.path.isdir(os.path.dirname(out_file))):
      os.makedirs(os.path.dirname(out_file))
    #
    with open(out_file, 'wb') as z:
      for file in files:
        hash = hashlib.md5(open(file, 'rb').read()).hexdigest()
        hash_old = hashed_old.get(file.replace(git_target, ''), '')

        # add only changed objects
        if hash != hash_old:
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
  if args['zip']:
    with zipfile.ZipFile(zip_file, 'w') as myzip:
      myzip.write(patch_file)



#
# PREVIEW/CONFIRM ROLLOUT
#
if (args['rollout'] or args['patch']):
  if args['rollout']:
    print('ROLLOUT CONFIRMED:')
    print('------------------')
  else:
    print('ROLLOUT PREVIEW: (files changed since last rollout)')
    print('----------------')

  # go thru existing files
  diff = {}
  hashed = []
  for (type, path) in folders.items():
    # skip some folders
    if type in ('APEX', 'DATA', 'PACKAGE BODY'):
      continue
    #
    files = glob.glob(path + '*.sql')
    for file in files:
      # calculate file hash
      hash = hashlib.md5(open(file, 'rb').read()).hexdigest()
      file = file.replace(git_target, '')
      hashed.append('{:<45} | {}'.format(file, hash))

      # show differences
      hash_old = hashed_old.get(file, '')
      if hash != hash_old:
        type = [k for k, v in folders.items() if v == git_target + os.path.dirname(file) + '/'][0]
        if not (type in diff):
          diff[type] = []
        diff[type].append([file, hash_old])

  # show differences
  for type, files in diff.items():
    for i, (file, hash) in enumerate(sorted(files)):
      flag = '<- CHECK' if (type == 'TABLE' and hash != '') else ''
      print('{:>20} | {:<36}{}'.format(type if i == 0 else '', file.split('/')[1], flag))

  # store hashes for next patch
  if args['rollout']:
    with open(rollout_log, 'w') as h:
      h.write('\n'.join(sorted(hashed)))
  #
  print()

print('TIME:', round(timeit.default_timer() - start, 2))
print('\n')


