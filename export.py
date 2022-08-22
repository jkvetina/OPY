# coding: utf-8
import sys, os, argparse, pickle, timeit, traceback, glob, csv, subprocess, datetime, shutil, zipfile, collections
from oracle_wrapper import Oracle
from export_fn import *

#
# ARGS
#
parser = argparse.ArgumentParser()
parser.add_argument('-g', '-target',  '--target',   help = 'Target folder (Git root)')
parser.add_argument('-n', '-name',    '--name',     help = 'Connection name')
parser.add_argument('-r', '-recent',  '--recent',   help = 'Filter objects compiled recently',          type = int,   default = -1)
parser.add_argument('-t', '-type',    '--type',     help = 'Filter specific object type',                             default = '',     nargs = '?')
parser.add_argument('-a', '-apex',    '--apex',     help = 'APEX application(s) to export',             type = int,                     nargs = '*')
parser.add_argument('-c', '-csv',     '--csv',      help = 'Export tables in data/ to CSV files',                                       nargs = '*')
parser.add_argument('-v', '-verbose', '--verbose',  help = 'Show object names during export',                         default = False,  nargs = '?',  const = True)
parser.add_argument('-d', '-debug',   '--debug',    help = 'Show some extra stuff when debugging',                    default = False,  nargs = '?',  const = True)
parser.add_argument('-i', '-info',    '--info',     help = 'Show DB/APEX versions and app details',                   default = False,  nargs = '?',  const = True)
parser.add_argument('-p', '-patch',   '--patch',    help = 'Prepare patch',                                           default = False,  nargs = '?',  const = True)
parser.add_argument(      '-rollout', '--rollout',  help = 'Mark rollout as done',                                    default = False,  nargs = '?',  const = True)
parser.add_argument('-z', '-zip',     '--zip',      help = 'Patch as ZIP',                                            default = False,  nargs = '?',  const = True)
parser.add_argument(      '-delete',  '--delete',   help = 'Delete unchanged files (db objects only)',                default = False,  nargs = '?',  const = True)
parser.add_argument(      '-lock',    '--lock',     help = 'Updates only objects in the locked.log',                  default = False,  nargs = '?',  const = True)
#
args = vars(parser.parse_args())
args = collections.namedtuple('ARG', args.keys())(*args.values())  # convert to named tuple

# check args
if args.debug:
  print('ARGS:')
  print('-----')
  for key, value in sorted(zip(args._fields, args)):
    if not (key in ('pwd', 'wallet_pwd')):
      print('{:>10} = {}'.format(key, value))
  print('')



#
# FIND CONNECTION FILE
#
root        = os.path.dirname(os.path.realpath(__file__))
conn_dir    = os.path.abspath(root + '/conn')
conn_files  = []
#
if args.target != None:
  conn_files.append(args.target + '/documentation/db.conf')
elif args.name != None:
  conn_files.append(os.path.normpath('{}/{}.conf'.format(conn_dir, args.name)))
#
for db_conf in conn_files:
  if os.path.exists(db_conf):
    with open(db_conf, 'rb') as b:
      connection = pickle.load(b)
      if args.target == None and 'target' in connection:  # overwrite target from pickle file
        args = args._replace(target = connection['target'])
      break

# check target
if args.target == None:
  print('#')
  print('# UNKNOWN TARGET')
  print('#')
  print()
  sys.exit()



#
# SETUP VARIABLES
#

# target folders by object types
git_target  = os.path.abspath(args.target + '/database') + '/'
git_root    = os.path.normpath(git_target + '../')
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
  'SYNONYM'           : git_target + 'synonyms/',
  'JOB'               : git_target + 'jobs/',
  'DATA'              : git_target + 'data/',
  'GRANT'             : git_target + 'grants/',
  'APEX'              : git_target + 'apex/',
}
objects_sorted = [
  'SYNONYM', 'SEQUENCE', 'TABLE', 'INDEX',
  'MATERIALIZED VIEW', 'VIEW',
  'FUNCTION', 'PROCEDURE', 'PACKAGE', 'PACKAGE BODY',
  'TRIGGER', 'JOB'
]

# map objects to patch folders
patch_map = {
  'init'      : [],
  'tables'    : ['SEQUENCE', 'TABLE'],
  'objects'   : ['INDEX', 'MATERIALIZED VIEW', 'VIEW', 'TRIGGER', 'PROCEDURE', 'FUNCTION', 'PACKAGE', 'PACKAGE BODY', 'SYNONYM'],
  'data'      : ['DATA'],
  'grants'    : ['GRANT'],
  'jobs'      : ['JOB'],
}

# some variables
start_timer     = timeit.default_timer()
today_date      = datetime.datetime.today().strftime('%Y-%m-%d')  # YYYY-MM-DD
patch_root      = os.path.normpath(git_target + '../patches')
patch_done      = os.path.normpath(git_target + '../patches_done')
patch_today     = '{}/patch_{}.sql'.format(patch_done, today_date)
patch_zip       = '{}/patch_{}.zip'.format(patch_done, today_date)
patch_log       = '{}/{}'.format(patch_done, 'patch.log')
rollout_log     = '{}/{}'.format(patch_done, 'rollout.log')
locked_log      = '{}/{}'.format(patch_done, 'locked.log')
common_root     = os.path.commonprefix([db_conf, git_target]) or '\\//\\//\\//'
#
patch_folders = {
  'init'      : patch_root + '/10_init/',
  'tables'    : patch_root + '/20_new_tables/',
  'changes'   : patch_root + '/30_table+data_changes/',
  'objects'   : patch_root + '/40_repeatable_objects/',
  'cleanup'   : patch_root + '/50_cleanup/',
  'data'      : patch_root + '/60_data/',
  'grants'    : patch_root + '/70_grants/',
  'jobs'      : patch_root + '/80_jobs/',                 # after data
  'finally'   : patch_root + '/90_finally/',
}
patch_store     = ('changes')   # store hashes for files in these folders
patch_manually  = '{}{}.sql'.format(patch_folders['changes'], today_date)
file_ext_obj    = '.sql'
file_ext_csv    = '.csv'
file_ext_spec   = '.spec.sql'

# for CSV files dont export audit columns
ignore_columns = ['updated_at', 'updated_by', 'created_at', 'created_by']

# apex folders
apex_dir        = folders['APEX']
apex_temp_dir   = apex_dir + 'temp/'  # temp file for partial APEX exports
apex_ws_files   = apex_dir + 'workspace_files/'
apex_tmp        = 'apex.#.tmp'  # temp file for running SQLcl on Windows

# cleanup junk files created on Mac probably by iCloud sync
path = apex_dir + '**/* [0-9].*'
for file in glob.glob(path, recursive = True):
  os.remove(file)



#
# CONNECT TO DATABASE
#
curr_schema       = connection['user'].upper().split('[')[1].rstrip(']') if '[' in connection['user'] else connection['user'].upper()
grants_made_file  = '{}{}.sql'.format(folders['GRANT'], curr_schema)
grants_recd_file  = os.path.dirname(grants_made_file) + '/received/#' + file_ext_obj
#
if not args.rollout:
  try:
    conn = Oracle(connection)
  except Exception:
    print('#')
    print('# CONNECTION FAILED')
    print('#')
    print()
    print(traceback.format_exc().splitlines()[-1])
    #print(sys.exc_info()[2])
    sys.exit()
  #
  data      = conn.fetch_assoc(query_today, recent = args.recent if args.recent >= 0 else '')
  req_today = data[0].today  # calculate date from recent arg
  schema    = data[0].curr_user

  # find wallet
  wallet_file = ''
  if 'wallet' in connection:
    wallet_file = connection['wallet']
  elif 'name' in connection:
    wallet_file = '{}/Wallet_{}.zip'.format(os.path.abspath(os.path.dirname(db_conf)), connection['name'])
    if not os.path.exists(wallet_file):
      wallet_file = '{}/Wallet_{}.zip'.format(os.path.abspath(conn_dir), connection['name'])
      if not os.path.exists(wallet_file):
        wallet_file = ''
  #
  print('CONNECTING:')
  print('-----------')
  print('      SOURCE | {}@{}/{}{}'.format(
    connection['user'],
    connection.get('host', ''),
    connection.get('service', ''),
    connection.get('sid', '')))
  #
  if wallet_file != '':
    print('      WALLET | {}'.format(connection['wallet'].replace(common_root, '~ ')))
  #
  print('             | {}'.format(db_conf.replace(common_root, '~ ')))
  print('      TARGET | {}'.format(git_target.replace(common_root, '~ ')))
  print('             |')

  # get versions
  try:
    version_apex  = conn.fetch_value(query_version_apex)
    version_db    = conn.fetch_value(query_version_db)
  except Exception:
    version_apex  = version_apex or ''
    version_db    = conn.fetch_value(query_version_db_old)
  #
  print('    DATABASE | {}'.format('.'.join(version_db.split('.')[0:2])))
  print('        APEX | {}'.format('.'.join(version_apex.split('.')[0:2])))
  print()



#
# PREP FOLDERS AND GET OLD HASHES
#

# create basic dirs
for dir in [git_target, patch_root, patch_done]:
  if not os.path.exists(dir):
    os.makedirs(dir)
#
for (type, dir) in patch_folders.items():
  if not os.path.exists(dir):
    os.makedirs(dir)

# delete old empty patch files
for file in glob.glob(os.path.dirname(patch_manually) + '/*' + file_ext_obj):
  if os.path.getsize(file) == 0:
    os.remove(file)

# create new patch file for manual changes (ALTER statements, related data changes...)
if args.patch:
  if not os.path.exists(patch_manually):
    with open(patch_manually, 'w', encoding = 'utf-8') as w:
      w.write('')

# switch to alternative log file (typically from PROD when preparing new patch for PROD)
if args.patch and args.patch != True:
  rollout_log = rollout_log.replace('.', '.{}.'.format(args.patch))
  if not os.path.exists(rollout_log):
    print('#')
    print('# REQUESTED PATCH FILE MISSING')
    print('#', rollout_log)
    print('#')
    print()

# get old hashes
hashed_old = {}
hashed_new = {}   # files/objects changed since last rollout
#
if os.path.exists(rollout_log):
  with open(rollout_log, 'r', encoding = 'utf-8') as r:
    for line in r.readlines():
      (hash, file) = line.split('|')
      if '/' in hash:
        hash, file = file, hash  # swap columns for backward compatibility
      hashed_old[file.strip()] = hash.strip()



#
# PREP LOCKED FILES
#

# process just files in the locked.log file
locked_objects  = []
if os.path.exists(locked_log):
  with open(locked_log, 'r', encoding = 'utf-8') as r:
    # get list of locked objects
    for short_file in r.readlines():
      short_file = short_file.strip()
      if len(short_file) > 1 and not (short_file in locked_objects):
        locked_objects.append(short_file)

      # remove not existing files
      if args.delete:
        file = os.path.normpath(git_root + '/' + short_file)
        if not os.path.exists(file):
          print('REMOVING', short_file)
          locked_objects.remove(short_file)
#
if args.lock and not args.delete:
  # add all existing files to the locked log
  for type in objects_sorted:
    for file in sorted(glob.glob(folders[type] + '/*.*')):
      short_file, hash_old, hash_new = get_file_details(file, git_root, hashed_old)
      if not (short_file in locked_objects):
        locked_objects.append(short_file)



#
# PREVIEW OBJECTS
#
data_objects  = []
count_objects = 0
#
if args.recent != 0 and not args.patch and not args.rollout:
  print()
  print('OBJECTS OVERVIEW:                                      CONSTRAINTS:')
  print('-----------------                                      ------------')

  # retrieve objects in specific order
  sort = ''
  for (i, object_type) in enumerate(objects_sorted):
    sort += 'WHEN \'{}\' THEN {}'.format(object_type, i)
  #
  data_objects = conn.fetch_assoc(query_objects.format(sort), object_type = args.type.upper(), recent = args.recent if args.recent >= 0 else '')
  summary = {}
  for row in data_objects:
    if not (row.object_type) in summary:
      summary[row.object_type] = 0
    summary[row.object_type] += 1
    if row.object_type in folders:
      count_objects += 1
  #
  all_objects = conn.fetch_assoc(query_summary)
  print('                       EXPORTING |   TOTAL')
  for row in all_objects:
    if row.object_count:
      print('{:>20} | {:>9} | {:>7} {:<6} {:>12}{}{:>4}'.format(*[
        row.object_type,
        summary.get(row.object_type, ''),
        row.object_count,
        '' if row.object_type in folders else '[N/A]',
        row.constraint_type or '',
        ' | ' if row.constraint_type else '',
        row.constraint_count or ''
      ]))
    else:
      print('{:>62}{}{:>4}'.format(row.constraint_type or '', ' | ' if row.constraint_type else '', row.constraint_count or ''))
  print()
  print()



#
# EXPORT OBJECTS
#
if count_objects:
  if (len(locked_objects) or args.lock):
    count_objects = min(count_objects, len(locked_objects))
    print('EXPORTING LOCKED OBJECTS: ({})'.format(count_objects))
    if args.verbose:
      print('-------------------------')
  else:
    print('EXPORTING OBJECTS: ({})'.format(count_objects))
    if args.verbose:
      print('------------------')
  if args.verbose:
    print('{:54}{:>8} | {:>8}'.format('', 'LINES', 'BYTES'))
  #
  recent_type = ''
  for (i, row) in enumerate(data_objects):
    object_type, object_name = row.object_type, row.object_name

    # make sure we have target folders ready
    if not (object_type in folders):
      if args.debug:
        print('#')
        print('# OBJECT_TYPE_NOT_SUPPORTED:', object_type)
        print('#\n')
      continue
    #
    folder    = folders[object_type]
    file_ext  = file_ext_obj if object_type != 'PACKAGE' else file_ext_spec
    file      = '{}{}{}'.format(folder, object_name.lower(), file_ext)

    # prepare short_file before we even create the file
    short_file, hash_old, hash_new = get_file_details(file, git_root, hashed_old)

    # check locked objects
    flag = ''
    if (len(locked_objects) or args.lock):
      if not (short_file in locked_objects):
        if hash_old == '' and not args.lock:
          # add new files to the list
          #locked_objects.append(short_file)
          # get them, but dont add on locked.log list
          # you can either -delete them
          # or keep them with following -lock call
          flag = '[+]'
        else:
          continue  # skip files not on the list

    # make sure we have target folders ready
    if not (os.path.isdir(folder)):
      os.makedirs(folder)

    # check object
    obj = get_object(conn, object_type, object_name)
    if obj == None and args.debug:
      print('#')
      print('# OBJECT_EMPTY:', object_type, object_name)
      print('#\n')
      continue
    #
    if args.verbose:
      if flag == '':
        flag = 'NEW' if object_type == 'TABLE' and hash_old == '' else '<--' if object_type == 'TABLE' else ''
      #
      if object_type != recent_type and recent_type != '':
        print('{:>20} |'.format(''))
      print('{:>20} | {:<30} {:>8} | {:>8} {}'.format(*[
        object_type if object_type != recent_type else '',
        object_name if len(object_name) <= 30 else object_name[0:27] + '...',
        obj.count('\n') + 1,                                                    # count lines
        len(obj) if obj else '',                                                # count bytes
        flag
      ]))
      recent_type = object_type
    elif count_objects > 0:
      perc = min((i + 1) / count_objects, 1)
      dots = int(70 * perc)
      sys.stdout.write('\r' + ('.' * dots) + ' ' + str(int(perc * 100)) + '%')
      sys.stdout.flush()
    #
    lines = get_lines(obj)
    cleanup_fn = 'clean_' + object_type.replace(' ', '_').lower()
    if getattr(sys.modules[__name__], cleanup_fn, None):
      lines = getattr(sys.modules[__name__], cleanup_fn)(lines, schema)
    obj   = '\n'.join(lines)

    # append comments
    if object_type in ('TABLE', 'VIEW', 'MATERIALIZED VIEW'):
      obj += get_object_comments(conn, object_name)

    # fill in job template
    if object_type in ('JOB'):
      obj = get_job_fixed(object_name, obj, conn)

    # write object to file
    obj = obj.rstrip()
    if obj.rstrip('/') != obj:
      obj = obj.rstrip('/').rstrip() + '\n/'
    #
    with open(file, 'w', encoding = 'utf-8') as w:
      w.write(obj + '\n\n')
  #
  if not args.verbose:
    print()
  print()



#
# UPDATE LOCKED FILE
#
if (len(locked_objects) or args.lock):
  content = '\n'.join(sorted(locked_objects)) + '\n'
  with open(locked_log, 'w', encoding = 'utf-8') as w:
    w.write(content)

# delete all database object files except APEX
if args.lock and args.delete:
  for type in objects_sorted:
    for file in sorted(glob.glob(folders[type] + '/*.*')):
      short_file, hash_old, hash_new = get_file_details(file, git_root, hashed_old)
      if not (short_file in locked_objects):
        #print('  {}'.format(short_file))
        os.remove(file)



#
# EXPORT DATA
#
if (args.csv or isinstance(args.csv, list)) and not args.patch and not args.rollout:
  if not (os.path.isdir(folders['DATA'])):
    os.makedirs(folders['DATA'])

  # export/refresh existing files
  tables = [os.path.splitext(os.path.basename(file))[0] for file in glob.glob(folders['DATA'] + '*' + file_ext_csv)]

  # when passing values to -csv arg, find relevant tables
  if isinstance(args.csv, list) and len(args.csv):
    tables = []
    for tables_like in args.csv:
      data = conn.fetch_assoc(query_csv_tables, tables_like = tables_like.upper())
      for row in data:
        table_name = row.table_name.lower()
        if not (table_name in tables):
          tables.append(table_name)

  # proceed with data export
  print()
  print('EXPORT DATA TO CSV: ({})'.format(len(tables)))
  if args.verbose:
    print('------------------- {:12} {:>8} | {:>8} | {}'.format('', 'LINES', 'BYTES', 'STATUS'))
  #
  for (i, table_name) in enumerate(sorted(tables)):
    file = '{}{}.csv'.format(folders['DATA'], table_name)
    #
    try:
      table_cols    = conn.fetch_value(query_csv_columns, table_name = table_name)
      table_exists  = conn.fetch('SELECT {} FROM {} WHERE ROWNUM = 1'.format(table_cols, table_name))
    except Exception:
      if args.verbose:
        print('  {:50} | REMOVED'.format(table_name))
      if os.path.exists(file):
        os.remove(file)
      continue
    #
    csv_file  = open(file, 'w', encoding = 'utf-8')
    writer    = csv.writer(csv_file, delimiter = ';', lineterminator = '\n', quoting = csv.QUOTE_NONNUMERIC)
    columns   = [col for col in conn.cols if not (col in ignore_columns)]
    order_by  = ', '.join([str(i) for i in range(1, min(len(columns), 5) + 1)])
    #
    try:
      data    = conn.fetch('SELECT {} FROM {} ORDER BY {}'.format(', '.join(columns), table_name, order_by))
    except Exception:
      print()
      print('#')
      print('# CSV_EXPORT_FAILED:', table_name)
      print('#\n')
      print(traceback.format_exc())
      print(sys.exc_info()[2])
      continue

    # save as CSV
    writer.writerow(conn.cols)  # headers
    for row in data:
      writer.writerow(row)
    csv_file.close()

    # show progress
    if args.verbose:
      short_file, hash_old, hash_new = get_file_details(file, git_root, hashed_old)
      #
      print('  {:30} {:>8} | {:>8} {}'.format(*[
        table_name,
        len(data),                # lines
        os.path.getsize(file),    # bytes
        '| NEW' if hash_old == '' else '| CHANGED' if hash_new != hash_old else ''
      ]))
    else:
      perc = (i + 1) / len(tables)
      dots = int(70 * perc)
      sys.stdout.write('\r' + ('.' * dots) + ' ' + str(int(perc * 100)) + '%')
      sys.stdout.flush()
  #
  if not args.verbose:
    print()
  print()

  # convert all existing CSV files to MERGE statement files in patch/data/ folder
  all_data = ''
  for file in glob.glob(folders['DATA'] + '*' + file_ext_csv):
    target_file = patch_folders['data'] + os.path.basename(file).replace(file_ext_csv, file_ext_obj)
    table_name  = os.path.basename(file).split('.')[0]
    content     = get_merge_from_csv(file, conn)
    if content:
      with open(target_file, 'w', encoding = 'utf-8') as w:
        w.write(content)
        all_data += 'DELETE FROM {};\n{}\n\n\n'.format(table_name, content)
  #
  with open(patch_folders['data'] + '/__.sql', 'w', encoding = 'utf-8') as w:
    w.write(all_data + 'COMMIT;\n\n')



#
# EXPORT GRANTS
#
# @TODO: export also system grants/roles, credentials, ACL...
#
if not args.rollout:
  last_type   = ''
  content     = []
  #
  for row in conn.fetch_assoc(query_grants_made):
    # limit to objects on the locked.log
    if (len(locked_objects) or args.lock):
      if not row.type in folders:  # skip unsupported object types
        continue
      #
      object_file = '{}{}{}'.format(folders[row.type], row.table_name.lower(), file_ext_obj)
      short_file, hash_old, hash_new = get_file_details(object_file, git_root, hashed_old)
      #
      if not short_file in locked_objects:
        continue

    # show object type header
    if last_type != row.type:
      content.append('\n--\n-- {}\n--'.format(row.type))
    content.append(row.sql)
    last_type = row.type
  content = '{}\n\n'.format('\n'.join(content)).lstrip()
  #
  if not os.path.exists(os.path.dirname(grants_made_file)):
    os.makedirs(os.path.dirname(grants_made_file))
  with open(grants_made_file, 'w', encoding = 'utf-8') as w:
    w.write(content)

  # received grants
  received_grants = {}
  for row in conn.fetch_assoc(query_grants_recd):
    if not (row.owner in received_grants):
      received_grants[row.owner] = {}
    if not (row.type in received_grants[row.owner]):
      received_grants[row.owner][row.type] = {}
    if not (row.table_name in received_grants[row.owner][row.type]):
      received_grants[row.owner][row.type][row.table_name] = []
    #
    query = 'GRANT {:<17} ON {}.{:<30} TO {};'.format(row.privilege, row.owner.lower(), row.table_name.lower(), curr_schema.lower())
    received_grants[row.owner][row.type][row.table_name].append(query)
  #
  switch_schema = 'ALTER SESSION SET CURRENT_SCHEMA = {};\n'
  for owner, types in received_grants.items():
    content = [switch_schema.format(owner.lower())]
    for type in types:
      content.append('--\n-- {}\n--'.format(type))
      for table_name in sorted(received_grants[owner][type]):
        for query in sorted(received_grants[owner][type][table_name]):
          content.append(query)
      content.append('')
    content.append(switch_schema.format(curr_schema.lower()))
    #
    if not os.path.exists(os.path.dirname(grants_recd_file)):
      os.makedirs(os.path.dirname(grants_recd_file))
    with open(grants_recd_file.replace('#', owner), 'w', encoding = 'utf-8') as w:
      w.write(('\n'.join(content) + '\n').lstrip())



#
# APEX APPLICATIONS OVERVIEW (for the same schema)
#
apex_apps = {}
if (args.apex or isinstance(args.apex, list)) and not args.patch and not args.rollout and not (args.csv or isinstance(args.csv, list)):
  all_apps  = conn.fetch_assoc(query_apex_applications, schema = curr_schema)
  workspace = ''
  #
  for row in all_apps:
    if args.apex == []:
      if (len(locked_objects) or args.lock):
        if not os.path.exists('{}f{}{}'.format(apex_dir, row.application_id, file_ext_obj)):
          continue  # show only keeped apps
    elif not (row.application_id in args.apex):
      continue
    #
    apex_apps[row.application_id] = row
    if workspace == '':
      workspace = row.workspace
  #
  if apex_apps != {}:
    header = 'APEX APPLICATIONS - {} WORKSPACE:'.format(workspace)
    #
    print()
    print(header + '\n' + '-' * len(header))
    print('{:<54}PAGES | LAST CHANGE AT'.format(''))
    for (app_id, row) in apex_apps.items():
      print('{:>12} | {:<38} {:>5} | {}'.format(app_id, row.application_name[0:36], row.pages, row.last_updated_on))
    print()



#
# EXPORT APEX APP
#
if apex_apps != {} and not args.patch and not args.rollout:
  for app_id in apex_apps:
    if not (args.apex == [] or app_id in args.apex):
      continue

    # recreate temp dir
    if os.path.exists(apex_temp_dir):
      shutil.rmtree(apex_temp_dir, ignore_errors = True, onerror = None)
    os.makedirs(apex_temp_dir)

    # delete folder to remove obsolete objects only on full export
    apex_dir_app = '{}f{}'.format(apex_dir, app_id)
    if os.path.exists(apex_dir_app):
      shutil.rmtree(apex_dir_app, ignore_errors = True, onerror = None)
    #
    if not os.path.exists(apex_dir):
      os.makedirs(apex_dir)
    if not os.path.exists(apex_ws_files):
      os.makedirs(apex_ws_files)

    # get app details
    apex = conn.fetch_assoc(query_apex_app_detail, app_id = app_id)[0]
    #
    print()
    print('EXPORTING APEX APP:')
    print('-------------------')
    print('         APP | {} {}'.format(apex.app_id, apex.app_alias))
    print('        NAME | {}'.format(apex.app_name))
    #
    if args.info:
      print('   WORKSPACE | {:<30}  CREATED AT | {}'.format(apex.workspace, apex.created_at))
      print('   COMPATIB. | {:<30}  CHANGED AT | {}'.format(apex.compatibility_mode, apex.changed_at))
      print()
      print('       PAGES | {:<8}      LISTS | {:<8}    SETTINGS | {:<8}'.format(apex.pages, apex.lists or '', apex.settings or ''))
      print('       ITEMS | {:<8}       LOVS | {:<8}  BUILD OPT. | {:<8}'.format(apex.items or '', apex.lovs or '', apex.build_options or ''))
      print('   PROCESSES | {:<8}  WEB SERV. | {:<8}  INIT/CLEAN | {:<8}'.format(apex.processes or '', apex.ws or '', (apex.has_init_code or '-') + '/' + (apex.has_cleanup or '-')))
      print('     COMPUT. | {:<8}    TRANSL. | {:<8}      AUTH-Z | {:<8}'.format(apex.computations or '', apex.translations or '', apex.authz_schemes or ''))
    print()

    # get component names, because the id itself wont tell you much
    apex_replacements_plan = {
      'AUTHZ' : query_apex_authz_schemes,
      'LOV'   : query_apex_lov_names,
    }
    apex_replacements = {}
    for (type, query) in apex_replacements_plan.items():
      if not (type in apex_replacements):
        apex_replacements[type] = {}
      #
      rows = conn.fetch(query, app_id = app_id)
      for data in rows:
        (component_id, component_name) = data
        apex_replacements[type][component_id] = component_name

    # overwrite default AuthN scheme for the application based on file
    default_authentication = conn.fetch_value(query_apex_authn_default_scheme, app_id = app_id)

    # prepare requests (multiple exports)
    request_conn = ''
    requests = []
    if wallet_file != '' and 'wallet' in connection:
      request_conn += 'set cloudconfig {}.zip\n'.format(wallet_file.rstrip('.zip'))
      request_conn += 'connect {}/"{}"@{}\n'.format(*[
        connection['user'],
        connection['pwd'],
        connection['service']
      ])
    else:
      request_conn += 'connect {}/"{}"@{}:{}/{}\n'.format(*[
        connection['user'],
        connection['pwd'],
        connection['host'],
        connection['port'],
        connection['sid']
      ])

    # always do full APEX export, but when -r > 0 then show changed components
    if args.recent > 0:
      # partial export, get list of changed objects since that, show it to user
      requests.append('apex export -applicationid {app_id} -list -changesSince {since}')  # -list must be first

    # export full app in several formats
    requests.append('apex export -dir {dir} -applicationid {app_id} -nochecksum -expType EMBEDDED_CODE')
    requests.append('apex export -dir {dir} -applicationid {app_id} -nochecksum -skipExportDate -expComments -expTranslations -split')
    requests.append('apex export -dir {dir} -applicationid {app_id} -nochecksum -skipExportDate -expComments -expTranslations')
    requests.append('apex export -dir {dir_ws_files} -expFiles -workspaceid ' + str(apex_apps[app_id].workspace_id))
    #
    #-expOriginalIds -> strange owner and app_id
    #
    # @TODO: export readable version(s) when switch to 22.1
    #
    #requests.append('apex export -applicationid {} -split -skipExportDate -expComments -expTranslations -expType APPLICATION_SOURCE,READABLE_YAML')
    #requests.append('apex export -applicationid {} -split -skipExportDate -expComments -expTranslations -expType READABLE_YAML')
    #
    # @TODO: export app+ws files + decode
    #

    # trade progress for speed, creating all the JVM is so expensive
    if not args.debug:
      requests = ['\n'.join(requests)]

    # export APEX stuff
    apex_tmp = apex_tmp.replace('#', '{}'.format(app_id))  # allow to export multiple apps at the same time
    changed = []
    for (i, request) in enumerate(requests):
      request = request_conn + '\n' + request.format(dir = apex_dir, dir_temp = apex_temp_dir, dir_ws_files = apex_ws_files, app_id = app_id, since = req_today, changed = changed)
      process = 'sql /nolog <<EOF\n{}\nexit;\nEOF'.format(request)  # for normal platforms

      # for Windows create temp file
      if os.name == 'nt':
        process = 'sql /nolog @' + apex_tmp
        with open(apex_tmp, 'w', encoding = 'utf-8') as w:
          w.write(request + '\nexit;')

      # run SQLcl and capture the output
      result  = subprocess.run(process, shell = True, capture_output = not args.debug, text = True)
      output  = (result.stdout or '').strip()

      # for Windows remove temp file
      if os.name == 'nt' and os.path.exists(apex_tmp):
        os.remove(apex_tmp)

      # check output for recent APEX changes
      if ' -list' in request:
        lines   = output.splitlines()
        objects = {}
        changed = []
        if len(lines) > 5 and lines[5].startswith('Date') and lines[6].startswith('----------------'):
          for line in lines[7:]:
            if (line.startswith('Disconnected') or line.startswith('Exporting Application')):
              break
            line_date   = line[0:16].strip()
            line_object = line[17:57].strip()
            line_type   = line_object.split(':')[0]
            line_name   = line[57:].strip()
            #
            if not (line_type in objects):
              objects[line_type] = []
            objects[line_type].append(line_name)
            changed.append(line_object)
          #
          print()
          print('CHANGES SINCE {}: ({})'.format(req_today, len(changed)))
          print('-------------------------')
          for obj_type, obj_names in objects.items():
            for (j, name) in enumerate(sorted(obj_names)):
              print('{:>20} | {}'.format(obj_type if j == 0 else '', name))
          print()
        changed = ' '.join(changed)

      # show progress
      if args.debug:
        print()
        print(process)
        print()
        print(output)
      else:
        perc = (i + 1) / len(requests)
        dots = int(70 * perc)
        sys.stdout.write('\r' + ('.' * dots) + ' ' + str(int(perc * 100)) + '%')
        sys.stdout.flush()

      # cleanup files after each loop
      clean_apex_files(app_id, folders['APEX'], apex_replacements, default_authentication)
    #
    print()
    print()

    # rename workspace files
    ws_files = 'files_{}.sql'.format(apex_apps[app_id].workspace_id)
    if os.path.exists(apex_ws_files + ws_files):
      target_file = '{}{}.sql'.format(apex_ws_files, apex_apps[app_id].workspace)
      if os.path.exists(target_file):
        os.remove(target_file)
      os.rename(apex_ws_files + ws_files, target_file)

    # move some changed files to proper APEX folder
    apex_partial = '{}f{}'.format(apex_temp_dir, app_id)
    if os.path.exists(apex_partial):
      remove_files = [
        'install_component.sql',
        'install_page.sql',
        'application/end_environment.sql',
        'application/set_environment.sql',
        'application/pages/delete*.sql',
      ]
      for file_pattern in remove_files:
        for file in glob.glob(apex_partial + '/' + file_pattern):
          os.remove(file)
      #
      shutil.copytree(apex_partial, '{}f{}'.format(apex_dir, app_id), dirs_exist_ok = True)

    # cleanup
    if os.path.exists(apex_temp_dir):
      shutil.rmtree(apex_temp_dir, ignore_errors = True, onerror = None)



#
# SHOW TIMER
#
if count_objects or apex_apps != {} or (args.csv or isinstance(args.csv, list)):
  print('TIME:', round(timeit.default_timer() - start_timer, 2))
  print('\n')



#
# PREPARE PATCH
#
if args.patch:
  header = 'PREPARING PATCH FOR {}:'.format(args.patch).replace(' FOR True:', ':')
  print()
  print(header)
  print('-' * len(header))
  print()

  # remove target patch files
  for file in (patch_today, patch_zip):
    if os.path.exists(file):
      os.remove(file)

  # cleanup old patches
  for file in glob.glob(patch_done + '/*' + file_ext_obj):
    os.remove(file)
  #
  for file in glob.glob(patch_folders['changes'] + '/*' + file_ext_obj):
    if os.path.getsize(file) == 0:
      os.remove(file)

  # prep arrays
  changed_objects   = []
  processed_objects = []
  processed_names   = []
  ordered_objects   = []
  references_todo   = {}
  references        = {}
  last_type         = ''
  table_relations   = {}
  tables_todo       = []
  patch_notes       = []
  patch_content     = []

  # start with tables, get referenced tables for each table
  for row in conn.fetch_assoc(query_tables_dependencies):
    table_relations[row.table_name] = row.references.split(', ')

  # get list of changed objects and their references
  for object_type in objects_sorted:
    for file in sorted(glob.glob(folders[object_type] + '/*' + file_ext_obj)):
      short_file, hash_old, hash_new = get_file_details(file, git_root, hashed_old)

      # check if object changed
      if hash_old == hash_new:                      # ignore unchanged objects
        continue
      #
      object_name = os.path.basename(file).split('.')[0].upper()
      curr_object = '{}.{}'.format(object_type, object_name)
      #
      references_todo[curr_object] = []
      references[curr_object] = []
      changed_objects.append(curr_object)
      #
      if object_type in ('TABLE', 'DATA'):
        tables_todo.append(object_name)           # to process tables first
        #
        if object_name in table_relations:
          for table_name in table_relations[object_name]:
            ref_object = '{}.{}'.format('TABLE', table_name)
            references_todo[curr_object].append(ref_object)
            references[curr_object].append(ref_object)
      else:
        for row in conn.fetch_assoc(query_objects_before, object_name = object_name, object_type = object_type):
          ref_object = '{}.{}'.format(row.type, row.name)
          references_todo[curr_object].append(ref_object)
          references[curr_object].append(ref_object)

  # sort objects to have them in correct order
  for i in range(0, 20):                            # adjust depending on your depth
    for obj, refs in references_todo.items():
      if obj in ordered_objects:                    # object processed
        continue

      # process tables first
      object_type, object_name = obj.split('.')
      if object_type != 'TABLE' and len(tables_todo):
        continue
      #
      if len(refs) == 0:
        ordered_objects.append(obj)                 # no more references
        if object_type == 'TABLE':
          tables_todo.remove(object_name)
        continue

      # pass only existing objects
      for ref_object in refs:
        if ref_object == obj:                       # ignore self reference
          references_todo[obj].remove(ref_object)
          continue
        if not (ref_object in changed_objects):     # ignore objects not part of the patch
          references_todo[obj].remove(ref_object)
          continue
        if ref_object in ordered_objects:           # ignore objects created in previous steps
          references_todo[obj].remove(ref_object)
          continue
        if not obj.startswith('TABLE.') and ref_object.startswith('TABLE.'):  # ignore tables referenced from objects
          references_todo[obj].remove(ref_object)
          continue

  # create patch plan
  for obj in ordered_objects:
    if not (obj in references):                     # ignore unknown objects
      continue
    #
    object_type, object_name = obj.split('.')
    file = '{}{}{}'.format(folders[object_type], object_name.lower(), file_ext_obj if object_type != 'PACKAGE' else file_ext_spec)
    short_file, hash_old, hash_new = get_file_details(file, git_root, hashed_old)
    flag = '[+]' if hash_old == '' else 'ALTERED' if hash_old != hash_new and object_type == 'TABLE' else ''
    #
    processed_names.append(obj)                     # to final check if order is correct
    processed_objects.append({
      'type'        : object_type,
      'name'        : object_name,
      'file'        : file,
      'short_file'  : short_file,
      'hash_old'    : hash_old,
      'hash_new'    : hash_new,
    })
    hashed_new[short_file] = hash_new  # store value for new patch.log
    #
    if ((last_type != object_type and last_type != '') or (len(references[obj]) and args.verbose)):
      patch_notes.append('{:<20} |'.format(''))
    patch_notes.append('{:>20} | {:<46}{:>8}'.format(object_type if last_type != object_type else '', object_name, flag))
    last_type = object_type
    #
    if args.verbose:
      for ref_object in references[obj]:
        if ref_object != obj and ref_object in changed_objects:
          object_type, object_name = ref_object.split('.')
          obj = '{:<30} {}'.format((object_name + ' ').ljust(32, '.'), object_type[0:12])
          if not (ref_object in processed_names):
            obj = (obj + ' <').ljust(48, '-') + ' MISSING OBJECT'
          patch_notes.append('{:<20} |   > {}'.format('', obj))

  # show changed data files
  for object_type in ('DATA',):
    if len(ordered_objects):
      patch_notes.append('{:<20} |'.format(''))
    #
    files = sorted(glob.glob(folders[object_type] + '/*' + file_ext_csv))
    if len(files):
      for file in files:
        short_file, hash_old, hash_new = get_file_details(file, git_root, hashed_old)
        object_name = os.path.basename(short_file).split('.')[0].upper()
        if hash_old != hash_new or 1 == 1:
          patch_notes.append('{:>20} | {:<54}'.format(object_type if last_type != object_type else '', object_name))
        last_type = object_type
      patch_notes.append('{:<20} |'.format(''))

  # create list of files to process
  processed_files = []
  patch_line      = '@@"../{}"'  # @@ = relative to script from which it is called from
  #
  for target_dir in sorted(patch_folders.values()):
    type    = next((type for type, dir in patch_folders.items() if dir == target_dir), None)
    files   = glob.glob(target_dir + '/*' + file_ext_obj)

    # process files in patch folder first
    if len(files):
      patch_content.append('\n--\n-- {}\n--'.format(type.upper()))
      for file in files:
        short_file = file.replace(git_root, '').replace('\\', '/').lstrip('/')
        #
        if os.path.basename(short_file) == '__.sql':    # ignore file with all data files merged
          continue
        #
        patch_content.append(patch_line.format(short_file))
        processed_files.append(short_file)

    # add objects mapped to current patch folder
    if type in patch_map:
      header_printed = False
      for obj in processed_objects:
        if not (obj['type'] in patch_map[type]):        # ignore non related types
          continue
        if not (obj['type'] in folders):                # ignore unknown types
          continue
        if obj['short_file'] in processed_files:        # ignore processed objects/files
          continue
        #
        if not header_printed:
          header_printed = True
          patch_content.append('\n--\n-- {}\n--'.format(type.upper()))
        #
        patch_content.append(patch_line.format(obj['short_file']))
        processed_files.append(obj['short_file'])

  # append APEX apps
  apex_apps = glob.glob(folders['APEX'] + '/f*' + file_ext_obj)
  if len(apex_apps):
    patch_content.append('\n--\n-- APEX\n--')
    for file in apex_apps:
      short_file, hash_old, hash_new = get_file_details(file, git_root, hashed_old)
      processed_files.append(short_file)
      patch_content.append(patch_line.format(short_file))
  patch_content.append('')

  # store new hashes for rollout
  content = []
  with open(patch_log, 'w', encoding = 'utf-8') as w:
    for file in sorted(hashed_new.keys()):
      content.append('{} | {}'.format(hashed_new[file], file))
    content = '\n'.join(content) + '\n'
    w.write(content)

  # show to user and store in the patch file
  print('\n'.join(patch_notes))
  print('\n'.join(patch_content))
  #
  with open(patch_today, 'w', encoding = 'utf-8') as w:
    w.write('--\n--' + '\n--'.join(patch_notes) + '\n' + '\n'.join(patch_content) + '\n')

  # create binary to whatever purpose
  if args.zip:
    if os.path.exists(patch_zip):
      os.remove(patch_zip)
    #
    shutil.make_archive(git_root + 'patch', 'zip', git_root)  # everything in folder
    os.rename(git_root + 'patch.zip', patch_zip)



#
# CONFIRM ROLLOUT - STORE CURRENT HASHES IN A LOG
#
if args.rollout:
  header = 'ROLLOUT CONFIRMED FOR {}:'.format(args.rollout).replace(' FOR True:', ':')
  print()
  print(header)
  print('-' * len(header))

  # show removed files
  if args.delete:
    for file in sorted(hashed_old.keys()):
      if not os.path.exists(git_root + '/' + file):
        print('  [-] {}'.format(file))

  # store hashes for next patch
  if args.rollout != True:
    rollout_log = rollout_log.replace('.', '.{}.'.format(args.rollout))
  #
  with open(rollout_log, 'w', encoding = 'utf-8') as w:
    # get files and hashes from patch.log file and overwrite old hashes
    if os.path.exists(patch_log):
      with open(patch_log, 'r', encoding = 'utf-8') as r:
        for line in r.readlines():
          if '|' in line:
            (hash, file) = line.split('|')
            hash = hash.strip()
            file = file.strip()
            hashed_old[file] = hash
            print('  [+] {}'.format(file))

    # keep all previous hashes
    content = []
    for file in sorted(hashed_old.keys()):
      short_file = file.replace(git_root, '').replace('\\', '/').lstrip('/')
      # ignore/remove non existing files only on -delete mode
      if args.delete and not os.path.exists(git_root + '/' + file):
        continue
      content.append('{} | {}'.format(hashed_old[file], file))
    #
    w.write('\n'.join(content) + '\n')

    # cleanup
    if os.path.exists(patch_log):
      os.remove(patch_log)
  #
  print()

