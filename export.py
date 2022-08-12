# coding: utf-8
import sys, os, argparse, pickle, timeit, traceback, glob, csv, subprocess, datetime, shutil, zipfile, hashlib
from oracle_wrapper import Oracle
from export_fn import *

#
# ARGS
#
parser = argparse.ArgumentParser()
parser.add_argument('-g', '-target',  '--target',   help = 'Target folder (Git root)')
parser.add_argument('-n', '-name',    '--name',     help = 'Connection name')
parser.add_argument('-t', '-type',    '--type',     help = 'Filter specific object type', default = '')
parser.add_argument('-r', '-recent',  '--recent',   help = 'Filter objects compiled since SYSDATE - $recent')
parser.add_argument('-a', '-app',     '--app',      help = 'APEX application')
parser.add_argument('-c', '-csv',     '--csv',      help = 'Export tables in data/ dor to CSV files',   nargs = '?', default = False, const = True)
parser.add_argument('-v', '-verbose', '--verbose',  help = 'Show object names during export',           nargs = '?', default = False, const = True)
parser.add_argument('-d', '-debug',   '--debug',    help = '',                                          nargs = '?', default = False, const = True)
parser.add_argument('-i', '-info',    '--info',     help = 'Show DB/APEX versions and app details',     nargs = '?', default = False, const = True)
parser.add_argument(      '-patch',   '--patch',    help = 'Prepare patch',                             nargs = '?', default = False, const = True)
parser.add_argument(      '-rollout', '--rollout',  help = 'Mark rollout as done',                      nargs = '?', default = False, const = True)
parser.add_argument('-f', '-feature', '--feature',  help = 'Feature branch, keep just changed files',   nargs = '?', default = False, const = True)
parser.add_argument('-z', '-zip',     '--zip',      help = 'Patch as ZIP',                              nargs = '?', default = False, const = True)
parser.add_argument(      '-delete',  '--delete',   help = 'Delete unchanged files (db objects only)',  nargs = '?', default = False, const = True)
parser.add_argument(      '-lock',    '--lock',     help = 'Updates only objects in the locked.log',    nargs = '?', default = False, const = True)
#
args = vars(parser.parse_args())
args['app']     = int(args['app']     or 0)
args['recent']  = int(args['recent']  or -1)
#
root      = os.path.dirname(os.path.realpath(__file__))
conn_dir  = os.path.abspath(root + '/conn')

# find connection file
conn_files = []
if 'target' in args and args['target'] != None and len(args['target']) > 0:
  conn_files.append(args['target'] + '/documentation/db.conf')
if 'name' in args:
  conn_files.append(os.path.normpath('{}/{}.conf'.format(conn_dir, args['name'])))
#
for db_conf in conn_files:
  if os.path.exists(db_conf):
    with open(db_conf, 'rb') as b:
      connection = pickle.load(b)
      if args['target'] == None and 'target' in connection:  # overwrite target from pickle file
        args['target'] = connection['target']
      break

# check args
if args['debug']:
  print('ARGS:')
  print('-----')
  for (key, value) in args.items():
    if not (key in ('pwd', 'wallet_pwd')):
      print('{:>10} = {}'.format(key, value))
  print('')

# check target
if (args['target'] == None or len(args['target']) == 0):
  print('#')
  print('# UNKNOWN TARGET')
  print('#')
  print()
  sys.exit()



#
# SETUP VARIABLES
#

# target folders by object types
git_target  = os.path.abspath(args['target'] + '/database') + '/'
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
  'tables'    : ['TABLE', 'SEQUENCE', 'INDEX', 'MATERIALIZED VIEW'],
  'objects'   : ['VIEW', 'TRIGGER', 'PROCEDURE', 'FUNCTION', 'PACKAGE', 'PACKAGE BODY', 'SYNONYM'],
  'jobs'      : ['JOB'],
  'data'      : ['DATA'],
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
  'jobs'      : patch_root + '/50_jobs/',
  'cleanup'   : patch_root + '/60_cleanup/',
  'data'      : patch_root + '/70_data/',
  'finally'   : patch_root + '/80_finally/',
}
patch_store     = ('changes')   # store hashes for files in these folders
patch_manually  = '{}{}.sql'.format(patch_folders['changes'], today_date)
patch_tables    = patch_folders['changes'] + today_date + '_tables.sql'  # file to notify users about table changes
file_ext_obj    = '.sql'
file_ext_csv    = '.csv'
file_ext_spec   = '.spec.sql'

# apex folders
apex_dir        = folders['APEX']
apex_temp_dir   = apex_dir + 'temp/'  # temp file for partial APEX exports
apex_ws_files   = apex_dir + 'workspace_files/'
apex_tmp        = 'apex.tmp'  # temp file for running SQLcl on Windows

# cleanup junk files created on Mac probably by iCloud sync
path = apex_dir + '**/* [0-9].*'
for file in glob.glob(path, recursive = True):
  os.remove(file)



#
# CONNECT TO DATABASE
#
curr_schema = connection['user'].split('[')[1].rstrip(']') if '[' in connection['user'] else connection['user']
grants_file = '{}{}.sql'.format(folders['GRANT'], curr_schema)
#
if not args['rollout']:
  conn      = Oracle(connection)
  data      = conn.fetch_assoc(query_today, recent = args['recent'] if args['recent'] >= 0 else '')
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
  print('    SOURCE | {}@{}/{}{}'.format(
    connection['user'],
    connection.get('host', ''),
    connection.get('service', ''),
    connection.get('sid', '')))
  #
  if wallet_file != '':
    print('    WALLET | {}'.format(connection['wallet'].replace(common_root, '~ ')))
  #
  print('           | {}'.format(db_conf.replace(common_root, '~ ')))
  print('    TARGET | {}'.format(git_target.replace(common_root, '~ ')))
  print()

  # get versions
  if args['info']:
    try:
      version_apex  = conn.fetch_value(query_version_apex)
      version_db    = conn.fetch_value(query_version_db)
    except Exception:
      version_apex  = version_apex or ''
      version_db    = conn.fetch_value(query_version_db_old)
    #
    print('  DATABASE | {}'.format('.'.join(version_db.split('.')[0:2])))
    print('      APEX | {}'.format('.'.join(version_apex.split('.')[0:2])))
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
if args['patch']:
  if not os.path.exists(patch_manually):
    with open(patch_manually, 'w', encoding = 'utf-8') as w:
      w.write('')

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

# split tables into buckets
tables_changed  = []
tables_added    = []



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

if args['lock'] and not args['delete']:
  # add all existing files to the locked log
  for type in objects_sorted:
    for file in sorted(glob.glob(folders[type] + '/*' + file_ext_obj)):
      short_file, hash_old, hash_new = get_file_details(file, git_root, hashed_old)
      if not (short_file in locked_objects):
        locked_objects.append(short_file)



#
# PREVIEW OBJECTS
#
data_objects  = []
count_objects = 0
#
if args['recent'] != 0 and not args['patch'] and not args['rollout'] and not args['feature']:
  print()
  print('OBJECTS OVERVIEW:                                      CONSTRAINTS:')
  print('-----------------                                      ------------')

  # retrieve objects in specific order
  sort = ''
  for (i, object_type) in enumerate(objects_sorted):
    sort += 'WHEN \'{}\' THEN {}'.format(object_type, i)
  #
  data_objects = conn.fetch_assoc(query_objects.format(sort), object_type = args['type'].upper(), recent = args['recent'] if args['recent'] >= 0 else '')
  summary = {}
  for row in data_objects:
    if not (row.object_type) in summary:
      summary[row.object_type] = 0
    summary[row.object_type] += 1
    if row.object_type in folders:
      count_objects += 1
  #
  all_objects = conn.fetch_assoc(query_summary)
  print('                     | CHANGED |   TOTAL')  # fetch data first
  for row in all_objects:
    if row.object_count:
      print('{:>20} | {:>7} | {:>7} {:<4} {:>12}{}{:>4}'.format(*[
        row.object_type,
        summary.get(row.object_type, ''),
        row.object_count,
        '' if row.object_type in folders else '<--',  # mark not supported object types
        row.constraint_type or '',
        ' | ' if row.constraint_type else '',
        row.constraint_count or ''
      ]))
    else:
      print('{:>58}{}{:>4}'.format(row.constraint_type or '', ' | ' if row.constraint_type else '', row.constraint_count or ''))
  #
  print('                             ^')  # to highlight affected objects
  print()



#
# EXPORT OBJECTS
#
if count_objects:
  if (len(locked_objects) or args['lock']):
    count_objects = len(locked_objects)
    print('EXPORTING LOCKED OBJECTS: ({})'.format(count_objects))
    if args['verbose']:
      print('-------------------------')
  else:
    print('EXPORTING OBJECTS: ({})'.format(count_objects))
    if args['verbose']:
      print('------------------')
  if args['verbose']:
    print('{:54}{:>8} | {:>8}'.format('', 'LINES', 'BYTES'))
  #
  recent_type = ''
  for (i, row) in enumerate(data_objects):
    object_type, object_name = row.object_type, row.object_name

    # make sure we have target folders ready
    if not (object_type in folders):
      if args['debug']:
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
    if (len(locked_objects) or args['lock']):
      flag = ' '  # dont show regular flags
      if not (short_file in locked_objects):
        if hash_old == '':
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
    if obj == None and args['debug']:
      print('#')
      print('# OBJECT_EMPTY:', object_type, object_name)
      print('#\n')
      continue
    #
    if args['verbose']:
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
  if not args['verbose']:
    print()
  print()

# update locked file
if (len(locked_objects) or args['lock']):
  content = '\n'.join(sorted(locked_objects)) + '\n'
  with open(locked_log, 'w', encoding = 'utf-8') as w:
    w.write(content)



#
# EXPORT DATA
#
if args['csv'] and not args['patch'] and not args['rollout'] and not args['feature']:
  if not (os.path.isdir(folders['DATA'])):
    os.makedirs(folders['DATA'])
  #
  files = [os.path.splitext(os.path.basename(file))[0] for file in glob.glob(folders['DATA'] + '*.csv')]
  ignore_columns = ['updated_at', 'updated_by', 'created_at', 'created_by', 'calculated_at']
  #
  print()
  print('EXPORT DATA TO CSV: ({})'.format(len(files)))
  if args['verbose']:
    print('------------------- {:12} {:>8} | {:>8} | {}'.format('', 'LINES', 'BYTES', 'STATUS'))
  #
  for (i, table_name) in enumerate(sorted(files)):
    try:
      table_cols    = conn.fetch_value(query_csv_columns, table_name = table_name)
      table_exists  = conn.fetch('SELECT {} FROM {} WHERE ROWNUM = 1'.format(table_cols, table_name))
    except Exception:
      print()
      print('#')
      print('# TABLE_MISSING:', table_name)
      print('#\n')
      #print(traceback.format_exc())
      #print(sys.exc_info()[2])
      continue
    #
    file        = '{}{}.csv'.format(folders['DATA'], table_name)
    csv_file    = open(file, 'w', encoding = 'utf-8')
    writer      = csv.writer(csv_file, delimiter = ';', lineterminator = '\n', quoting = csv.QUOTE_NONNUMERIC)
    columns     = [col for col in conn.cols if not (col in ignore_columns)]
    order_by    = ', '.join([str(i) for i in range(1, min(len(columns), 5) + 1)])
    #
    try:
      data      = conn.fetch('SELECT {} FROM {} ORDER BY {}'.format(', '.join(columns), table_name, order_by))
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
    if args['verbose']:
      short_file, hash_old, hash_new = get_file_details(file, git_root, hashed_old)
      #
      print('  {:30} {:>8} | {:>8} {}'.format(*[
        table_name,
        len(data),                # lines
        os.path.getsize(file),    # bytes
        '| NEW' if hash_old == '' else '| CHANGED' if hash_new != hash_old else ''
      ]))
    else:
      perc = (i + 1) / len(files)
      dots = int(70 * perc)
      sys.stdout.write('\r' + ('.' * dots) + ' ' + str(int(perc * 100)) + '%')
      sys.stdout.flush()
  #
  if not args['verbose']:
    print()
  print()



#
# EXPORT GRANTS
#
if not args['rollout']:
  all_grants  = conn.fetch_assoc(query_grants_made)
  last_type   = ''
  content     = []
  #
  for row in all_grants:
    # limit to objects on the locked.log
    if (len(locked_objects) or args['lock']):
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
  content = '{}\n\n'.format('\n'.join(content).lstrip())
  #
  if not os.path.exists(os.path.dirname(grants_file)):
    os.makedirs(os.path.dirname(grants_file))
  with open(grants_file, 'w', encoding = 'utf-8') as w:
    w.write(content)



#
# APEX APPLICATIONS OVERVIEW (for the same schema)
#
apex_apps = {}
if not args['patch'] and not args['rollout'] and not args['feature'] and (not args['csv'] or args['app']):
  all_apps  = conn.fetch_assoc(query_apex_applications, schema = connection['user'].upper())
  workspace = ''
  #
  for row in all_apps:
    if (len(locked_objects) or args['lock']):
      if not os.path.exists('{}f{}{}'.format(apex_dir, row.application_id, file_ext_obj)):
        continue  # show only keeped apps
    apex_apps[row.application_id] = row
    if workspace == '':
      workspace = row.workspace
  #
  if apex_apps != {} and not args['app'] and not args['patch'] and not args['rollout'] and not args['feature']:
    header = 'APEX APPLICATIONS - {} WORKSPACE:'.format(workspace)
    #
    print()
    print(header + '\n' + '-' * len(header))
    print('{:<52}PAGES | LAST CHANGE AT'.format(''))
    for (app_id, row) in apex_apps.items():
      print('{:>10} | {:<38} {:>5} | {}'.format(app_id, row.application_name[0:36], row.pages, row.last_updated_on))
    print()



#
# EXPORT APEX APP
#
if 'app' in args and args['app'] in apex_apps and not args['patch'] and not args['rollout'] and not args['feature']:
  # recreate temp dir
  if os.path.exists(apex_temp_dir):
    shutil.rmtree(apex_temp_dir, ignore_errors = False, onerror = None)
  os.makedirs(apex_temp_dir)

  # delete folder to remove obsolete objects only on full export
  apex_dir_app = '{}f{}'.format(apex_dir, args['app'])
  if os.path.exists(apex_dir_app):
    shutil.rmtree(apex_dir_app, ignore_errors = False, onerror = None)
  #
  if not os.path.exists(apex_dir):
    os.makedirs(apex_dir)
  if not os.path.exists(apex_ws_files):
    os.makedirs(apex_ws_files)

  # get app details
  apex = conn.fetch_assoc(query_apex_app_detail, app_id = args['app'])[0]
  #
  print()
  print('EXPORTING APEX APP:')
  print('-------------------')
  print('         APP | {} {}'.format(apex.app_id, apex.app_alias))
  print('        NAME | {}'.format(apex.app_name))
  #
  if args['info']:
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
    rows = conn.fetch(query, app_id = args['app'])
    for data in rows:
      (component_id, component_name) = data
      apex_replacements[type][component_id] = component_name

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
  if args['recent'] > 0:
    # partial export, get list of changed objects since that, show it to user
    requests.append('apex export -applicationid {app_id} -list -changesSince {since}')  # -list must be first

  # export full app in several formats
  requests.append('apex export -dir {dir} -applicationid {app_id} -nochecksum -expType EMBEDDED_CODE')
  requests.append('apex export -dir {dir} -applicationid {app_id} -nochecksum -skipExportDate -expComments -expTranslations -split')
  requests.append('apex export -dir {dir} -applicationid {app_id} -nochecksum -skipExportDate -expComments -expTranslations')
  requests.append('apex export -dir {dir_ws_files} -expFiles -workspaceid ' + str(apex_apps[args['app']].workspace_id))
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

  # export APEX stuff
  changed = []
  for (i, request) in enumerate(requests):
    request = request_conn + '\n' + request.format(dir = apex_dir, dir_temp = apex_temp_dir, dir_ws_files = apex_ws_files, app_id = args['app'], since = req_today, changed = changed)
    process = 'sql /nolog <<EOF\n{}\nexit;\nEOF'.format(request)  # for normal platforms

    # for Windows create temp file
    if os.name == 'nt':
      process = 'sql /nolog @' + apex_tmp
      with open(apex_tmp, 'w', encoding = 'utf-8') as w:
        w.write(request + '\nexit;')

    result  = subprocess.run(process, shell = True, capture_output = not args['debug'], text = True)
    output  = result.stdout.strip()

    if os.name == 'nt' and os.path.exists(apex_tmp):
      os.remove(apex_tmp)

    # check output for recent APEX changes
    if ' -list' in request:
      lines   = output.split('\n')
      objects = {}
      changed = []
      if len(lines) > 5 and lines[5].startswith('Date') and lines[6].startswith('----------------'):
        for line in lines[7:]:
          if line.startswith('Disconnected'):
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
    if args['debug']:
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
    clean_apex_files(args['app'], folders['APEX'], apex_replacements)
  #
  print()
  print()

  # rename workspace files
  ws_files = 'files_{}.sql'.format(apex_apps[args['app']].workspace_id)
  if os.path.exists(apex_ws_files + ws_files):
    target_file = '{}{}.sql'.format(apex_ws_files, apex_apps[args['app']].workspace)
    if os.path.exists(target_file):
      os.remove(target_file)
    os.rename(apex_ws_files + ws_files, target_file)

  # move some changed files to proper APEX folder
  apex_partial = '{}f{}'.format(apex_temp_dir, args['app'])
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
    shutil.copytree(apex_partial, '{}f{}'.format(apex_dir, args['app']), dirs_exist_ok = True)

  # cleanup
  if os.path.exists(apex_temp_dir):
    shutil.rmtree(apex_temp_dir, ignore_errors = False, onerror = None)

# show timer after all db queries are done
if count_objects or args['app'] > 0 or args['app'] or args['csv']:
  print('TIME:', round(timeit.default_timer() - start_timer, 2))
  print('\n')



#
# PREPARE PATCH
#
if args['patch'] and not args['feature']:
  print()
  print('PREPARING PATCH:')
  print('----------------')

  # remove target patch files
  for file in [patch_today, patch_zip]:
    if os.path.exists(file):
      os.remove(file)
  if os.path.exists(patch_tables):
    os.remove(patch_tables)

  # cleanup old patches
  for file in glob.glob(patch_done + '/*' + file_ext_obj):
    os.remove(file)

if (args['patch'] or args['feature']):
  # get order good for deployment
  tables_sorted = []
  table_notes   = []
  #
  try:
    data = conn.fetch_assoc(query_tables_sorted)
    for row in data:
      tables_sorted.append(row.table_name)
  except Exception:
    if args['debug']:
      print('#')
      print('# CYCLE_DETECTED_MOST_LIKELY')
      print('#')
      #print(traceback.format_exc())
      print(sys.exc_info()[2])
      print()

  # get list of files in correct order
  buckets = []
  for target_dir in sorted(patch_folders.values()):
    # go thru patch template files
    type        = next((type for type, dir in patch_folders.items() if dir == target_dir), None)
    object_type = ''
    files       = glob.glob(target_dir + '/*' + file_ext_obj)
    files_todo  = [[type, object_type, sorted(files)]]

    # go thru database objects in requested order
    if type in patch_map:
      for object_type in patch_map[type]:
        if object_type in folders:
          files_path  = folders[object_type] + '/*' + (file_ext_csv if object_type == 'DATA' else file_ext_obj)
          files       = sorted(glob.glob(files_path))

          # sort tables to be in installable order
          if object_type == 'TABLE':
            files = get_files_sorted(files, tables_sorted)
          #
          files_todo.append([type, object_type, files])

    # pass only changed files
    for (type, object_type, files) in files_todo:
      files_changed = []
      for file in files:
        short_file, hash_old, hash_new = get_file_details(file, git_root, hashed_old)

        # check package spec vs body (in the same dir)
        if object_type == 'PACKAGE BODY' and file.endswith(file_ext_spec):   # ignore package spec in body dir
          continue
        if object_type == 'PACKAGE' and not file.endswith(file_ext_spec):    # ignore package body in spec dir
          continue

        # ignore unchanged files in some folders
        if type in patch_store and hash_new == hash_old:
          continue

        # special treatment for tables
        if type == 'tables' and object_type == 'TABLE':
          table_name = os.path.basename(short_file).split('.')[0].upper()
          if hash_old == '':
            # add new tables to the list
            tables_added.append(table_name)
          elif hash_new != hash_old:
            # ignore changed tables, they will need a manual patch
            tables_changed.append(table_name)

            # dont put changed table on the patch list
            continue

        # check file hash and compare it with hash in rollout.log
        if (hash_new != hash_old or object_type == '') and os.path.getsize(file) > 0:
          files_changed.append(file)

        # store hash even for manual patch files
        if (type in patch_store or object_type != '') and hash_new != hash_old:
          hashed_new[short_file] = hash_new
      #
      if len(files_changed):
        buckets.append([type, object_type, files_changed])

      # pass changed tables so we can show then on the screen
      files_changed = []
      if type == 'changes' and len(tables_changed):
        buckets.append([type, 'TABLE', tables_changed])

if args['patch'] and not args['feature']:
  patch_files = []

  # open target file and write new content there
  count_lines = 0
  with open(patch_today, 'w', encoding = 'utf-8') as w:
    last_type = ''
    for (type, object_type, files) in buckets:
      if type != last_type:
        print('{:20} | {}'.format('', patch_folders[type].replace(patch_root + '/', '')))
      #
      last_object_type = ''
      for (i, file) in enumerate(files):
        content = ''
        short_file, hash_old, hash_new = file, '', ''
        file_exists = os.path.exists(file)
        if file_exists:
          short_file, hash_old, hash_new = get_file_details(file, git_root, hashed_old)

        # show file with/for table changes
        if type == 'changes' and len(tables_changed) and i == 0:
          print('{:>20} > {:40}  | {}'.format('', os.path.basename(patch_tables), 'MANUALLY'))

        # show progress to user
        if not args['debug']:
          object_name = os.path.basename(short_file)
          status      = ''
          if object_type != '':
            object_name = object_name.split('.')[0].upper()
            status      = '| MANUALLY' if not file_exists else '| NEW' if hash_old == '' else '| CHANGED'
          #
          print('{:>20} {} {}{:<40}{}'.format(*[
            object_type if object_type != last_object_type else '',
            '>' if not file_exists else '+' if '.' in object_name else '|',
            '  ' if object_type != '' else '',
            object_name,
            status
          ]))

        # retrieve file content
        if object_type == 'DATA' and file.endswith(file_ext_csv):
          # convert CSV files to MERGE
          content = get_merge_from_csv(file, conn)

          # add CSV file to patch.log
          if file_exists:
            hashed_new[short_file] = hashlib.md5(open(file, 'rb').read()).hexdigest()
          #
        elif file_exists:
          # retrieve object content
          with open(file, 'r', encoding = 'utf-8') as r:
            content = r.read()

          # drop changed view first due to grant issues
          # drop MVW so we can actually create it again
          if object_type in ('VIEW', 'MATERIALIZED VIEW'):
            object_name = os.path.basename(short_file).split('.')[0]
            content = 'DROP {} {};\n--\n'.format(object_type, object_name) + content

        # dont copy file, just append target patch file
        if len(content):
          if object_type == 'SYNONYM':
            content = content.rstrip().split(' FOR ')
            content = '{:<57} FOR {}\n'.format(content[0], content[1])
          else:
            content = '--\n-- {}\n--\n{}\n\n'.format(short_file, content.rstrip())
          w.write(content)
          count_lines += content.count('\n')
          #
          if args['debug']:
            print(content)
        #
        last_object_type = object_type
      #
      if not args['debug']:
        print('{:20} |'.format(''))
      #
      last_type = type
    #
    patch_files.append([patch_today, count_lines])

    # append GRANTs
    if os.path.exists(grants_file):
      with open(grants_file, 'r', encoding = 'utf-8') as r:
        content = r.read()
        w.write(content)
        count_lines += content.count('\n')

  # store APEX files in separated patch files
  for file in glob.glob(folders['APEX'] + '/f*' + file_ext_obj):
    short_file, hash_old, hash_new = get_file_details(file, git_root, hashed_old)
    target_file = patch_today.replace(file_ext_obj, '.' + os.path.basename(file))
    #
    if hash_old != hash_new:
      if os.path.exists(target_file):
        os.remove(target_file)
      shutil.copyfile(file, target_file)
      patch_files.append([target_file, ''])

  # create binary to whatever purpose
  if args['zip']:
    with zipfile.ZipFile(patch_zip, 'w', zipfile.ZIP_DEFLATED) as zip:
      zip.write(patch_today)
      patch_files.append([patch_zip, ''])

  # store new hashes for rollout
  content = []
  with open(patch_log, 'w', encoding = 'utf-8') as w:
    for file in sorted(hashed_new.keys()):
      content.append('{} | {}'.format(hashed_new[file], file))
    content = '\n'.join(content) + '\n'
    w.write(content)
    patch_files.append([patch_log, 0])

  # summary, list created files
  print('{:56}{:>8} | {:>8}'.format('', 'LINES', 'BYTES'))
  for (file, count_lines) in patch_files:
    if count_lines == '':
      pass
    elif count_lines == 0:
      count_lines = content.count('\n') + 1
    elif count_lines > 0:
      count_lines += 1
    print('{:>20} | {:30}   {:>8} | {:>8}'.format('', os.path.basename(file), count_lines, os.path.getsize(file)))
  print()



#
# SHOW CHANGED/NEW TABLES AS A MAP
#
if (args['patch'] or args['feature']) and args['verbose']:
  # get table references
  references = {}
  for row in conn.fetch_assoc(query_tables_dependencies):
    references[row.table_name] = row.references.replace(' ', '').split(',') if row.references else []

  # if query for getting sorted tables failed, use alphabetic order
  sorted_flag = True
  if not len(tables_sorted):
    tables_sorted = sorted(references.keys())
    sorted_flag = False

  # show only some tables
  filter_tables = tables_changed + tables_added
  #
  if len(filter_tables):
    print()
    print('TABLE REFERENCES FOR NEW/CHANGED TABLES: ({}{}) {}'.format(*[
      str(len(filter_tables)) + '/' if len(filter_tables) > 0 else '',
      len(references),
      '- NOT SORTED' if not sorted_flag else ''
    ]))
    print('----------------------------------------')
    #
    recent_parent = ''
    for table_name in tables_sorted:
      if (not table_name in references or (not (table_name in filter_tables) and len(filter_tables))):
        continue
      #
      if not len(references[table_name]):
        table_notes.append(' {:>30} | {:<30} | {}'.format(table_name, '', 'NEW' if table_name in tables_added else ''))
      #
      curr_parent = table_name
      for referenced_table in references[table_name]:
        if not (referenced_table in filter_tables) and len(filter_tables):
          continue
        #
        if curr_parent != recent_parent:
          table_notes.append(' {:>30} | {:<30} | {}'.format(curr_parent, '', 'NEW' if curr_parent in tables_added else ''))
          recent_parent = curr_parent
        #
        if referenced_table != recent_parent:
          table_notes.append(' {:>30} | {:<30} | {}'.format('', referenced_table, 'NEW' if referenced_table in tables_added else ''))
      recent_parent = curr_parent
    #
    if len(table_notes):
      content = '\n'.join(table_notes) + '\n'
      print(content)

      # write patch file to notify user about changed tables
      with open(patch_tables, 'w', encoding = 'utf-8') as w:
        w.write('/*\n{}*/\n'.format(content))
  #
  if not args['feature']:
    print()



#
# SHOW LIST OF CHANGED FILES
#
if args['feature'] and not args['patch'] and not args['rollout']:
  print()
  print('CREATING FEATURE BRANCH PATCH:')
  print('------------------------------')
  print()

  # find all unchanged files, sorted by object type
  content = []
  for type in objects_sorted:
    file_found  = False
    files       = sorted(glob.glob(folders[type] + '/*' + file_ext_obj))

    # sort tables in installable order
    if type == 'TABLE':
      files = get_files_sorted(files, tables_sorted)

    # process files
    for file in files:
      short_file, hash_old, hash_new = get_file_details(file, git_root, hashed_old)

      # check package spec vs body (in the same dir)
      if type == 'PACKAGE BODY' and file.endswith(file_ext_spec):   # ignore package spec in body dir
        continue
      if type == 'PACKAGE' and not file.endswith(file_ext_spec):    # ignore package body in spec dir
        continue

      # process only changed files
      if hash_new != hash_old:
        # append type header when first file is found
        if not file_found:
          content.append('--\n-- {}\n--'.format(type))
        file_found = True
        #
        content.append('@@"./{}"'.format(os.path.normpath(file).replace(os.path.normpath(args['target']), '').replace('\\', '/').lstrip('/')))
    #
    if file_found:
      content.append('')

  # append GRANTs
  if os.path.exists(grants_file):
    content.append('--\n-- GRANTS\n--')
    content.append('@@"./{}"'.format(os.path.normpath(grants_file).replace(os.path.normpath(args['target']), '').replace('\\', '/').lstrip('/')))
    content.append('')

  # append APEX files
  changed_files = []
  for file in glob.glob(folders['APEX'] + '/f*' + file_ext_obj):
    short_file, hash_old, hash_new = get_file_details(file, git_root, hashed_old)
    if hash_old != hash_new:
      changed_files.append(file)
  #
  if len(changed_files):
    content.append('--\n-- APEX\n--')
    for file in changed_files:
      content.append('@@"./{}"'.format(os.path.normpath(file).replace(os.path.normpath(args['target']), '').replace('\\', '/').lstrip('/')))
    content.append('')

  # copy objects to the patch file
  content = '\n'.join(content) + '\n'
  with open(patch_today, 'w', encoding = 'utf-8') as w:
    w.write(content)
  #
  print(content)



#
# CONFIRM ROLLOUT - STORE CURRENT HASHES IN A LOG
#
if args['rollout'] and not args['feature']:
  print()
  print('ROLLOUT CONFIRMED:')
  print('------------------')

  # show removed files
  for file in sorted(hashed_old.keys()):
    if not os.path.exists(git_root + '/' + file):
      print('  [-] {}'.format(file))

  # store hashes for next patch
  with open(rollout_log, 'w', encoding = 'utf-8') as w:
    # get files and hashes from patch.log file and overwrite old hashes
    if os.path.exists(patch_log):
      with open(patch_log, 'r', encoding = 'utf-8') as r:
        for line in r.readlines():
          if '|' in line:
            (hash, file) = line.split('|')
            hashed_old[file.strip()] = hash.strip()
            print('  [+] {}'.format(file))
    #
    content = []
    for file in sorted(hashed_old.keys()):
      short_file = file.replace(git_root, '').replace('\\', '/').lstrip('/')
      #
      if os.path.exists(git_root + '/' + file):
        content.append('{} | {}'.format(hashed_old[file], file))
    #
    w.write('\n'.join(content) + '\n')

    # cleanup
    if os.path.exists(patch_log):
      os.remove(patch_log)
  #
  print()
  print()



#
# DELETE UNLOCKED FILES
#
if args['lock'] and args['delete']:
  # delete all database object files except APEX
  for type in objects_sorted:
    for file in sorted(glob.glob(folders[type] + '/*.*')):
      short_file, hash_old, hash_new = get_file_details(file, git_root, hashed_old)
      if not (short_file in locked_objects):
        #print('  {}'.format(short_file))
        os.remove(file)

