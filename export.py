# coding: utf-8
import sys, os, argparse, pickle, timeit, traceback, glob, csv, subprocess, shutil, collections, inspect, yaml
from oracle_wrapper import Oracle
from export_fn import *

#
# ARGS
#
parser = argparse.ArgumentParser()
parser.add_argument('-g', '-target',  '--target',   help = 'Target folder (Git root)')
parser.add_argument('-n', '-name',    '--name',     help = 'Connection name')
parser.add_argument('-r', '-recent',  '--recent',   help = 'Filter objects compiled recently',          type = int,   default = -1)
parser.add_argument('-t', '-type',    '--type',     help = 'Filter specific object type',                             default = '',     nargs = '*')
parser.add_argument('-a', '-apex',    '--apex',     help = 'APEX application(s) to export',             type = int,                     nargs = '*')
parser.add_argument('-c', '-csv',     '--csv',      help = 'Export tables in data/ to CSV files',                                       nargs = '*')
parser.add_argument('-v', '-verbose', '--verbose',  help = 'Show object names during export',                                           nargs = '*')
parser.add_argument('-d', '-debug',   '--debug',    help = 'Show some extra stuff when debugging',                    default = False,  nargs = '?',  const = True)
parser.add_argument('-i', '-info',    '--info',     help = 'Show DB/APEX versions and app details',                   default = False,  nargs = '?',  const = True)
parser.add_argument('-p', '-patch',   '--patch',    help = 'Prepare patch (allow to pass env and opt. name)',                           nargs = '+')
parser.add_argument('-o', '-rollout', '--rollout',  help = 'Mark rollout as done (pass env)',                                           nargs = '+')
parser.add_argument('-z', '-zip',     '--zip',      help = 'Patch as ZIP',                                            default = False,  nargs = '?',  const = True)
parser.add_argument(      '-lock',    '--lock',     help = 'Lock existing files into locked.log',                     default = False,  nargs = '?',  const = True)
parser.add_argument(      '-add',     '--add',      help = 'Add new objects/files even when -locked',                                   nargs = '*')
parser.add_argument(      '-delete',  '--delete',   help = 'Delete unchanged files in patch or...',                   default = False,  nargs = '?',  const = True)
parser.add_argument('-f', '-files',   '--files',    help = 'Export app/ws files',                                     default = False,  nargs = '?',  const = True)
parser.add_argument('-e', '-env',     '--env',      help = 'Target environment',                                                        nargs = '?')
parser.add_argument('-x', '-fix',     '--fix',      help = 'Fix iCloud dupe files',                                   default = False,  nargs = '?',  const = True)
#
args = vars(parser.parse_args())

# adjust args for patching
args['env_name']    = args['env'] if 'env' in args else ''
args['patch_name']  = ''
args.pop('env', '')
#
if 'patch' in args and args['patch'] != None:
  if len(args['patch']) > 1:
    args['patch_name']  = args['patch'][1]
  args['env_name']      = args['patch'][0] if not args['env_name'] else args['env_name']
  args['patch']         = True
  args['rollout']       = False
  #
elif 'rollout' in args and args['rollout'] != None:
  args['env_name']      = args['rollout'][0] if not args['env_name'] else args['env_name']
  args['patch']         = False
  args['rollout']       = True
else:
  args['patch']         = False
  args['rollout']       = False

# adjust args for adding new objects
if 'add' in args and args['add'] != None:
  args['add_like']      = '' if args['add'] == [] else args['add'][0]
  args['add']           = True
else:
  args['add_like']      = ''
  args['add']           = False

# adjust args to see changed objects
if 'verbose' in args and args['verbose'] != None:
  args['env_name']      = args['verbose'][0] if len(args['verbose']) and not args['env_name'] else args['env_name']
  args['verbose']       = True
else:
  args['verbose']       = True if args['recent'] == 1 else False
#
args = collections.namedtuple('ARG', args.keys())(*args.values())  # convert to named tuple
#
start_timer = timeit.default_timer()

# check args
if args.debug:
  print('ARGS:')
  print('-----')
  for key, value in sorted(zip(args._fields, args)):
    if not (key in ('pwd', 'wallet_pwd')):
      print('{:>10} = {}'.format(key, value))
  print()



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
  print('#\n# MISSING TARGET\n#\n')
  sys.exit()



#
# LOAD CONFIGURATION
#
config_file = '/config.yaml'
cfg_root    = os.path.normpath(args.target)
cfg_shared  = {}
cfg_project = {}
conf_used   = []
conf_files  = [
  os.path.normpath(conn_dir + '/../' + config_file),
  os.path.normpath(os.path.dirname(__file__) + config_file)
]
#
for conf_file in conf_files:
  if os.path.exists(conf_file):
    with open(conf_file, 'r', encoding = 'utf-8') as f:
      cfg_shared = list(yaml.load_all(f, Loader = yaml.loader.SafeLoader))
      cfg_shared = cfg_shared[0] if len(cfg_shared) > 0 else {}
      conf_used.append(conf_file)
    break

# overload shared config with project specific file
conf_file = os.path.normpath(args.target + config_file)
if os.path.exists(conf_file):
  with open(conf_file, 'r', encoding = 'utf-8') as f:
    cfg_project = list(yaml.load_all(f, Loader = yaml.loader.SafeLoader))
    cfg_project = cfg_project[0] if len(cfg_project) > 0 else {}
    conf_used.append(conf_file)
#
cfg = collections.defaultdict(dict)
cfg.update(cfg_shared)
if cfg_project != {}:
  if args.debug:
    print('')
    print('CONFIG UPDATE:')
    print('--------------')
  for key, nested_dict in cfg_project.items():
    if isinstance(cfg[key], dict):
      cfg[key].update(nested_dict)
    else:
      cfg[key] = nested_dict
    if args.debug:
      print('  ', key, nested_dict)
  if args.debug:
    print('\n')

# normalize paths from config, replace #ROOT# with actual root
for name, value in cfg.items():
  if isinstance(value, dict):
    cfg[name] = {}
    for key, val in value.items():
      cfg[name][key] = get_fixed_path(val, cfg_root, args)
      if isinstance(cfg[name][key], list):
        for idx, val2 in enumerate(cfg[name][key]):
          cfg[name][key][idx] = get_fixed_path(val2, cfg_root, args)
  #
  elif isinstance(value, list):
    cfg[name] = []
    for key, val in enumerate(value):
      cfg[name].append(get_fixed_path(val, cfg_root, args))
  #
  else:
    cfg[name] = get_fixed_path(value, cfg_root, args)

# overwrite args based on config file
if 'auto_lock_add_prefix' in cfg and len(cfg['auto_lock_add_prefix']) > 0:
  args = args._replace(add_like = cfg['auto_lock_add_prefix'])
  args = args._replace(add      = True)
  args = args._replace(lock     = True)
#
if 'auto_filter_prefix' in cfg and len(cfg['auto_filter_prefix']) > 0 and args.type == '':
  args = args._replace(type     = ['%', cfg['auto_filter_prefix']])
#
if 'auto_verbose' in cfg and cfg['auto_verbose']:
  args = args._replace(verbose  = cfg['auto_verbose'])
#
if 'auto_csv_add' in cfg and cfg['auto_csv_add']:
  args = args._replace(csv      = cfg['auto_csv_add'])
elif 'auto_csv_refresh' in cfg and cfg['auto_csv_refresh']:
  args = args._replace(csv      = cfg['auto_csv_refresh'])

# convert to tuple
cfg = collections.namedtuple('CFG', cfg.keys())(*cfg.values())  # convert to named tuple
#
if cfg_shared == {} and cfg_project == {}:
  print('#\n# MISSING CONFIG\n#\n')
  sys.exit()



#
# CLEANUP FILES
# cleanup junk files created on Mac by iCloud sync
#
files = glob.glob(cfg.git_root + '**/* [0-9].*', recursive = True)
for file in files:
  os.remove(file)
  if args.fix:
    print(file.replace(cfg.git_root, ''))
#
if args.fix:
  print('\n  {} files removed\n'.format(len(files)))
  sys.exit()



#
# CONNECT TO DATABASE
#
curr_schema       = connection['user'].upper().split('[')[1].rstrip(']') if '[' in connection['user'] else connection['user'].upper()
grants_made_file  = '{}{}{}'.format(cfg.folders['GRANT'][0], curr_schema, cfg.folders['GRANT'][1])
grants_recd_file  = (os.path.dirname(grants_made_file) + cfg.grants_recd)
grants_privs_file = (os.path.dirname(grants_made_file) + cfg.grants_privs).replace('#SCHEMA_NAME#', curr_schema)
grants_dirs_file  = (os.path.dirname(grants_made_file) + cfg.grants_directories).replace('#SCHEMA_NAME#', curr_schema)
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
  user_home = os.path.expanduser('~')

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
    print('      WALLET | {}'.format(connection['wallet']).replace(user_home, '~'))
  #
  print('             | {}'.format(db_conf.replace(user_home, '~')))
  print('      TARGET | {}'.format(cfg.git_target.replace(user_home, '~')))
  print('      CONFIG | {}'.format(conf_used[0].replace(user_home, '~')))
  if len(conf_used) > 1:
    print('             | {}'.format(conf_used[1].replace(user_home, '~')))
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
for dir in [cfg.git_target, cfg.patch_root, cfg.patch_done, cfg.patch_today, cfg.patch_manually, cfg.rollout_log]:
  dir = os.path.dirname(dir)
  if not os.path.exists(dir):
    os.makedirs(dir)
#
for (type, dir) in cfg.patch_folders.items():
  if not os.path.exists(dir):
    os.makedirs(dir)

# delete old empty patch files
for file in glob.glob(os.path.dirname(cfg.patch_manually) + '/*.sql'):
  if os.path.getsize(file) == 0:
    os.remove(file)

# create new patch file for manual changes (ALTER statements, related data changes...)
if args.patch:
  if not os.path.exists(cfg.patch_manually):
    with open(cfg.patch_manually, 'w', encoding = 'utf-8') as w:
      w.write('')

# switch to alternative log file (typically from PROD when preparing new patch for PROD)
if args.patch and args.patch_name:
  cfg = cfg._replace(patch_today = cfg.patch_named)
#
if args.patch and not os.path.exists(cfg.rollout_log):
  print('#')
  print('# REQUESTED PATCH FILE MISSING')
  print('#', cfg.rollout_log)
  print('#')
  print()

# get old hashes
hashed_old = {}
hashed_new = {}   # files/objects changed since last rollout
cached_obj = {}
#
for hash_file in (cfg.rollout_log, cfg.patch_log):
  if os.path.exists(hash_file):
    with open(hash_file, 'r', encoding = 'utf-8') as r:
      for line in r.readlines():
        (hash, file) = line.split('|')
        if '/' in hash:
          hash, file = file, hash  # swap columns for backward compatibility
        hashed_old[file.strip()] = hash.strip()
    break



#
# PREP LOCKED FILES
#

# process just files in the locked.log file
locked_objects = []
if os.path.exists(cfg.locked_log):
  with open(cfg.locked_log, 'r', encoding = 'utf-8') as r:
    # get list of locked objects
    for shortcut in r.readlines():
      shortcut = shortcut.strip()
      if len(shortcut) > 1 and not (shortcut in locked_objects):
        locked_objects.append(shortcut)

      # remove not existing files
      if args.delete:
        file = os.path.normpath(cfg.git_root + shortcut)
        if not os.path.exists(file):
          print('REMOVING', shortcut)
          locked_objects.remove(shortcut)

# add all existing files to the locked log when just -lock is used
if args.lock and not args.delete and not args.add:
  for object_type in cfg.objects_sorted:
    for file in get_files(object_type, cfg, sort = True):
      obj = get_file_details(object_type, '', file, cfg, hashed_old, cached_obj)
      if not (obj.shortcut in locked_objects):
        locked_objects.append(obj.shortcut)



#
# PREVIEW OBJECTS
#
data_objects      = []
exported_objects  = []
count_objects     = 0
removed_files     = []
adding_files      = []
#
if args.recent != 0 and not args.patch and not args.rollout:
  print()
  print('OBJECTS OVERVIEW:                                      CONSTRAINTS:')
  print('-----------------                                      ------------')

  # retrieve objects in specific order
  sort = ''
  for (i, object_type) in enumerate(cfg.objects_sorted):
    sort += 'WHEN \'{}\' THEN {}'.format(object_type, i)
  #
  binds = {
    'object_type' : args.type[0].upper() if len(args.type) > 0 else '',
    'object_name' : args.type[1].upper().rstrip('%') if len(args.type) > 1 else '',
    'recent'      : args.recent if args.recent >= 0 else ''
  }
  if args.debug:
    print(binds)
  #
  data_objects = conn.fetch_assoc(query_objects.format(sort), **binds)
  summary = {}
  for row in data_objects:
    obj = get_file_details(row.object_type, row.object_name, '', cfg, hashed_old, cached_obj)
    if obj == {}:
      continue

    # show just locked files
    if (len(locked_objects) or args.lock):
      if not (obj.shortcut in locked_objects):
        if args.add and len(args.add_like) > 0 and row.object_name.startswith(args.add_like):     # add new files to the locked list
          locked_objects.append(obj.shortcut)
          adding_files.append(obj.shortcut)
        elif args.add and len(args.add_like) == 0 and (obj.hash_old == '' or row.object_name.startswith(args.add_like)):     # add new files to the locked list
          locked_objects.append(obj.shortcut)
          adding_files.append(obj.shortcut)
        else:
          continue  # skip files not on the locked list
    #
    if row.object_type in cfg.folders:
      if not (row.object_type) in summary:
        summary[row.object_type] = 0
      summary[row.object_type] += 1
      count_objects += 1
  #
  all_objects = conn.fetch_assoc(query_summary, object_name = binds['object_name'])
  print('                       EXPORTING |   TOTAL')
  for row in all_objects:
    if row.object_count:
      print('{:>20} | {:>9} | {:>7} {:<6} {:>12}{}{:>4}'.format(*[
        row.object_type,
        summary.get(row.object_type, ''),
        row.object_count,
        '' if row.object_type in cfg.folders else '[N/A]',
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
  header = 'EXPORTING {}OBJECTS: ({})'.format('LOCKED ' if (len(locked_objects) or args.lock) else '', count_objects)
  print(header)
  if (args.verbose or args.recent == 1):
    print('-' * len(header.split(':')[0]) + '-')
    print('{:54}{:>8} | {:>8}'.format('', 'LINES', 'BYTES'))

  # cleanup target folders (to cleanup Git from removed objects)
  if args.recent < 0 and args.delete:
    for object_type in cfg.folders.keys():
      if object_type in ('APEX', 'DATA'):
        continue
      for file in get_files(object_type, cfg, sort = False):
        os.remove(file)

  # go thru objects
  recent_type = ''
  for (i, row) in enumerate(data_objects):
    # make sure we have target folders ready
    if not (row.object_type in cfg.folders):
      if args.debug:
        print('#')
        print('# OBJECT_TYPE_NOT_SUPPORTED:', row.object_type, row.object_name)
        print('#\n')
      continue

    # prepare shortcut before we even create the file
    obj = get_file_details(row.object_type, row.object_name, '', cfg, hashed_old, cached_obj)

    # check locked objects
    flag = ''
    if (len(locked_objects) or args.lock or args.add):
      if (obj.shortcut in adding_files):
        flag = 'ADDING'
      elif not (obj.shortcut in locked_objects):
        continue                                  # skip files not on the locked list

    # make sure we have target folders ready
    file_folder = os.path.dirname(obj.file)
    if not (os.path.isdir(file_folder)):
      os.makedirs(file_folder)

    # check object
    content = get_object(conn, obj.type, obj.name)
    if content == None and args.debug:
      print('#')
      print('# OBJECT_EMPTY:', obj.type, obj.name)
      print('#\n')
      continue
    #
    if (args.verbose or args.recent == 1):
      if flag == '' and obj.type == 'TABLE':
        flag = 'NEW' if obj.hash_old == '' else 'ALTERED' if obj.hash_old != obj.hash_new else ''
      elif flag == '':
        flag = 'NEW' if obj.hash_old == '' else 'CHANGED' if obj.hash_old != obj.hash_new else ''
      #
      if obj.type != recent_type and recent_type != '':
        print('{:>20} |'.format(''))
      print('{:>20} | {:<30} {:>8} | {:>8} {}'.format(*[
        obj.type if obj.type != recent_type else '',
        obj.name if len(obj.name) <= 30 else obj.name[0:27] + '...',
        (content.count('\n') + 1) if content else 0,                                    # count lines
        len(content) if content else '',                                                # count bytes
        flag
      ]))
      recent_type = obj.type
    elif count_objects > 0:
      perc = min((i + 1) / count_objects, 1)
      dots = int(70 * perc)
      sys.stdout.write('\r' + ('.' * dots) + ' ' + str(int(perc * 100)) + '%')
      sys.stdout.flush()

    # call cleanup function for specific object type, if exists
    lines = get_lines(content)
    cleanup_fn = 'clean_' + obj.type.replace(' ', '_').lower()
    if getattr(sys.modules[__name__], cleanup_fn, None):
      lines = getattr(sys.modules[__name__], cleanup_fn)(object_name = obj.name, lines = lines, schema = schema, cfg = cfg)
    content = '\n'.join(lines)

    # prepend silent object drop
    if obj.type in cfg.drop_objects:
      content = template_object_drop.lstrip().format(object_type = obj.type, object_name = obj.name) + content
    elif obj.type in cfg.drop_objects_mview_log:
      content = template_object_drop_mview_log.lstrip().format(object_name = obj.name) + content

    # append comments
    if obj.type in ('TABLE', 'VIEW', 'MATERIALIZED VIEW'):
      content += get_object_comments(conn, obj.name)

    # fill in job template
    if obj.type in ('JOB',):
      content = get_job_fixed(obj.name, content, conn)

    # write object to file
    content = content.rstrip()
    if content.rstrip('/') != content:
      content = content.rstrip('/').rstrip() + '\n/'
    #
    with open(obj.file, 'w', encoding = 'utf-8') as w:
      w.write(content + '\n\n')
    exported_objects.append(obj.shortcut)
  #
  if not (args.verbose or args.recent == 1):
    print()
  else:
    print('{:>20} |'.format(''))
  print()



#
# EXPORT DATA
#
if (args.csv or isinstance(args.csv, list)) and not args.patch and not args.rollout and not (args.apex or isinstance(args.apex, list)):
  if not (os.path.isdir(cfg.folders['DATA'][0])):
    os.makedirs(cfg.folders['DATA'][0])

  # export/refresh existing files
  tables      = []
  table_files = {}  # to keep flags and use them in MERGE statements
  #
  for file in get_files('DATA', cfg, sort = True):
    # basically we need to extract table_name from the filename
    table_name = os.path.basename(file).split('.')[0]
    tables.append(table_name)
    table_files[table_name] = file

  # overwrite prefix when requesting specific object type
  if len(args.type) > 1:
    args        = args._replace(csv = [args.type[1]])
    tables      = []
    table_files = {}

  # when passing values to -csv arg, find relevant tables
  if (isinstance(args.csv, list) and len(args.csv) or cfg.auto_csv_refresh):
    if not (cfg.auto_csv_refresh):
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
    print('-------------------')
    print('{:>8} | {:>3} | {:>3} | {:<30}   {:>6} | {:>8}'.format('INS', 'UPD', 'DEL', '', 'LINES', 'BYTES'))
  #
  for (i, table_name) in enumerate(tables):
    obj = get_file_details('DATA', table_name, '', cfg, hashed_old, cached_obj)

    # create file for new tables
    if not (table_name in table_files):
      table_files[table_name] = obj.file
    file = table_files[table_name]
    #
    try:
      table_cols    = conn.fetch_value(query_csv_columns, table_name = table_name)
      table_exists  = conn.fetch('SELECT {} FROM {} WHERE ROWNUM = 1'.format(table_cols, table_name))
    except Exception:
      if args.verbose:
        print('{:74}REMOVED'.format(table_name))
      if os.path.exists(file):
        os.remove(file)
      continue
    #
    csv_file  = open(file, 'w', encoding = 'utf-8')
    writer    = csv.writer(csv_file, delimiter = ';', lineterminator = '\n', quoting = csv.QUOTE_NONNUMERIC)
    columns   = [col for col in conn.cols if not (col in cfg.ignore_columns)]
    order_by  = ', '.join([str(i) for i in range(1, min(len(columns), 5) + 1)])

    # filter table rows if requested
    where_filter = ''
    if table_name.upper() in cfg.csv_export_filters:
      where_filter = ' WHERE ' + cfg.csv_export_filters[table_name.upper()]

    # fetch data from table
    try:
      query = 'SELECT {} FROM {}{} ORDER BY {}'.format(', '.join(columns), table_name, where_filter, order_by)
      data  = conn.fetch(query)
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

    # add CSV file to the locked.log
    if (len(locked_objects) or args.lock):
      if not (obj.shortcut in locked_objects):
        locked_objects.append(obj.shortcut)

    # show progress
    if args.verbose:
      print('   {:1} {:>3} | {:>3} | {:>3} | {:30}   {:>6} | {:>8} {}'.format(*[
        '*' if len(where_filter) else '',
        ' Y ' if (cfg.merge_insert in file or cfg.merge_auto_insert) else '',
        ' Y ' if (cfg.merge_update in file or cfg.merge_auto_update) else '',
        ' Y ' if (cfg.merge_delete in file or cfg.merge_auto_delete) else '',
        table_name.upper(),
        len(data),                # lines
        os.path.getsize(file),    # bytes
        'NEW' if obj.hash_old == '' else 'CHANGED' if obj.hash_new != obj.hash_old else ''
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
  for file in get_files('DATA', cfg, sort = True):
    table_name  = os.path.basename(file).split('.')[0]
    target_file = cfg.patch_folders['data'] + table_name + '.sql'
    skip_insert = '' if (cfg.merge_insert in file or cfg.merge_auto_insert) else '--'
    skip_update = '' if (cfg.merge_update in file or cfg.merge_auto_update) else '--'
    skip_delete = '' if (cfg.merge_delete in file or cfg.merge_auto_delete) else '--'

    # re-apply filter to remove correct rows when requested
    where_filter = ''
    if table_name.upper() in cfg.csv_export_filters:
      where_filter = '\n{}WHERE {}'.format(skip_delete, cfg.csv_export_filters[table_name.upper()])
    #
    content = get_merge_from_csv(file, conn, skip_insert, skip_update, skip_delete, where_filter)
    if content:
      with open(target_file, 'w', encoding = 'utf-8') as w:
        w.write(content)
        all_data += '{}\n\n\n'.format(content)
  #
  with open(cfg.patch_folders['data'] + '/__.sql', 'w', encoding = 'utf-8') as w:
    w.write(all_data + 'COMMIT;\n\n')



#
# EXPORT GRANTS
#
# @TODO: export also credentials, ACL...
#
if args.recent != 0 and not args.patch and not args.rollout:
  last_type   = ''
  content     = []
  #
  for row in conn.fetch_assoc(query_grants_made):
    # limit to objects on the locked.log
    if (len(locked_objects) or args.lock):
      if not row.type in cfg.folders:  # skip unsupported object types
        continue
      #
      obj = get_file_details('GRANT', row.table_name, '', cfg, hashed_old, cached_obj)
      if not obj.shortcut in locked_objects:
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
    with open(grants_recd_file.replace('#SCHEMA_NAME#', owner), 'w', encoding = 'utf-8') as w:
      w.write(('\n'.join(content) + '\n').lstrip())

  # privileges granted to user
  content = ''
  for row in conn.fetch_assoc(query_user_roles):
    content += row.line + '\n'
  content += '--\n'
  for row in conn.fetch_assoc(query_user_privs):
    content += row.line + '\n'
  #
  with open(grants_privs_file, 'w', encoding = 'utf-8') as w:
    w.write(content.lstrip('--\n') + '\n')

  # export directories
  content = ''
  for row in conn.fetch_assoc(query_directories):
    content += row.line + '\n'
  #
  with open(grants_dirs_file, 'w', encoding = 'utf-8') as w:
    w.write((content + '\n').lstrip())



#
# APEX APPLICATIONS OVERVIEW (for the same schema)
#
apex_apps = {}
if (args.apex or isinstance(args.apex, list)) and not args.patch and not args.rollout:
  all_apps  = conn.fetch_assoc(query_apex_applications, schema = curr_schema)
  workspace = ''
  #
  for row in all_apps:
    if args.apex == []:
      if (len(locked_objects) or args.lock):
        if not os.path.exists('{}f{}.sql'.format(cfg.apex_dir, row.application_id)):
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
    if os.path.exists(cfg.apex_temp_dir):
      shutil.rmtree(cfg.apex_temp_dir, ignore_errors = True, onerror = None)
    os.makedirs(cfg.apex_temp_dir)

    # remove app/ws files
    app_dir = os.path.normpath(cfg.apex_app_files.replace('#APP_ID#', str(app_id)))
    if (cfg.apex_files or args.files):
      if os.path.exists(cfg.apex_ws_files):
        shutil.rmtree(cfg.apex_ws_files, ignore_errors = True, onerror = None)
      #
      if os.path.exists(app_dir):
        shutil.rmtree(app_dir, ignore_errors = True, onerror = None)

    # delete folder to remove obsolete objects only on full export
    apex_app_folder = '{}f{}'.format(cfg.apex_dir, app_id)
    if os.path.exists(apex_app_folder):
      if (cfg.apex_files or args.files):
        shutil.rmtree(apex_app_folder, ignore_errors = True, onerror = None)
      else:
        # we need to skip app/ws files
        dirs = [x[0] for x in os.walk(apex_app_folder)]
        for d in dirs:
          if not (app_dir in d or d in app_dir):
            shutil.rmtree(d, ignore_errors = True, onerror = None)
        #
        files = glob.glob(apex_app_folder + '/*.*', recursive = False)
        for f in files:
          os.remove(f)

    # create empty dirs
    if not os.path.exists(cfg.apex_dir):
      os.makedirs(cfg.apex_dir)
    if not os.path.exists(cfg.apex_ws_files):
      os.makedirs(cfg.apex_ws_files)

    # get app details
    conn.execute(query_apex_security_context, app_id = app_id)
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
      'AUTHZ'   : query_apex_authz_schemes,
      'LOV'     : query_apex_lov_names,
      'GROUPS'  : query_apex_page_groups,
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
        connection['sid'] if 'sid' in connection else connection['service']
      ])

    # always do full APEX export, but when -r > 0 then show changed components
    if args.recent > 0 and cfg.apex_show_changes:
      # partial export, get list of changed objects since that, show it to user
      requests.append('apex export -applicationid {app_id} -list -changesSince {since}')  # -list must be first

    # export full app in several formats
    if cfg.apex_splited:
      requests.append('apex export -dir {dir} -applicationid {app_id} -nochecksum -skipExportDate -expComments -expTranslations -expType APPLICATION_SOURCE{apex_json}{apex_yaml}{apex_embed} -split')
    if cfg.apex_full:
      requests.append('apex export -dir {dir} -applicationid {app_id} -nochecksum -skipExportDate -expComments -expTranslations')

    # trade progress for speed, creating all the JVM is so expensive
    if not args.debug:
      requests = ['\n'.join(requests)]

    # export APEX stuff
    apex_tmp      = cfg.apex_tmp.replace('#APP_ID#', '{}'.format(app_id))  # allow to export multiple apps at the same time
    apex_full     = cfg.apex_full_file.replace('#APP_ID#', '{}'.format(app_id))
    apex_readable = cfg.apex_readable.replace('#APP_ID#', '{}'.format(app_id))
    changed = []
    for (i, request) in enumerate(requests):
      replace_list = {
        'dir'           : cfg.apex_dir,
        'dir_temp'      : cfg.apex_temp_dir,
        'dir_ws_files'  : cfg.apex_ws_files,
        'app_id'        : app_id,
        'since'         : req_today,
        'changed'       : changed,
        'apex_json'     : ',READABLE_JSON' if cfg.apex_readable_json else '',
        'apex_yaml'     : ',READABLE_YAML' if cfg.apex_readable_yaml else '',
        'apex_embed'    : ',EMBEDDED_CODE' if cfg.apex_embedded      else ''
      }
      request = request_conn + '\n' + request.format(**replace_list)
      process = 'sql /nolog <<EOF\n{}\nexit;\nEOF'.format(request)  # for normal platforms

      # for Windows create temp file
      if os.name == 'nt':
        process = 'sql /nolog @' + apex_tmp
        with open(apex_tmp, 'w', encoding = 'utf-8') as w:
          w.write(request + '\nexit;')

      # run SQLcl and capture the output
      result  = subprocess.run(process, shell = True, capture_output = True, text = True)
      output  = (result.stdout or '').strip()

      # for Windows remove temp file
      if os.name == 'nt' and os.path.exists(apex_tmp) and not args.debug:
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
        print('--\n-- REQUEST:\n--\n' + process, request if not (request in process) else '')
        print('--\n-- RESULT:\n--\n'  + output)
      else:
        perc = (i + 1) / len(requests)
        dots = int(70 * perc)
        sys.stdout.write('\r' + ('.' * dots) + ' ' + str(int(perc * 100)) + '%')
        sys.stdout.flush()

      # cleanup files after each loop
      clean_apex_files(app_id, apex_replacements, default_authentication, cfg)
    #
    print('\n')

    # move readable files
    if len(apex_readable):
      from_dir = os.path.normpath(apex_readable + 'readable/')
      #
      if os.path.exists(from_dir):
        shutil.copytree(from_dir, apex_readable, dirs_exist_ok = True)
        shutil.rmtree(from_dir, ignore_errors = True, onerror = None)

      # move some files close to the original files
      for file_type in ('yaml', 'json'):
        # full app file
        file = '{}application/f{}.{}'.format(apex_readable, app_id, file_type)
        file = file.replace('\\', '/').replace('//', '/')
        if os.path.exists(file):
          os.rename(file, file.replace('application/', ''))
        # individual pages
        for file in glob.glob('{}application/pages/*.{}'.format(apex_readable, file_type)):
          file = file.replace('\\', '/').replace('//', '/')
          os.rename(file, file.replace('/pages/p', '/pages/page_'))

    # move APEX full export file
    if len(apex_full):
      if os.path.exists(apex_full):
        os.remove(apex_full)
      file = '{}f{}.sql'.format(cfg.apex_dir, app_id)
      if os.path.exists(file):
        os.makedirs(os.path.dirname(apex_full), exist_ok = True)
        os.rename(file, apex_full)

    # export APEX app and workspace files (app_id=0) in a RAW format
    if (cfg.apex_files or args.files):
      print()
      print('EXPORTING FILES:')
      print('----------------')
      #
      for loop_app_id in (app_id, 0):
        files = conn.fetch_assoc(query_apex_files, app_id = loop_app_id)
        print('{:>12} | {}'.format(loop_app_id if loop_app_id != 0 else 'WORKSPACE', len(files)))
        #
        if len(files):
          for row in files:
            file = cfg.apex_app_files.replace('#APP_ID#', str(loop_app_id)) + row.filename
            if loop_app_id == 0:
              file = cfg.apex_ws_files + row.filename
            #
            os.makedirs(os.path.dirname(file), exist_ok = True)
            #
            with open(file, 'wb') as w:
              if args.debug:
                print('    ' + row.filename)
              w.write(row.f.read())  # blob_content
          if args.debug:
            print()
      print()

      # rename workspace files
      ws_files = 'files_{}.sql'.format(apex_apps[app_id].workspace_id)
      if os.path.exists(cfg.apex_ws_files + ws_files):
        target_file = '{}{}.sql'.format(cfg.apex_ws_files, apex_apps[app_id].workspace)
        if os.path.exists(target_file):
          os.remove(target_file)
        os.rename(cfg.apex_ws_files + ws_files, target_file)

      # move some changed files to proper APEX folder
      apex_partial = '{}f{}'.format(cfg.apex_temp_dir, app_id)
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
        shutil.copytree(apex_partial, '{}f{}'.format(cfg.apex_dir, app_id), dirs_exist_ok = True)

    # cleanup
    if os.path.exists(cfg.apex_temp_dir):
      shutil.rmtree(cfg.apex_temp_dir, ignore_errors = True, onerror = None)



#
# UPDATE LOCKED FILE
#
if (len(locked_objects) or args.lock):
  content = '\n'.join(sorted(locked_objects)) + '\n'
  with open(cfg.locked_log, 'w', encoding = 'utf-8') as w:
    w.write(content)

# delete all database object files not on the list and except APEX folder
if args.lock and args.delete:
  for object_type in cfg.objects_sorted:
    for file in get_files(object_type, cfg, sort = True):
      obj = get_file_details(object_type, '', file, cfg, hashed_old, cached_obj)
      if not (obj.shortcut in locked_objects):
        os.remove(file)



#
# SHOW TIMER
#
if count_objects or apex_apps != {} or (args.csv or isinstance(args.csv, list)):
  print('TIME:', round(timeit.default_timer() - start_timer, 2))



#
# PREPARE PATCH
#
if args.patch:
  header = 'PREPARING PATCH AT {}:'.format(get_file_shortcut(cfg.patch_today, cfg))
  print()
  print(header)
  print('-' * len(header))
  print()

  # remove target patch files
  for file in (cfg.patch_today, cfg.patch_zip):
    if os.path.exists(file):
      os.remove(file)

  # cleanup old patches
  for file in glob.glob(cfg.patch_done + '/*.sql'):
    os.remove(file)
  #
  for file in glob.glob(cfg.patch_folders['changes'] + '/*.sql'):
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
  patch_notes       = {}
  patch_content     = []

  # start with tables, get referenced tables for each table
  for row in conn.fetch_assoc(query_tables_dependencies):
    table_relations[row.table_name] = row.references.split(', ')

  # get list of changed objects and their references
  for object_type in cfg.objects_sorted:
    for file in get_files(object_type, cfg, sort = True):
      obj = get_file_details(object_type, '', file, cfg, hashed_old, cached_obj)

      # check if object changed
      if obj.hash_old == obj.hash_new:              # ignore unchanged objects
        continue
      #
      object_code = '{}.{}'.format(obj.type, obj.name)
      #
      references_todo[object_code]  = []
      references[object_code]       = []
      changed_objects.append(object_code)
      #
      if obj.type in ('TABLE', 'DATA'):
        tables_todo.append(obj.name)                # to process tables first
        #
        if obj.name in table_relations:
          for table_name in table_relations[obj.name]:
            ref_object = '{}.{}'.format('TABLE', table_name)
            references_todo[object_code].append(ref_object)
            references[object_code].append(ref_object)
      else:
        for row in conn.fetch_assoc(query_objects_before, object_name = obj.name, object_type = obj.type):
          ref_object = '{}.{}'.format(row.type, row.name)
          references_todo[object_code].append(ref_object)
          references[object_code].append(ref_object)

  # sort objects to have them in correct order
  for i in range(0, 50):                            # adjust depending on your depth
    for obj, refs in references_todo.items():
      if obj in ordered_objects:                    # object processed
        continue

      # process tables first, first 20 rounds just for tables
      object_type, object_name = obj.split('.')
      if (object_type != 'TABLE' and i <= 20) or (object_type == 'TABLE' and i > 20):
        continue

      # find references
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
  for object_code in ordered_objects:
    if not (object_code in references):             # ignore unknown objects
      continue
    #
    object_type, object_name = object_code.split('.')
    #
    # @TODO: ^ switch this to shortcut
    #
    obj = get_file_details(object_type, object_name, '', cfg, hashed_old, cached_obj)
    #
    processed_names.append(object_code)             # to final check if order is correct
    processed_objects.append(obj)
    hashed_new[obj.shortcut] = obj.hash_new         # store value for new patch.log
    #
    if not (object_type in patch_notes):
      patch_notes[object_type] = []
    patch_notes[object_type].append(object_name)

    # QA check
    if args.verbose:
      for ref_object in references[object_code]:
        if ref_object != object_code and ref_object in changed_objects:
          object_type, object_name = ref_object.split('.')
          object_line = '{:<30} {}'.format((object_name + ' ').ljust(32, '.'), object_type[0:12])
          if not (ref_object in processed_names):
            object_line = (object_line + ' <').ljust(48, '-') + ' MISSING OBJECT'
            print('{:<20} |   > {}'.format('', object_line))

  # show changed data files
  for object_type in ('DATA',):
    files = get_files(object_type, cfg, sort = True)
    found = []
    for file in files:
      obj = get_file_details(object_type, '', file, cfg, hashed_old, cached_obj)
      if obj.hash_old != obj.hash_new:
        found.append(obj.name)
        hashed_new[obj.shortcut] = obj.hash_new
    #
    for table_name in found:
      if not (object_type in patch_notes):
        patch_notes[object_type] = []
      patch_notes[object_type].append(table_name)

  # create list of files to process
  processed_files = []
  for target_dir in sorted(cfg.patch_folders.values()):
    type    = next((type for type, dir in cfg.patch_folders.items() if dir == target_dir), None)
    files   = glob.glob(target_dir + '/*.sql')

    # sort data files into installable order
    if type == 'data':
      tables_map = {}
      found_done = []
      found_todo = []
      #
      for file in files:
        table_name = os.path.basename(file).split('.')[0].upper()
        if not (table_name in ('__',)):
          tables_map[table_name] = file
          found_todo.append(table_name)
      #
      for i in range(0, 1000):
        if not len(found_todo):
          break
        for table_name in tables_map.keys():
          file = tables_map[table_name]
          if table_name in table_relations:
            related_found = False
            for related_table in table_relations[table_name]:
              if related_table in found_todo and related_table != table_name:
                related_found = True
                break
            if related_found:
              continue
          if not (file in found_done):
            found_done.append(file)
          if table_name in found_todo:
            found_todo.remove(table_name)
      #
      files = found_done  # overwrite with sorted files

    # remove processed files
    if type in cfg.patch_tracked:
      for file in ([] + files):  # to modify original list
        shortcut = get_file_shortcut(file, cfg)
        hash_old = hashed_old[shortcut] if shortcut in hashed_old else ''
        hash_new = get_file_hash(file)
        #
        if hash_old == hash_new:                      # ignore unchanged files
          files.remove(file)
        #
        if os.path.basename(shortcut) == '__.sql':    # ignore file with all data files merged
          files.remove(file)

    # process files in patch folder first
    if len(files):
      patch_content.append('\n--\n-- {}\n--'.format(type.upper()))
      for file in files:
        shortcut = get_file_shortcut(file, cfg)
        patch_content.append(cfg.patch_line.format(shortcut))
        processed_files.append(shortcut)
        #
        if type in cfg.patch_tracked:
          hashed_new[shortcut] = get_file_hash(file)
      #
      if type == 'data':
        patch_content.append('--\nCOMMIT;')

    # add objects mapped to current patch folder
    if type in cfg.patch_map:
      header_printed = False
      for obj in processed_objects:
        if not (obj.type in cfg.patch_map[type]):     # ignore non related types
          continue
        if not (obj.type in cfg.folders):             # ignore unknown types
          continue
        if obj.shortcut in processed_files:           # ignore processed objects/files
          continue
        #
        if not header_printed:
          header_printed = True
          if len(files):
            patch_content.append('--')                # shorter splitter when there are files in patch folder
          else:
            patch_content.append('\n--\n-- {}\n--'.format(type.upper()))
        #
        patch_content.append(cfg.patch_line.format(obj.patch_file))
        processed_files.append(obj.shortcut)

  # append (changed) APEX apps
  apex_apps = glob.glob(cfg.folders['APEX'][0] + '/f*' + cfg.folders['APEX'][1])
  apex_apps += glob.glob(cfg.apex_full_file.replace('#APP_ID#', '*'))
  #
  for file in ([] + apex_apps):
    shortcut = get_file_shortcut(file, cfg)
    hashed_new[shortcut] = get_file_hash(file)
    if hashed_old.get(shortcut, '') == hashed_new[shortcut]:
      apex_apps.remove(file)
  #
  if len(apex_apps):
    patch_content.append('\n--\n-- APEX\n--')
    for file in apex_apps:
      obj = get_file_details('APEX', '', file, cfg, hashed_old, cached_obj)
      processed_files.append(obj.shortcut)
      patch_content.append(cfg.patch_line.format(obj.patch_file))
      hashed_new[obj.shortcut] = obj.hash_new
  patch_content.append('')

  # from processed files add all starting with date
  start_with_date = re.compile('^([0-9]{4}-[0-9]{2}-[0-9]{2})')
  for shortcut in processed_files:
    if not (shortcut in hashed_new):
      if start_with_date.match(os.path.basename(shortcut)):
        hashed_new[shortcut] = get_file_hash(cfg.git_root + shortcut)

  # add files from changes folder
  for file in glob.glob(cfg.patch_folders['changes'] + '/*.sql'):
    shortcut = get_file_shortcut(file, cfg)
    hashed_new[shortcut] = get_file_hash(file)

  # store new hashes for rollout
  content = []
  with open(cfg.patch_log, 'w', encoding = 'utf-8') as w:
    for file in sorted(hashed_new.keys()):
      content.append('{} | {}'.format(hashed_new[file], file))
    content = '\n'.join(content) + '\n'
    w.write(content)

  # show sorted overview
  patch_log = []
  for object_type in cfg.objects_sorted:
    if object_type in patch_notes:
      for object_name in sorted(patch_notes[object_type]):
        obj   = get_file_details(object_type, object_name, '', cfg, hashed_old, cached_obj)
        flag  = '[+]' if obj.hash_old == '' else 'ALTERED' if obj.hash_old != obj.hash_new and object_type == 'TABLE' else ''
        #
        patch_log.append('{:>20} | {:<46}{:>8}'.format(object_type if last_type != object_type else '', object_name, flag))
        last_type = object_type
      patch_log.append('{:<20} |'.format(''))

  # show to user and store in the patch file
  print('\n'.join(patch_log))
  print('\n'.join(patch_content))
  #
  with open(cfg.patch_today, 'w', encoding = 'utf-8') as w:
    w.write('--\n--' + '\n--'.join(patch_log) + '\n' + '\n'.join(patch_content) + '\n')

  # create binary to whatever purpose
  if args.zip:
    if os.path.exists(cfg.patch_zip):
      os.remove(cfg.patch_zip)
    #
    shutil.make_archive(cfg.git_root + 'patch', 'zip', cfg.git_root)  # everything in folder
    os.rename(cfg.git_root + 'patch.zip', cfg.patch_zip)



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
      if not os.path.exists(cfg.git_root + file):
        print('  [-] {}'.format(file))

  # store hashes for next patch
  with open(cfg.rollout_log, 'w', encoding = 'utf-8') as w:
    # get files and hashes from patch.log file and overwrite old hashes
    if os.path.exists(cfg.patch_log):
      with open(cfg.patch_log, 'r', encoding = 'utf-8') as r:
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
      # ignore/remove non existing files only on -delete mode
      if args.delete and not os.path.exists(cfg.git_root + file):
        continue
      content.append('{} | {}'.format(hashed_old[file], file))
    #
    w.write('\n'.join(content) + '\n')

    # cleanup
    if os.path.exists(cfg.patch_log):
      os.remove(cfg.patch_log)

# make some space
print('\n')

