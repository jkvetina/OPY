# coding: utf-8
import sys, os, argparse, pickle, timeit, traceback, glob, csv, subprocess, datetime, shutil, zipfile, hashlib
from oracle_wrapper import Oracle
from export_fn import *

#
# ARGS
#
parser = argparse.ArgumentParser()
parser.add_argument('target',                       help = 'Target folder (Git root)')
parser.add_argument('-n', '-name',    '--name',     help = 'Connection name')
parser.add_argument('-t', '-type',    '--type',     help = 'Filter specific object type', default = '')
parser.add_argument('-r', '-recent',  '--recent',   help = 'Filter objects compiled since SYSDATE - $recent')
parser.add_argument('-a', '-app',     '--app',      help = 'APEX application')
parser.add_argument('-c', '-csv',     '--csv',      help = 'Export tables in data/ dor to CSV files',   nargs = '?', default = False, const = True)
parser.add_argument('-v', '-verbose', '--verbose',  help = 'Show object names during export',           nargs = '?', default = False, const = True)
parser.add_argument('-d', '-debug',   '--debug',    help = '',                                          nargs = '?', default = False, const = True)
parser.add_argument('-p', '-patch',   '--patch',    help = 'Prepare patch',                             nargs = '?', default = False, const = True)
parser.add_argument(      '-rollout', '--rollout',  help = 'Mark rollout as done',                      nargs = '?', default = False, const = True)
parser.add_argument('-z', '-zip',     '--zip',      help = 'Patch as ZIP',                              nargs = '?', default = False, const = True)
#
args = vars(parser.parse_args())
args['app']     = int(args['app']     or 0)
args['recent']  = int(args['recent']  or -1)
#
root      = os.path.dirname(os.path.realpath(__file__))
conn_dir  = os.path.abspath(root + '/conn')

# primary connection file
db_conf = os.path.normpath(args['target'] + '/documentation/db.conf')
if args['name']:
  db_conf = os.path.normpath('{}/{}.conf'.format(conn_dir, args['name']))
#
with open(db_conf, 'rb') as f:
  conn_bak = pickle.load(f)

# overwrite target from pickle file
if 'name' in args and len(args['target']) <= 1 and 'target' in conn_bak:
  args['target'] = conn_bak['target']

# check args
if args['debug']:
  print('ARGS:')
  print('-----')
  for (key, value) in args.items():
    if not (key in ('pwd', 'wallet_pwd')):
      print('{:>8} = {}'.format(key, value))
  print('')

# target folders by object types
git_target = os.path.abspath(args['target'] + '/database') + '/'
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

# current dir
rollout_dir   = os.path.normpath(git_target + '../patches')
rollout_done  = os.path.normpath(git_target + '../patches_done')
rolldirs      = ['41_sequences', '42_functions', '43_procedures', '45_views', '44_packages', '48_triggers', '49_indexes']
rolldir_obj   = rollout_dir + '/40_objects---LIVE'
rolldir_man   = rollout_dir + '/20_diffs---MANUALLY'
rolldir_apex  = rollout_dir + '/90_apex_app---LIVE'
today         = datetime.datetime.today().strftime('%Y-%m-%d')
rollout_log   = '{}/{}'.format(rollout_done, 'rollout.log')
patch_file    = '{}/{}.sql'.format(rollout_done, today)
zip_file      = '{}/{}.zip'.format(rollout_done, today)
apex_dir      = folders['APEX']
apex_temp_dir = apex_dir + 'temp/'
apex_ws_files = apex_dir + 'workspace_files/'
apex_tmp      = 'apex.tmp'



#
# CONNECT TO DATABASE
#
start   = timeit.default_timer()
common  = os.path.commonprefix([db_conf, git_target]) or '\\//\\//\\//'
conn    = Oracle(conn_bak)
#
data    = conn.fetch_assoc(query_today, recent = args['recent'] if args['recent'] >= 0 else '')
today   = data[0].today  # calculate date from recent arg
schema  = data[0].curr_user

# find wallet
wallet_file = ''
if 'wallet' in conn_bak:
  wallet_file = conn_bak['wallet']
elif 'name' in conn_bak:
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

# get versions
data = conn.fetch_assoc(query_verions)[0]
print('  DATABASE | {}'.format('.'.join(data.db_version.split('.')[0:2])))
print('      APEX | {}'.format('.'.join(data.apex_version.split('.')[0:2])))
print()

# create basic dirs
for dir in [git_target, rollout_dir, rollout_done, rolldir_obj, rolldir_man, rolldir_apex]:
  if not (os.path.exists(dir)):
    os.makedirs(dir)



#
# PREVIEW OBJECTS
#
data_objects = []
if args['recent'] != 0:
  print()
  print('OBJECTS OVERVIEW:                                      CONSTRAINTS:')
  print('-----------------                                      ------------')
  #
  data_objects = conn.fetch_assoc(query_objects, object_type = args['type'].upper(), recent = args['recent'] if args['recent'] >= 0 else '')
  summary = {}
  for row in data_objects:
    if not (row.object_type) in summary:
      summary[row.object_type] = 0
    summary[row.object_type] += 1
  #
  all_objects = conn.fetch_assoc(query_summary)
  print('                     | CHANGED |   TOTAL')  # fetch data first
  for row in all_objects:
    check = '' if row.object_type in folders else '<--'  # mark not supported object types
    print('{:>20} | {:>7} | {:>7} {:<4} {:>12}{}{:>4}'.format(row.object_type, summary.get(row.object_type, ''), row.object_count, check, row.constraint_type or '', ' | ' if row.constraint_type else '', row.constraint_count or ''))
  #
  print('                             ^')  # to highlight affected objects
  print()



#
# EXPORT OBJECTS
#
if len(data_objects):
  print('EXPORTING OBJECTS: ({}){}'.format(len(data_objects), '\n------------------' if args['verbose'] else ''))
  #
  recent_type = ''
  for (i, row) in enumerate(data_objects):
    object_type, object_name = row.object_type, row.object_name

    # make sure we have target folders ready
    if not (object_type in folders):
      if (args['debug']):
        print('#')
        print('# OBJECT_TYPE_NOT_SUPPOERTED:', object_type)
        print('#\n')
      continue
    #
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
    if obj == None and args['debug']:
      print('#')
      print('# OBJECT_EMPTY:', object_type, object_name)
      print('#\n')
      continue
    #
    if args['verbose']:
      obj_type    = object_type if object_type != recent_type else ''
      obj_name    = object_name if len(object_name) <= 30 else object_name[0:27] + '...'
      obj_length  = len(obj) if obj else ''
      obj_check   = '< NAME' if obj and len(object_name) > 30 else ''
      print('{:>20} | {:<30} {:>8} {}'.format(obj_type, obj_name, obj_length, obj_check))
      recent_type = object_type
    else:
      perc = (i + 1) / len(data_objects)
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
  if not (os.path.isdir(folders['DATA'])):
    os.makedirs(folders['DATA'])
  #
  files = [os.path.splitext(os.path.basename(file))[0] for file in glob.glob(folders['DATA'] + '*.csv')]
  ignore_columns = ['updated_at', 'updated_by', 'created_at', 'created_by', 'calculated_at']
  #
  print()
  print('EXPORT TABLES DATA: ({})'.format(len(files)))
  if args['verbose']:
    print('-------------------')
  #
  for (i, table_name) in enumerate(sorted(files)):
    try:
      table_cols    = conn.fetch(query_csv_columns.format(table_name))[0][0]
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

    # show progress
    if args['verbose']:
      print('  {:30} {:>8}'.format(table_name, len(data)))
    else:
      perc = (i + 1) / len(files)
      dots = int(70 * perc)
      sys.stdout.write('\r' + ('.' * dots) + ' ' + str(int(perc * 100)) + '%')
      sys.stdout.flush()
    #
    writer.writerow(conn.cols)  # headers
    for row in data:
      writer.writerow(row)
    csv_file.close()
  #
  if not args['verbose']:
    print()
  print()



#
# APEX APPLICATIONS OVERVIEW (for the same schema)
#
all_apps  = conn.fetch_assoc(query_apex_applications, schema = conn_bak['user'].upper())
apex_apps = {}
#
if len(all_apps):
  header    = 'APEX APPLICATIONS - {} WORKSPACE:'.format(all_apps[0].workspace)
  #
  print()
  print(header + '\n' + '-' * len(header))
  print('                                                  | PAGES | LAST CHANGE AT')
  for row in all_apps:
    apex_apps[row.application_id] = row
    print('{:>10} | {:<36} | {:>5} | {}'.format(row.application_id, row.application_name[0:36], row.pages, row.last_updated_on))
  print()



#
# EXPORT APEX APP
#
if 'app' in args and args['app'] in apex_apps:
  # recreate temp dir
  if os.path.exists(apex_temp_dir):
    shutil.rmtree(apex_temp_dir)
  os.makedirs(apex_temp_dir)

  # prep target dir
  if args['recent'] <= 0:
    # delete folder to remove obsolete objects only on full export
    apex_dir_app = '{}f{}'.format(apex_dir, args['app'])
    if os.path.exists(apex_dir_app):
      shutil.rmtree(apex_dir_app)
  #
  if not os.path.exists(apex_dir):
    os.makedirs(apex_dir)
  if not os.path.exists(apex_ws_files):
    os.makedirs(apex_ws_files)

  # get app details
  apex = conn.fetch_assoc(query_apex_app_detail.format(args['app']))[0]
  #
  print()
  print('EXPORTING APEX APP:')
  print('-------------------')
  print('         APP | {} {}'.format(apex.app_id, apex.app_alias))
  print('        NAME | {}'.format(apex.app_name))
  print('   WORKSPACE | {:<30}  CREATED AT | {}'.format(apex.workspace, apex.created_at))
  print('   COMPATIB. | {:<30}  CHANGED AT | {}'.format(apex.compatibility_mode, apex.changed_at))
  print()
  print('       PAGES | {:<8}      LISTS | {:<8}    SETTINGS | {:<8}'.format(apex.pages, apex.lists or '', apex.settings or ''))
  print('       ITEMS | {:<8}       LOVS | {:<8}  BUILD OPT. | {:<8}'.format(apex.items or '', apex.lovs or '', apex.build_options or ''))
  print('   PROCESSES | {:<8}  WEB SERV. | {:<8}  INIT/CLEAN | {:<8}'.format(apex.processes or '', apex.ws or '', (apex.has_init_code or '-') + '/' + (apex.has_cleanup or '-')))
  print('     COMPUT. | {:<8}    TRANSL. | {:<8}      AUTH-Z | {:<8}'.format(apex.computations or '', apex.translations or '', apex.authz_schemes or ''))
  print()

  # prepare requests (multiple exports)
  request_conn = ''
  requests = []
  if wallet_file != '' and 'wallet' in conn_bak:
    request_conn += 'set cloudconfig {}.zip\n'.format(wallet_file.rstrip('.zip'))
    request_conn += 'connect {}/"{}"@{}\n'.format(conn_bak['user'], conn_bak['pwd'], conn_bak['service'])
  else:
    request_conn += 'connect {}/"{}"@{}:{}/{}\n'.format(conn_bak['user'], conn_bak['pwd'], conn_bak['host'], conn_bak['port'], conn_bak['sid'])
  #
  if args['recent'] > 0 and os.name != 'nt':
    # partial export, get list of changed objects since that, show it to user
    requests.append('apex export -applicationid {app_id} -list -changesSince {since}')  # -list must be first
    requests.append('apex export -dir {dir} -applicationid {app_id} -changesSince {since} -nochecksum -expType EMBEDDED_CODE')
    requests.append('apex export -dir {dir_temp} -applicationid {app_id} -split -expComponents {changed}')
  else:
    # export app in several formats
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
  if os.name == 'nt':
    # for Windows create one script, full export only
    content = request_conn + '\n\r'
    for request in requests:
      content += request.format(dir = apex_dir, dir_temp = apex_temp_dir, dir_ws_files = apex_ws_files, app_id = args['app']) + '\n\r'

    # create temp file
    with open(apex_tmp, 'w') as f:
      f.write(content + 'exit;')
    #
    process = 'sql /nolog @apex.tmp'
    result  = subprocess.run(process, shell = True, capture_output = True, text = True)
    #
    if os.path.exists(apex_tmp):
      os.remove(apex_tmp)

    # cleanup files after each loop
    clean_apex_files(folders)

  else:
    # for normal platforms
    changed = []
    for (i, request) in enumerate(requests):
      request = request_conn + '\n' + request.format(dir = apex_dir, dir_temp = apex_temp_dir, dir_ws_files = apex_ws_files, app_id = args['app'], since = today, changed = changed)
      process = 'sql /nolog <<EOF\n{}\nexit;\nEOF'.format(request)
      result  = subprocess.run(process, shell = True, capture_output = True, text = True)
      output  = result.stdout.strip()

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
          print('CHANGES SINCE {}: ({})'.format(today, len(changed)))
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
      clean_apex_files(folders)
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
    shutil.rmtree(apex_temp_dir)

# get old hashes
hashed_old = {}
if os.path.exists(rollout_log):
  f = open(rollout_log, 'r')
  for line in f.readlines():
    (file, hash) = line.split('|')
    hashed_old[file.strip()] = hash.strip()



#
# PREPARE PATCH
#
if args['patch']:
  print()
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
          (curr_dir, short) = file.replace(rollout_dir, '').replace('\\', '/').lstrip('/').split('/')
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
  print()
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
      print('{:>20} | {:<36}{}'.format(type if i == 0 else '', file.replace('\\', '/').split('/')[1], flag))

  # store hashes for next patch
  if args['rollout']:
    with open(rollout_log, 'w') as h:
      h.write('\n'.join(sorted(hashed)))
  #
  print()

print('TIME:', round(timeit.default_timer() - start, 2))
print('\n')


