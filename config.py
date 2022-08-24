import os, datetime

# target folders by object types
git_root    = '#ROOT#/'
git_target  = '#ROOT#/database/'
folders     = {
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

# export objects by type in this order
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
today_date    = datetime.datetime.today().strftime('%Y-%m-%d')  # YYYY-MM-DD
patch_root    = '#ROOT#/patches/'
patch_done    = '#ROOT#/patches_done/'
patch_today   = '{}/patch_{}.sql'.format(patch_done, today_date)
patch_zip     = '{}/patch_{}.zip'.format(patch_done, today_date)
patch_log     = '{}/{}'.format(patch_done, 'patch.log')
rollout_log   = '{}/{}'.format(patch_done, 'rollout.log')
locked_log    = '{}/{}'.format(patch_done, 'locked.log')

# patch folders, sorted
patch_folders = {
  'init'      : patch_root + '/10_init/',
  'tables'    : patch_root + '/20_new_tables/',
  'changes'   : patch_root + '/30_table+data_changes/',
  'objects'   : patch_root + '/40_repeatable_objects/',
  'cleanup'   : patch_root + '/50_cleanup/',
  'data'      : patch_root + '/60_data/',           # commit after + refresh stats?
  'grants'    : patch_root + '/70_grants/',
  'jobs'      : patch_root + '/80_jobs/',           # after data
  'finally'   : patch_root + '/90_finally/',
}
#
patch_manually  = '{}{}.sql'.format(patch_folders['changes'], today_date)
file_ext_obj    = '.sql'
file_ext_csv    = '.csv'
file_ext_spec   = '.spec.sql'

# for CSV files dont export audit columns
ignore_columns = ['updated_at', 'updated_by', 'created_at', 'created_by']

# apex folders
apex_dir        = folders['APEX']
apex_temp_dir   = apex_dir + 'temp/'                # temp file for partial APEX exports
apex_ws_files   = apex_dir + 'workspace_files/'
apex_tmp        = 'apex.#.tmp'                      # temp file for running SQLcl on Windows

