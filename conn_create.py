# coding: utf-8
import sys, os, pickle, argparse
from oracle_wrapper import Oracle



# get passed arguments
parser = argparse.ArgumentParser(add_help = False)
parser.add_argument('-n', '--name',       help = 'Connection name')
parser.add_argument('-u', '--user',       help = 'User name')
parser.add_argument('-p', '--pwd',        help = 'Password (for user or wallet)')
parser.add_argument('-h', '--host',       help = 'Host')
parser.add_argument('-o', '--port',       help = 'Port')
parser.add_argument('-s', '--sid',        help = 'SID')
parser.add_argument('-r', '--service',    help = 'Service name')
parser.add_argument('-w', '--wallet',     help = 'Wallet name')
parser.add_argument('-x', '--wallet_pwd', help = 'Wallet password')
parser.add_argument('-t', '--target',     help = 'Target for Git')
#
args = vars(parser.parse_args())
args = {key: args[key] for key in args if args[key] != None}  # remove empty values

# get root
root = os.path.dirname(os.path.realpath(__file__))
conn_dir = root + '/conn'
pickle_file = '{}/{}.conf'.format(conn_dir, args['name'])
#
if not os.path.exists(conn_dir):
  os.makedirs(conn_dir)

# auto update/append existing file
if os.path.exists(pickle_file):
  with open(pickle_file, 'rb') as f:
    for (arg, value) in pickle.load(f).items():
      if not arg in args:
        args[arg] = value

#
# check wallet for connection name
#
if 'wallet' in args and args['wallet']:
  wallet_dir = '{}/Wallet_{}'.format(conn_dir, args['wallet'])
  if not os.path.isdir(wallet_dir):
    print('#')
    print('# WALLET DIR MISSING', wallet_dir)
    print('#')
    print()
    sys.exit()

  # set defaults
  if not ('service' in args):
    args['service'] = (args['wallet'] + '_medium').lower()

  # store as full path
  args['wallet'] = wallet_dir

  # get DSN from TNS file
  with open(args['wallet'] + '/tnsnames.ora') as f:
    for line in f.readlines():
      if line.startswith(args['service'].lower()):
        args['dsn'] = line.split('=', 1)[1].strip()
  #
  if not ('dsn' in args):
    print('#')
    print('# DSN MISSING', args['service'])
    print('#')
    print()
    sys.exit()



#
# store args into unreadable pickle
#
with open(pickle_file, 'wb') as f:
  pickle.dump(args, f, protocol = pickle.HIGHEST_PROTOCOL)

# check pickle
with open(pickle_file, 'rb') as f:
  args = pickle.load(f)

# check args after merge with current pickle
print('ARGS:\n--')
for (key, value) in args.items():
  if not (key in ('pwd', 'wallet_pwd')):
    print('{:>8} = {}'.format(key, value))
print('')



#
# create target dir
#
if not os.path.exists(args['target']):
  os.makedirs(args['target'])



#
# check connectivity
#
ora = Oracle(args)
data = ora.fetch("SELECT TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') FROM DUAL")
print('CONNECTED:\n--\n  SERVER TIME =', data[0][0])  # data[row][col]
print()

