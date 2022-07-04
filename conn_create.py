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
parser.add_argument('-w', '--wallet',     help = 'Wallet path')
parser.add_argument('-x', '--wallet_pwd', help = 'Wallet password')
#
args = vars(parser.parse_args())
args = {key: args[key] for key in args if args[key] != None}  # remove empty values

# get root
root = os.path.dirname(os.path.realpath(__file__))
conn_dir = '/conn'
pickle_file = '{}{}/{}.conf'.format(root, conn_dir, args['name'])

# check wallet for connection name
wallet_dir = '{}{}/Wallet_{}'.format(root, conn_dir, args['name'])
if os.path.isdir(wallet_dir):
  args['wallet'] = wallet_dir

  # set defaults
  if not ('service' in args):
    args['service'] = (args['name'] + '_medium').lower()

  # get DSN from TNS file
  with open(args['wallet'] + '/tnsnames.ora') as f:
    for line in f.readlines():
      if line.startswith(args['service'].lower()):
        args['dsn'] = line.split('=', 1)[1].strip()

# check args
print('ARGS:\n--')
for (key, value) in args.items():
  if not (key in ('pwd', 'wallet_pwd')):
    print('{:>8} = {}'.format(key, value))
print('')



# store args into unreadable pickle
with open(pickle_file, 'wb') as f:
  pickle.dump(args, f, protocol = pickle.HIGHEST_PROTOCOL)

# check pickle
with open(pickle_file, 'rb') as f:
  args = pickle.load(f)



# check connectivity
ora = Oracle(args)
data = ora.fetch("SELECT TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') FROM DUAL")
print('CONNECTED:\n--\n  SERVER TIME =', data[0][0])  # data[row][col]
print()

