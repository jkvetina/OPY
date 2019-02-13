import sys, os
import cx_Oracle



class OracleCols:

  def __init__(self, cols):
    self.cols   = cols
    self.pos    = {}
    self.length = 0   # maximum column_name length
    #
    for pos, name in enumerate(cols):
      self.pos[name]  = pos
      self.length     = max(self.length, len(name))

  def __getattr__(self, name):
    return self.pos[name]

  def __str__(self):
    return(str(self.cols) + ' ' + str(len(self.cols)))



class Oracle:

  def __init__(self, tns):
    self.conn = None    # recent connection link
    self.curs = None    # recent cursor
    self.cols = []      # recent columns mapping (name to position) to avoid associative arrays
    self.desc = {}      # recent columns description (name, type, display_size, internal_size, precision, scale, null_ok)
    self.tns = {
      'user'    : '',
      'pwd'     : '',
      'host'    : '',
      'port'    : 1521,
      'sid'     : None,
      'service' : None,
      'lang'    : '.AL32UTF8',
    }
    self.tns.update(tns)
    self.connect()

  def connect(self):
    os.environ['NLS_LANG'] = self.tns['lang']
    self.tns['dsn'] = cx_Oracle.makedsn(self.tns['host'], self.tns['port'], service_name = self.tns['service']) \
      if self.tns['service'] else cx_Oracle.makedsn(self.tns['host'], self.tns['port'], sid = self.tns['sid'])
    self.conn = cx_Oracle.connect(self.tns['user'], self.tns['pwd'], self.tns['dsn'])

  def execute(self, query, **binds):
    self.curs = self.conn.cursor()
    return self.curs.execute(query.strip(), **binds)

  def fetch(self, query, limit = 0, **binds):
    self.curs = self.conn.cursor()
    data = self.curs.execute(query.strip(), **binds).fetchmany(limit)
    self.cols = [row[0].lower() for row in self.curs.description]
    self.desc = {}
    for row in self.curs.description:
      self.desc[row[0].lower()] = row
    return (data, OracleCols(self.cols))

  def commit(self):
    try: self.conn.commit()
    except:
      return

  def rollback(self):
    try: self.conn.rollback()
    except:
      return

