import sys, os
import cx_Oracle



class Oracle_Cols:
  # we dont do associative arrays due to large overhead
  # instead we use this object to map names to column positions

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
    self.conn = None    # connection
    self.curs = None    # most recent cursor
    self.tns = {
      'user'    : '',
      'pwd'     : '',
      'server'  : '',
      'port'    : 1521,
      'sid'     : '',
      'service' : '',
      'lang'    : '.AL32UTF8',
    }
    self.tns.update(tns)
    self.connect()

  def connect(self):
    os.environ['NLS_LANG'] = self.tns['lang']
    self.conn = cx_Oracle.connect(
      self.tns['user'],
      self.tns['pwd'],
      cx_Oracle.makedsn(
        self.tns['server'], self.tns['port'],
        self.tns['sid'],
        service_name = self.tns['service']
      )
    )

  def execute(self, query, **binds):
    curs = self.curs = self.conn.cursor()
    return curs.execute(query.strip(), **binds)

  def fetch(self, query, limit = 0, **binds):
    curs = self.curs = self.conn.cursor()
    data = curs.execute(query.strip(), **binds).fetchmany(limit)
    desc = {}
    cols = [row[0].lower() for row in curs.description]
    for col_id, col in enumerate(curs.description):
      desc[col[0].lower()] = col
    return (data, Oracle_Cols(cols), desc)

  def commit(self):
    try: self.conn.commit()
    except:
      return

  def rollback(self):
    try: self.conn.rollback()
    except:
      return

