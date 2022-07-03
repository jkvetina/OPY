import sys, re
from export_queries import *



def get_object(conn, object_type, object_name):
  # get object from database
  if object_type == 'JOB':
    desc = conn.fetch(query_describe_job, object_name = object_name)
  else:
    desc = conn.fetch(query_describe_object, object_type = object_type, object_name = object_name)
  #
  return re.sub('\t', '    ', str(desc[0][0]).strip())  # replace tabs with 4 spaces



def replace(subject, pattern, replacement, flags = 0):
  return re.compile(pattern, flags).sub(replacement, subject)



def get_lines(obj):
  lines = [s.rstrip() for s in obj.split('\n')]
  lines[0] = lines[0].lstrip()
  #
  return lines



def fix_simple_name(obj):
  obj = replace(obj, '("[A-Z0-9_$#]+")\.', '')
  obj = re.sub(r'"([A-Z0-9_$#]+)"', lambda x : x.group(1).lower(), obj)
  #
  return obj



