import kudu
import os
import subprocess
import sys
import tempfile
import time

DSTAT_COL_NAMES = ["usr", "sys", "idl", "wai", "hiq", "siq", "read", "writ", "recv", "send",
                  "in","out","int","csw"]


def connect_to(host, port=7051):
  """Returns a kudu client object connecting to the specified kudu master"""
  return kudu.Client("{0}:{1}".format(host, port))


def open_or_create_table(client, table, drop=False):
  """Based on the default dstat column names create a new table indexed by a timstamp col"""
  exists = False
  if client.table_exists(table):
    exists = True
    if drop:
      client.delete_table(table)
      exists = False

  if not exists:
    # Create the schema for the table, basically all float cols
    cols = [kudu.ColumnSchema.create("ts", kudu.INT64)]
    cols += [kudu.ColumnSchema.create(x, kudu.FLOAT) for x in DSTAT_COL_NAMES]

    # Based on the column meta data create a new schema object, where the first column
    # is the key column.
    schema = kudu.schema_from_list(cols, 1)
    client.create_table(table, schema)

  return client.open_table(table)

def append_row(table, line):
  """The line is the raw string read from stdin, that is then splitted by , and prepended
  with the current timestamp."""
  data = [float(x.strip()) for x in line.split(",")]

  op = table.insert()
  op["ts"] = int(time.time())
  for c, v in zip(DSTAT_COL_NAMES, data):
    op[c] = v
  return op

def start_dstat():
  tmpdir = tempfile.mkdtemp()
  path = os.path.join(tmpdir, "dstat.pipe")
  os.mkfifo(path)
  proc = subprocess.Popen(["dstat", "-cdngy", "--output", "{0}".format(path)])
  return proc.pid, path

if __name__ == "__main__":

  drop = False

  if len(sys.argv) > 1:
    operation = sys.argv[1]
    if operation in ["drop"]:
      drop = True

  client = connect_to("127.0.0.1")
  table = open_or_create_table(client, "dstat", drop)

  # Start dstat
  dstat_id, pipe_path = start_dstat()

  try:
    # Create file handle to read from pipe
    fid = open(pipe_path, "r")

    # Create session object
    session = client.new_session()
    counter = 0

    # The dstat output first prints uninteresting lines, skip until we find the header
    skip = True
    while True:
      line  = fid.readline()
      if line.startswith("\"usr\""):
        skip = False
        continue
      if not skip:
        session.apply(append_row(table, line))
        counter += 1
        if counter % 10 == 0:
          session.flush()
  except KeyboardInterrupt:
    if os.path.exists(pipe_path):
      os.remove(pipe_path)
