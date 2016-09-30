import kudu
import os
import subprocess
import sys
import tempfile
import time
from kudu.client import Partitioning

DSTAT_COL_NAMES = ["usr", "sys", "idl", "wai", "hiq", "siq", "read", "writ", "recv", "send",
                  "in","out","int","csw"]


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
    builder = kudu.schema_builder()
    builder.add_column("ts", kudu.int64, nullable=False, primary_key=True)
    for col in DSTAT_COL_NAMES:
      builder.add_column(col, kudu.float_)
    schema = builder.build()

    # Create hash partitioning buckets
    partitioning = Partitioning().add_hash_partitions('ts', 2)

    client.create_table(table, schema, partitioning)

  return client.table(table)

def append_row(table, line):
  """The line is the raw string read from stdin, that is then splitted by , and prepended
  with the current timestamp."""
  data = [float(x.strip()) for x in line.split(",")]

  op = table.new_insert()
  # Convert to microseconds
  op["ts"] = int(time.time() * 1000000)
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

  client = kudu.connect("127.0.0.1", 7051)
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
