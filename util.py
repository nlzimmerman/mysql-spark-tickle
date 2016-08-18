import json

def make_connection(credentials):
  # This has to go here for some reason that I assume is related to serialization.
  import mysql.connector
  connection = mysql.connector.connect(**credentials)
  connection.time_zone = "+00:00"
  return connection

def timeify(x):
  if type(x) is datetime.datetime:
    # No time zone encoding, because I think it was wrong to include it in the first place.
    return x.strftime('%Y-%m-%dT%H:%M:%S')
  else:
    return x

def query_result_to_json_string(select_fields, q):
  return json.dumps(
    dict(
      zip(
        select_fields,
        map(timeify, q)
      )
    ),
    sort_keys=True,
    separators=(',', ':')
  )

def list_indexes(credentials, table_name):
  connection = make_connection(credentials)
  c = connection.cursor()
  c.execute("show indexes in {0}".format(table_name))
  r = c.fetchall()
  cols = c.column_names
  c.close()
  connection.close()
  r_dict = [dict(zip(cols, x)) for x in r]
  indices = tuple({x['Key_name'] for x in r_dict})
  index_seq = {
    key_name: list(sorted(y['Seq_in_index'] for y in r_dict if y['Key_name'] == key_name))
    for key_name in indices
  }
  index_columns = {
    key_name: [y['Column_name'] for x in index_seq[key_name] for y in r_dict if y['Key_name']==key_name and y['Seq_in_index']==x]
    for key_name in indices
  }
  return index_columns
