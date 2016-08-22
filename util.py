import json
import datetime
import dateutil.parser
import re

def make_connection(credentials):
  # This has to go here for some reason that I assume is related to serialization.
  import mysql.connector
  connection = mysql.connector.connect(**credentials)
  connection.time_zone = "+00:00"
  return connection

# This is imperfect but much better than most others, and fast since it's a regex.
# http://www.pelagodesign.com/blog/2009/05/20/iso-8601-date-validation-that-doesnt-suck/
time_regex_str = r"^([\+-]?\d{4}(?!\d{2}\b))((-?)((0[1-9]|1[0-2])(\3([12]\d|0[1-9]|3[01]))?|W([0-4]\d|5[0-2])(-?[1-7])?|(00[1-9]|0[1-9]\d|[12]\d{2}|3([0-5]\d|6[1-6])))([T\s]((([01]\d|2[0-3])((:?)[0-5]\d)?|24\:?00)([\.,]\d+(?!:))?)?(\17[0-5]\d([\.,]\d+)?)?([zZ]|([\+-])([01]\d|2[0-3]):?([0-5]\d)?)?)?)?$"
time_re = re.compile(time_regex_str)

def detimeify(x):
    if type(x) is str and time_re.match(x):
        return dateutil.parser.parse(x)
    else:
        return x


def timeify(x):
  if type(x) is datetime.datetime:
    # No time zone encoding, because I think it was wrong to include it in the first place.
    return x.strftime('%Y-%m-%dT%H:%M:%S')
  else:
    return x

def string_to_dict(x):
    return dict((k, detimeify(v)) for k, v in json.loads(x).items())

def query_result_to_json_string(select_fields, q, constraint_dict = dict()):
  return json.dumps(
    dict(
        **dict(
          zip(
            select_fields,
            map(timeify, q)
          )
        ),
        **constraint_dict
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
