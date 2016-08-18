#!/usr/bin/env python3
# -*- coding: utf-8
import argparse
from pyspark import SparkContext, SparkConf
import yaml
import os
import math
import json
import datetime
import sys
import pprint
from util import make_connection, query_result_to_json_string, list_indexes

# Parse command-line arguments.
parser = argparse.ArgumentParser(description='Fill this in later.')
parser.add_argument(
    '-c', '--credential-file',
    type = str, required = True,
    help = "REQUIRED: Path to the yaml file containing your credentials."
)
parser.add_argument(
    '-o', '--output-directory',
    type = str, required = True,
    help = "REQUIRED: Path to the directory to write the output to. Must not exist!"
)
parser.add_argument(
    '-p', '--partitions',
    type = int, required = False, default = 1000000,
    help = "The number of partitions over which to query."
)
parser.add_argument(
    '-i', '--index',
    type = str,
    help = "The name of the index column we are enumerating"
)
parser.add_argument(
    '-t', '--table-name',
    type = str, required = True,
    help = "The name of the table in question"
)
parser.add_argument(
    '-l', '--list-indexes',
    action = 'store_true',
    help = "List index columns and exit"
)
parser.add_argument(
    '--no-force-index',
    dest = 'force_index',
    action = 'store_false'
)

args = parser.parse_args()

with open(args.credential_file, 'rb') as f:
  credentials = yaml.load(f.read().decode('utf-8'))

if args.list_indexes:
  pp = pprint.PrettyPrinter(indent=4)
  pp.pprint(list_indexes(credentials, args.table_name))
  sys.exit()

if args.index is None:
  raise Exception("Index must be specified, use --list-indexes to list.")

first_columns = { x[0] for x in list_indexes(credentials, args.table_name).values() }
if args.force_index and args.index not in first_columns:
  raise Exception("Index is not valid.")

# Fail fast if the output directory already exists.
if os.path.exists(args.output_directory):
  raise Exception("You need to provide an output directory that does not already exist.")

index = args.index
table_name = args.table_name
# Here, and here only, we are inferring the range of the index. We will not do
# this when adding columns to the index (in get_additional_column)

connection = make_connection(credentials)
c = connection.cursor()
c.execute("select min({0}) from {1}".format(index, table_name))
[min_index] = c.fetchall()[0]
c.execute("select max({0}) from {1}".format(index, table_name))
[max_index] = c.fetchall()[0]
c.close()
connection.close()
step = math.ceil((max_index-min_index)/args.partitions)
intervals = range(min_index, max_index+1, step)

def query_first_column(start, step, index, table_name):
  if type(start) is int:
    index_format_string = "{index}"
  elif type(start) is datetime.datetime:
    index_format_string = "'{index}'"
  else:
    raise Exception("Don't know how to format query given start coordinate type {0}".format(type(start)))
  s = (
      "SELECT DISTINCT({index}) FROM {table_name} WHERE "+
      index_format_string+">={start} AND "+
      index_format_string+"<{end}").format(
      **{
        "index": index,
        "table_name":table_name,
        "start": start,
        "end": start+step
      }
  )

  connection = make_connection(credentials)
  c = connection.cursor()
  # This usage is specific to the first column. For additional columns
  # we will need a different syntax.
  c.execute(s)
  for r in c:
    yield(query_result_to_json_string((index,), r))

  c.close()
  connection.close()

conf = SparkConf().setAppName("get_first_column")
sc = SparkContext(conf=conf)

start_indices = sc.parallelize(intervals, args.partitions)
first_column = start_indices.flatMap(lambda x: query_first_column(x, step, index, table_name))
first_column.saveAsTextFile(args.output_directory)
