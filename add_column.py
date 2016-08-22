#!/usr/bin/env python3
# -*- coding: utf-8
import argparse
from pyspark import SparkContext, SparkConf
import dateutil.parser
import yaml
import os
import math
import json
import datetime
import sys
import pprint
from util import make_connection, query_result_to_json_string, list_indexes, string_to_dict

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
    '-f', '--filter',
    #nargs = '1',
    action = 'append',
    required = False,
    help = "Column filters to apply."
)
parser.add_argument(
    '-p', '--partitions',
    type = int, required = False, default = 100,
    help = "The number of partitions over which to query."
)
parser.add_argument(
    '-i', '--input_directory',
    type = str, required=True,
    help = "The directory containin jsonl of the first column"
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
    '-a', '--column-to-add',
    type = str, required=True,
    help = "Column to add to input to create output"
)
parser.add_argument(
    '-s', '--sort-column',
    type = str,
    help = "Column to sort on for checkpointing. Inferred iff there is just one column."
)
parser.add_argument(
    '-e', '--checkpoint-every',
    type = int, default = 100000,
    help = "Checkpoint every N steps on the sort column"
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
#
# if args.index is None:
#   raise Exception("Index must be specified, use --list-indexes to list.")

# Fail fast if the output directory already exists.
if os.path.exists(args.output_directory):
  raise Exception("You need to provide an output directory that does not already exist.")


conf = SparkConf().setAppName("add_column")
sc = SparkContext(conf=conf)
table_indexes = list_indexes(credentials, args.table_name)

input_rdd = sc.textFile(args.input_directory, args.partitions).map(string_to_dict)
input_indexes = input_rdd.take(1)[0]
if args.sort_column is not None:
    sort_column = args.sort_column
elif len([x for x in input_indexes]) == 1:
    sort_column = [x for x in input_indexes][0]
else:
    raise Exception("You have more than one column in your input dir and so need to specify a sort column for checkpointing.")


# The goal here is to infer which table index we are actually using.

# I should be able to do a lot better than this, but my brain isn't working
# that well today it seems. Here, we are finding all the valid indexes in the database
# that contain the indexes contained in the input __in any order__.
if args.force_index:
    # build up candidate_indexes, a list of table indexes that match what we've
    # already downloaded.
    candidate_indexes = list()
    for k, v in table_indexes.items():
        # gets the keys of input_indexes and makes a list of them.
        unclaimed_indexes = [x for x in input_indexes]
        counter = 0
        success = False
        while len(unclaimed_indexes) > 0:
            if v[counter] in unclaimed_indexes:
                unclaimed_indexes = [x for x in unclaimed_indexes if x != v[counter]]
                success = True
                counter += 1
            else:
                # This is not a valid index
                success = False
                break
        if success:
            candidate_indexes.append(k)

    # Now, build valid_index, the list of indexes whose Nth column matches the column
    # we want to add and whose [0:N] columns match what we already have.
    valid_indexes = list()
    for index_name in candidate_indexes:
        columns = table_indexes[index_name]
        unclaimed_indexes = [x for x in input_indexes]
        counter = 0
        while len(unclaimed_indexes) > 0:
            unclaimed_indexes = [x for x in unclaimed_indexes if x != columns[counter]]
            counter += 1
        if columns[counter] == args.column_to_add:
            valid_indexes.append(index_name)

    if len(valid_indexes) == 0:
        raise Exception("we have no indices that let you add the column you asked to add!")

    # lesson: next time use a serialization format that lets you specify column order. :)

def detect_predicate(x):
    # filter predicate MUST be a float or something like a datetime right now.
    try:
        o = float(x)
        return o
    except ValueError:
        return dateutil.parser.parse(x)

def string_to_filter_function(filter_string):
    # I'm working too hard here, and also need to DRY this code up.
    if len(filter_string.split(">=")) > 1:
        f = filter_string.split(">=")
        if len(f) > 2:
            raise Exception("Couldn't parse filter string in >= case.")
        subject = f[0].strip()
        predicate = detect_predicate(f[1])
        return lambda x: x[subject] >= predicate
    elif len(filter_string.split("<=")) > 1:
        f = filter_string.split("<=")
        if len(f) > 2:
            raise Exception("Couldn't parse filter string in <= case.")
        subject = f[0].strip()
        predicate = detect_predicate(f[1])
        return lambda x: x[subject] <= predicate
    elif len(filter_string.split("==")) > 1:
        f = filter_string.split("==")
        if len(f) > 2:
            raise Exception("Couldn't parse filter string in == case.")
        subject = f[0].strip()
        predicate = detect_predicate(f[1])
        return lambda x: x[subject] == predicate
    elif len(filter_string.split(">")) > 1:
        f = filter_string.split(">")
        if len(f) > 2:
            raise Exception("Couldn't parse filter string in > case.")
        subject = f[0].strip()
        predicate = detect_predicate(f[1])
        return lambda x: x[subject] > predicate
    elif len(filter_string.split("<")) > 1:
        f = filter_string.split("<")
        if len(f) > 2:
            raise Exception("Couldn't parse filter string in < case.")
        subject = f[0].strip()
        predicate = detect_predicate(f[1])
        return lambda x: x[subject] < predicate
    else:
        raise Exception("Couldn't parse filter string.")

filter_rdd = input_rdd
if args.filter is not None:
    for filter_string in args.filter:
        filter_rdd = filter_rdd.filter(string_to_filter_function(filter_string))


def sql_format_string(x):
    if type(x) is int:
        return "{}".format(x)
    elif type(x) is datetime.datetime:
        return "'{}'".format(x)
    else:
        raise Exception("Don't know how to format query given start coordinate type {0}".format(type(start)))
def query_additional_column(constraints, new_column, table_name):
    # build up a where statement

    constraint_string = " AND ".join(
        k+"="+sql_format_string(v) for k, v in constraints.items()
    )
    s = (
        # trailing space is critical!
        "SELECT DISTINCT({new_column}) from {table_name} ".format(new_column = new_column, table_name = table_name)+
        "WHERE "+constraint_string
    )
    connection = make_connection(credentials)
    c = connection.cursor()
    c.execute(s)
    columns = c.column_names
    for r in c:
        yield(query_result_to_json_string(columns, r, constraints))
    c.close()
    connection.close()


# we are doing the sort on the driver so this is checkpointable. If the index doesn't
# fit in memory, it will blow up. I don't believe that there is anything to do for that.

iteration_list = list(sorted(filter_rdd.map(lambda x: x[sort_column]).distinct().collect()))
stride = args.checkpoint_every
n_checkpoints = int(len(iteration_list)/stride)+1
zeros_to_pad = str(int(math.ceil(math.log10(n_checkpoints))))
for checkpoint_number in range(0, n_checkpoints):
    print("Checkpoint {0}/{1}: {2}".format(checkpoint_number, n_checkpoints, datetime.datetime.now()))
    start_from = checkpoint_number*stride
    iteration_subset = iteration_list[start_from:(start_from+stride)]
    iteration_subset_broadcast = sc.broadcast(iteration_subset)
    working_section = filter_rdd.filter(lambda x: x[sort_column] in iteration_subset_broadcast.value).coalesce(args.partitions)
    query_results = working_section.flatMap(lambda x: query_additional_column(x, args.column_to_add, args.table_name))
    query_results.saveAsTextFile(args.output_directory+("/{0:0"+zeros_to_pad+"d}").format(checkpoint_number))
    iteration_subset_broadcast.unpersist()

sys.exit()
