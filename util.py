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
