# Initializes schemas for our specific use-case
def createSchema(cur, conn, schemas):
    if type(schemas) is not list: schemas = [schemas]

    for schema in schemas: cur.execute(schema)
    conn.commit()

# Fetches data from .csv and converts to list of tuples
def fromCSV(file_path):
    data = []

    with open(file_path, 'r') as file:
        next(file)
        lines = file.readlines()

        for line in lines:
            line = line.replace('\n', '')
            line_parsed = line.split(',')
            data.append(tuple(line_parsed))

    return data

# Need to double check if can insert single entry as list
# Takes tuples and inserts them into a table
def insertData(cur, conn, data, table):
    if type(data) is not list: data = [data]

    attributes = fetchAttributes(cur, table)

    # Need a string of '?'s for each attribute
    q_string = '('
    for _ in attributes: q_string = q_string + '?, '
    q_string = q_string[:-2] + ')'

    # Ignores repetitive data
    cur.executemany(
        f'INSERT OR IGNORE INTO {table} ' \
        f'{attributes} ' \
        f'VALUES {q_string};', data
    )
    conn.commit()

def fetchAttributes(cur, table):
    cur.execute(f'PRAGMA table_info({table});')
    raw_info = cur.fetchall()

    attributes = ()
    for tuple in raw_info: attributes = attributes + (tuple[1],)

    return attributes