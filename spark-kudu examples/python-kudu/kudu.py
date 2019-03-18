# Part 1: CREATE A TABLE
# Step 1
import kudu
from kudu.client import Partitioning
from datetime import datetime

# Step 2
# If your port is different replace `7051` with your port
client = kudu.connect(host='kudu-master-dns', port=7051)

# Step 3
builder = kudu.schema_builder()

# Step 4
builder.add_column('column1').type(kudu.int64).nullable(False).primary_key()
builder.add_column('column2', type_=kudu.unixtime_micros, nullable=False, compression='lz4')

# Step 5
schema = builder.build()

# Step 6
partitioning = Partitioning().add_hash_partitions(column_names=['column1'], num_buckets=3)

# Step 7
client.create_table('insert-table-name', schema, partitioning)

# Step 8
table = client.table('insert-table-name')

# Part 1.5: NEW SESSION
session = client.new_session()

# Part 2: INSERT ROW
# Step 1
op = table.new_insert({'column1': 1, 'column2': datetime.utcnow()})

# Step 2
session.apply(op)

# Part 3: UPSERT ROW
op = table.new_upsert({'column1': 2, 'column2': "2016-01-01T00:00:00.000000"})
session.apply(op)

# Part 4: UPDATE ROW
op = table.new_update({'column1': 1, 'column2': ("2017-01-01", "%Y-%m-%d")})
session.apply(op)

# Part 5: DELETE A ROW
op = table.new_delete({'column1': 2})
session.apply(op)