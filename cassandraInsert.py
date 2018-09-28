#############
## PARQUET ##
#############

import pandas as pd
from fastparquet import write, ParquetFile
from pyspark.sql import SQLContext

## Dummy variables
fPath = '/path/to/file1.parq'
df1 = pd.DataFrame([[1,'B','C'],[2,'D','E']], columns=list('abc'))

# Fastparquet
write(fPath, df1, append=False) # Write; Read
df2 = ParquetFile(fPath).to_pandas(['a','b'], filters=[('a','in',[1])])
df3 = df2[df2.a==1] # Subset

# SqlContext
sqlCtx.createDataFrame(df1).write.parquet(fPath) # Write directory
df4 = spark.read.parquet(fPath) # Read file | directory

##########
## AVRO ##
##########

import pandas as pd
from fastavro import writer, reader

## Schema declaration
schema = {
    'doc'       : 'Counting Office Inventory.'  ,
    'name'      : 'Test'                        ,
    'namespace' : 'inventory'                   ,
    'type'      : 'record'                      ,
    'fields'    : [
        {'name' : 'object'  , 'type': 'string' },
        {'name' : 'count'   , 'type': 'int'    },
        {'name' : 'unixtime', 'type': 'long'   },
    ],
}

## Record composition
records = [
    {u'object': u'table', u'count': 1, u'unixtime': 1520434430},
    {u'object': u'chair', u'count': 2, u'unixtime': 1520434431},
]

## Writing
with open('test.avro', 'wb') as outFile:
writer(outFile, schema, records)

## Reading
with open('test.avro', 'rb') as inFile:
for record in reader(inFile):
df = pd.DataFrame.from_records(records)


#############
## PYSPARK ##
#############

# BASH: Invoke spark shell with spark-avro package
cd /opt/Spark/spark-2.2.1-bin-hadoop2.7/
bin/pyspark --packages com.databricks:spark-avro_2.11:4.0.0

# Python
df=spark.read.format("com.databricks.spark.avro").load("/path/test.avro")
subset = df.where("count > 1") # Simple subsetting
subset.write.format("com.databricks.spark.avro").save("/path/subset.avro")


######################
## CASSANDRA DRIVER ##
######################

# 0) Simple Example

from cassandra.cluster import Cluster
session = Cluster().connect()

statement = '''
    INSERT INTO keyspace.table ( col_a, col_b, col_c )
    VALUES ( %(col_a)s, %(col_b)s, %(col_c)s )
; '''

insertDict = {'col_a':1, 'col_b':'B', 'col_c':'C'}
session.execute(statement, insertDict)


# 1) SINGLE THREAD/PROCESS SYNCHRONOUS INSERT #
# ---------------------------------------------
from cassandra.cluster import Cluster

statement = 'INSERT INTO test.test2 (a, b) VALUES (%s, %s);'

for i in xrange(100000):
    insert = statement %(i,i)
    session.execute(insert)


# 2) SINGLE THREAD/PROCESS CONCURRENT INSERT
# ------------------------------------------
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

session = Cluster().connect()

statement = session.prepare(
    'INSERT INTO test.test2 (a, b) VALUES (?,?)'
)

values = [(x,x) for x in xrange(0, 10000)]

execute_concurrent_with_args(
    session, statement, values, concurrency=100
)


# 3) MULTI-PROCESS CONCURRENT INSERT
# ----------------------------------

from multiprocessing import Process, Queue
import time
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

# Define multiprocessing function
def ccrt(Tuple, Qout):
    session = Cluster().connect()

    statement = session.prepare(
        'INSERT INTO test.test2 (a, b) VALUES (?,?)'
    )

    values = [(x,x) for x in xrange(Tuple[0], Tuple[1])]

    execute_concurrent_with_args(
        session, statement, values, concurrency=100
    )

    Qout.put(time.time())

# Prepare data & parallelization process
N = 4;
Qout = Queue()

tupL = [
    (0,25000), (25000,50000), (50000,75000), (75000,100000)
]

procL = [
    Process(
        name='Number %s'%i,
        target=ccrt,
        args=(tupL[i], Qout)
    ) for i in xrange(N)
]

# Timer
a = time.time()
[item.start() for item in procL] # Start processes
print procL
time.sleep(20)
print procL # Wait for execution

# Output
outL = [Qout.get() for i in xrange(N)] # Collect results
[item.join() for item in procL] # Kill processes
print sum([item - a for item in outL])/len(outL) # Calculate mean runtime

