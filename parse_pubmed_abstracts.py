from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import Row, StructType, StructField, IntegerType, StringType

from lxml import etree
import pymysql
import json

import random
import time
import os
import gc

####################################################
parserconfig_path = 'parser_config.json'
####################################################

with open(parserconfig_path, 'r') as f:
    config = json.load(f)
print(config)  # temp

local_data_directory = config['local_data_directory']
path2jar = config['path2jar']  # jdbc jar file
N_EXEC = config['N_executors']  # parallelization width
client_config = config['client_config']  # mysql connection 
db_name = config['db_name']
table_name = config['table_name']
spark_driver_memory = config['spark_driver_memory_string']

SUBMIT_ARGS = "--driver-class-path file://{} --jars file://{} pyspark-shell".format(path2jar, path2jar)   # build submit_args string
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS

url = "jdbc:mysql://localhost:3306/{}?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=America/Chicago".format(db_name)  # mysql runs on port 3306

################################################

db = pymysql.connect(**client_config)

# init mysql db
try:
    # copied from old sqlite code
    # init db
    cursor = db.cursor()
    sql = 'CREATE DATABASE {}'.format(db_name)
    print(sql)
    cursor.execute(sql)
except Exception as e:
    print('Warning while creating new database:')
    print(e)

    
###################################################
# init db

try:
    print('creating table...')   

    db = pymysql.connect(**client_config, database=db_name)

    cursor = db.cursor()
    sql = "CREATE TABLE {} (pmid INT,\
                            title TEXT,\
                            abstract TEXT,\
                            PRIMARY KEY (pmid))".format(table_name)
    print(sql)
    cursor.execute(sql)
except Exception as e:
    print('Warning while creating new table:')
    print(e)

db.commit()
print(db)

################################################
# init spark

print('initializing spark...')
conf = SparkConf()
conf = (conf.setMaster('local[*]')
       .set('spark.driver.memory',spark_driver_memory)
       .set("spark.jars", path2jar))        

sc = SparkContext(conf=conf)
spark = SparkSession(sc)

print(sc._conf.getAll())

###################################################

def parse_helper(namestr):

    path = dir_brdcst.value + namestr
    tree = etree.parse(path)  # autodetect zip and read the .gz compressed file
    base_xpath = '/PubmedArticleSet/PubmedArticle/MedlineCitation'

    for el in tree.xpath(base_xpath, smart_strings=False):

        # for each article:
        title = ''  # init title and abstract in case of missing data
        abstract = ''  # but assume pmid will be present
       
        for child in el.iterchildren():

            if child.tag == 'PMID':
                pmid = int(child.text)

            if child.tag == 'Article':
                for article_child in child.iterchildren():
                    if article_child.tag == 'ArticleTitle':
                        if article_child.text is not None:
                            title += article_child.text
                        for title_child in article_child:  # handle interposed tags (this is particular to lxml)
                            if title_child.tail is not None:
                                title += title_child.tail

                    if article_child.tag == 'Abstract':
                        for abstract_child in article_child.iterchildren():
                            if abstract_child.tag == 'AbstractText':
                                if abstract_child.text is not None:
                                    abstract += abstract_child.text
                                for abstract_inner_child in abstract_child:  # handle interposed tags (this is particular to lxml)
                                    if abstract_inner_child.tail is not None:
                                        abstract += abstract_inner_child.tail
                                abstract += ' '

        yield Row(pmid, title, abstract)

    gc.collect()  # note - probably don't need to do this explicitly
                  # this is leftover from troubleshooting a mem leak

# get the list of filenames
print('processing xml files...')
filenames_list = os.listdir(local_data_directory)
print(filenames_list[:10])
dir_brdcst = sc.broadcast(local_data_directory)
filenames_rdd = sc.parallelize(filenames_list, N_EXEC)
print("number of partitions in filenames_rdd: {}".format(filenames_rdd.getNumPartitions()))

start_time = time.time()
print("building abstracts rdd...")
abstracts_rdd = filenames_rdd.flatMap(parse_helper)
print('number of partitions in abstracts rdd: {}'.format(abstracts_rdd.getNumPartitions()))

print('converting rdd to dataframe...')
schema = StructType([StructField('pmid', IntegerType(), False),  # Nullable?
                     StructField('title', StringType(), True),
                     StructField('abstract', StringType(), True)])
abstracts_df = spark.createDataFrame(abstracts_rdd, schema)  # potentially cache here
print('number of partitions in abstracts df: {}'.format(abstracts_df.rdd.getNumPartitions()))
end_time = time.time()
print('duration: {} s'.format(end_time - start_time))

print('writing dataframe to disk...')
start_time = time.time()
abstracts_df.write.format('jdbc').options(
                          numPartitions=4,  # reduce num partitions to limit parallel db writes
                          rewriteBatchedStatements=False,
                          url=url,
                          dbtable=table_name).mode('overwrite').save()  # or, append

end_time = time.time()
print('duration: {} s'.format(end_time - start_time))

print('finished parsing pubmed xml data to mySQL database!')