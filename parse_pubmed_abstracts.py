# new workflow -
# download the raw files (alas)
# next troubleshooting idea: put the socket in /project2 directory

import os
import gc
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import Row, StructType, StructField, IntegerType, StringType

import time
from lxml import etree
import mysql.connector as mysql

import random

# don't forget to modify the env variables (java_home will be set by module load java)
#    more info: https://spark.apache.org/docs/latest/configuration.html
#      (/software/java-1.8-x86_64)
#     of course these ones: PYTHON=`which python`
#                        export PYSPARK_PYTHON=$PYTHON
#                        export PYSPARK_DRIVER_PYTHON=$PYTHON
#                        module load spark/2.3.2


#local_data_directory = '/media/midway/pubmed/gz/'  # vizasaur2 bc local path
local_data_directory = '/project2/jevans/brendan/pubmed_xml_data/gz/'  # midway path

# mysql
#SUBMIT_ARGS = "--packages mysql:mysql-connector-java:8.0.16 pyspark-shell"  # this works on my local machine
SUBMIT_ARGS = "--driver-class-path file:///home/brendanchambers/my_resources/mysql-connector-java-8.0.16/mysql-connector-java-8.0.16.jar --jars file:///home/brendanchambers/my_resources/mysql-connector-java-8.0.16/mysql-connector-java-8.0.16.jar pyspark-shell"
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS

db_name = 'test_pubmed' 
table_name = 'abstracts_v2' # 'abstracts'
# db name collisons? https://stackoverflow.com/questions/14011968/user-cant-access-a-database

url = "jdbc:mysql://localhost:3306/{}?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=America/Chicago".format(db_name)  # mysql runs on port 3306

#############################f##################

client_config = {'unix_socket':'/home/brendanchambers/.sql.sock'} 
'''
client_config = {'user':'brendanchambers',
                 'password':'', 
                 'host':'localhost',
                 'unix_socket':'/project2/jevans/study_dbs/mysql/.sql.sock'}  # temp version
'''
                # 'password':'',
#                 'host':'localhost',

# troubleshooting- https://dev.mysql.com/doc/refman/5.5/en/information-functions.html#function_current-user
#  more - https://forums.mysql.com/read.php?10,512805,512860#msg-512860
#   docs - https://dev.mysql.com/doc/refman/5.5/en/connection-access.html

db = mysql.connect(**client_config)  # todo obfuscate'

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

    
# create table'

try:
    print('creating table...')   

    db = mysql.connect(**client_config, database=db_name)

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
# testing - doing this after db creation

print('initializing spark')
# init spark
conf = SparkConf()
conf = (conf.setMaster('local[*]')
       .set('spark.driver.memory','24G')
       .set("spark.jars", "/home/brendanchambers/my_resources/mysql-connector-java-8.0.16/mysql-connector-java-8.0.16.jar"))        
'''
.set('spark.executor.memory','1G')  # 20
.set('spark.driver.memory','1G')   # 40
.set('spark.driver.maxResultSize','500M')  #.set('spark.storage.memoryFraction',0))  # this setting is now a legacy option
.set('spark.python.worker.reuse', 'false')
.set('spark.python.worker.memory','512m')
.set('spark.executor.cores','1'))
'''
sc = SparkContext(conf=conf)
#sc.addJar('home/brendanchambers/my_resources/mysql-connector-java-8.0.16/mysql-connector-java-8.0.16.jar')  # temp
spark = SparkSession(sc)  # don't need this for vanilla RDDs

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
        
        # todo year, journal, authors (update: these are now available in the 'metadata' table)

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

    #yield Row(random.randint(0,10000000), namestr, namestr)  # dummy return value for testing

    gc.collect()

# get the list of filenames

filenames_list = os.listdir(local_data_directory)
#filenames_list = filenames_list[:25] # temp
print(filenames_list[:10])
# temp:
dir_brdcst = sc.broadcast(local_data_directory)


# todo get list of xml filenames using os listdir
filenames_rdd = sc.parallelize(filenames_list, 4)  # temp: limit for testing
print("number of partitions in filenames_rdd: {}".format(filenames_rdd.getNumPartitions()))

print('processing xml files...')

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
                          numPartitions=4,
                          rewriteBatchedStatements=False,
                          url=url,
                          dbtable=table_name).mode('overwrite').save()  # or, append
'''
driver = 'com.mysql.cj.jdbc.Driver'
abstracts_df.write.format('jdbc').options(
                          numPartitions=4,
                          rewriteBatchedStatements=False,
                          url=url,
                          driver=driver,
                          dbtable=table_name).mode('overwrite').save()  # or, append
'''
# todo load the pwd in from a config file & upload to github
end_time = time.time()
print('duration: {} s'.format(end_time - start_time))

print('finished!')




