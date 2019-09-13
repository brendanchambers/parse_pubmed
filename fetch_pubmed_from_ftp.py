# new workflow -
# download the raw files (alas)



from ftplib import FTP
import gzip
from io import BytesIO

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import Row, StructType, StructField, IntegerType, StringType

import time



ftp_target_dir =  'ftp.ncbi.nlm.nih.gov' # 'ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline'
ftp_subdir = '/pubmed/baseline/'

#local_data_directory = '/home/brch/Data/pubmed/xml/'
#local_data_directory = '/media/midway/pubmed/xml/'


################################################
print('initializing spark')
# init spark
conf = SparkConf()
conf = (conf.setMaster('local[*]')
        .set('spark.executor.memory','1G')  # 20
        .set('spark.driver.memory','8G')   # 40
        .set('spark.driver.maxResultSize','500M'))  #.set('spark.storage.memoryFraction',0))  # this setting is now a legacy option
        #.set('spark.python.worker.reuse', 'false')
        #.set('spark.python.worker.memory','512m')
        #.set('spark.executor.cores','1'))
sc = SparkContext(conf=conf)
spark = SparkSession(sc)  # don't need this for vanilla RDDs

print(sc._conf.getAll())

###############################################


def ftp_helper(namestr):
    # local save file for ftp'd xml
    target = target_brdcst.value + namestr.split('.')[0] + '.xml.gz'  # pubmed year chunk# .xml
    try:
        with FTP(ftp_dir_broadcast.value) as ftp:
            ftp.login()

            r = BytesIO()
            ftp_target = ftp_subdir_broadcast.value + namestr
            ftp.retrbinary('RETR {}'.format(ftp_target), r.write)

            with open(target, 'wb') as f:
                #f.write(gzip.decompress(r.getvalue()))
                f.write(r.getvalue())

            r.close()
            del ftp_target

    except Exception as e:
        return e

    return True

Create

# get the list of filenames

with FTP(ftp_target_dir) as ftp:
    ftp.login()
    ftp.cwd(ftp_subdir)
    my_generator = ftp.mlsd()
    filenames_list = [s[0] for s in my_generator if ('.gz' in s[0] and '.md5' not in s[0])]

print(filenames_list[:10])





# broadcast ftp destination info
ftp_dir_broadcast = sc.broadcast(ftp_target_dir)
ftp_subdir_broadcast = sc.broadcast(ftp_subdir)
target_brdcst = sc.broadcast(local_data_directory)


filenames_rdd = sc.parallelize(filenames_list, 20)  # temp: limit for testing
print("number of partitions in filenames_rdd: {}".format(filenames_rdd.getNumPartitions()))

print('processing ftp namestrings...')
start_time = time.time()
errcheck_rdd = filenames_rdd.map(ftp_helper)
#filenames_rdd.foreach(ftp_helper)
print("number of partitions in errcheck rdd: {}".format(errcheck_rdd.getNumPartitions()))

print(errcheck_rdd.collect())

end_time = time.time()
#errcheck_rdd.collect()
print("duration: {} ".format(end_time - start_time))

print('finished!')




