

In this directory, I develop a pipeline for fetching the pubmed database from NIH servers.



Early attempts and unit tests are available in a working directory on midway




# KEY FILES:

## (1) fetch_pubmed_from_ftp.py.
         download xml.gz compressed file chunks\
         using pyspark for local parallelization
         
## (2) parse_pubmed_xml.py.
        decompress .gz files and parse with lxml parser\
        using pyspark for local parallization\
        store in a mysql db (via pyspark dataframe & jdbc)\
            schema: (pmid, title, abstract)\
                  (other metadata is avaialable)\
        the planned use case here is to integrate with citation dataset using joins on pmid



###   Other notes:

 resulting db contains INT pmid, TEXT title, TEXT abstract
 year, journal, authors are already available via NIH OpenCite
    and available via 'metadata' table in the db


 don't forget to modify the env variables (java_home will be set by module load java)
    more info: https://spark.apache.org/docs/latest/configuration.html
     (/software/java-1.8-x86_64)
     of course these ones: PYTHON=`which python`
                        export PYSPARK_PYTHON=$PYTHON
                        export PYSPARK_DRIVER_PYTHON=$PYTHON
                        module load spark/2.3.2
                        
                        
#### strangeness in naming requirements on midway for mysql:
      # https://stackoverflow.com/questions/14011968/user-cant-access-a-database
      
#### typical xml gz data directories for knowledge lab users
local_data_directory = '/media/midway/pubmed/gz/'  # vizasaur2 path
local_data_directory = '/project2/jevans/brendan/pubmed_xml_data/gz/'  # midway path
local_data_directory = '/home/brendan/FastData/pubmed/gz/'  # knowledge-garden path
      
### connection config for mysql

e.g., to run on midway--

client_config = {'user': 'jim',
                 'password': 'nice_try', 
                 'host':'localhost',
                 'unix_socket':'/project2/jevans/study_dbs/mysql/.sql.sock'}

