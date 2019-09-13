

In this directory, I develop a pipeline for fetching the pubmed database from NIH servers.



Most of these files are early attempts and unit tests.




#FINAL PRODUCTS:

#(1) fetch_pubmed_from_ftp.py.
         download xml.gz compressed file chunks\
         using pyspark for local parallelization
         
#(2) parse_pubmed_xml.py.
        decompress .gz files and parse with lxml parser\
        using pyspark for local parallization\
        store in a mysql db (via pyspark dataframe & jdbc)\
            schema: (pmid, title, abstract)\
                  (other metadata is avaialable)\
        the planned use case here is to integrate with citation dataset using joins on pmid

#(3) parse_pubmed_xml_midwayVersion.py
	same as above but configured for running in local mode on a midway2 compute node
        

