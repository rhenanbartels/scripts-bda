from hdfs import InsecureClient
from os import path
import happybase
from base import spark


server = spark.conf.get("spark.server")
user_name_hdfs = spark.conf.get("spark.hdfs.user")
dir_files_pdf = spark.conf.get("spark.hdfs.dir")
url_oracle_server = spark.conf.get("spark.url.oracle.server")
table_oracle = spark.conf.get("spark.table.oracle")
user_oracle = spark.conf.get("spark.jdbc.user")
passwd_oracle = spark.conf.get("spark.jdbc.password")

server_hdfs = 'http://{}:50070'.format(server)

""" 
    Connection to hbase
"""
def get_table(table_name):
    try:
        connection = happybase.Connection(server, timeout=300000)
        return connection.table(table_name)
    except:
        connection = happybase.Connection(server, timeout=300000)
        return connection.table(table_name)


"""
    Method to save byte files extrated to HDFS
"""
def save_file_hdfs(rdd):
    n_file_id = int(rdd[0])
    n_info_tec = rdd[1].replace("/", "-")
    n_file = rdd[2]
    hdfsclient = InsecureClient(server_hdfs, user=user_name_hdfs) 
    hdfsclient.write(path.join(dir_files_pdf, '{}_{}.pdf'.format(n_file_id, n_info_tec)), n_file, overwrite=True)
    return rdd


row = get_table('file_info_tecnica').row('row1')
last_id = int(row["last_id:"])


#Connect to oracle to get all table GATE.GATE_INFO_TECNICA to extract files
table = spark.read.format("jdbc").option("url", url_oracle_server) \
   .option("dbtable", table_oracle) \
   .option("user", user_oracle) \
   .option("password", passwd_oracle) \
   .option("driver", "oracle.jdbc.driver.OracleDriver") \
   .load()

#Get id and byte file from rows greater than last_id variable
select_file = table.where("ITCN_DK > {} and ITCN_ARQUIVO is not null".format(last_id)).select("ITCN_DK", "ITCN_NR_INFOTECNICA", "ITCN_ARQUIVO")

#Transform in RDD and call the method save to HDFS
select_file.rdd.map(save_file_hdfs).count()

#Get max id value from table oracle
max_value = table.groupBy().max("ITCN_DK").first()[0]

#Update value last_id in hbase table
get_table('file_info_tecnica').put(b'row1',{b'last_id:': str(int(max_value))})
