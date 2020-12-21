import argparse
import pyspark

from pyspark.sql.functions import unix_timestamp, from_unixtime, current_timestamp, lit, date_format
from generic_utils import execute_compute_stats


def execute_process(options):

    spark = pyspark.sql.session.SparkSession \
            .builder \
            .appName("criar_tabela_documentos_arquivados") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .enableHiveSupport() \
            .getOrCreate()

    schema_exadata = options['schema_exadata']
    schema_exadata_aux = options['schema_exadata_aux']
    table_name = options['table_name']

    table = spark.sql("""
        SELECT DISTINCT D.docu_dk, D.docu_orgi_orga_dk_responsavel
        FROM documento D
        LEFT JOIN (
            SELECT item_docu_dk
            FROM {0}.mcpr_item_movimentacao
            JOIN {0}.mcpr_movimentacao ON item_movi_dk = movi_dk
            WHERE movi_orga_dk_destino IN (200819, 100500)
        ) T ON item_docu_dk = docu_dk
        LEFT JOIN (
            SELECT vist_docu_dk,
                CASE
                WHEN cod_pct IN (20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 180, 181, 182, 183)
                    AND stao_tppr_dk IN (
                        7912, 6548, 6326, 6681, 6678, 6645, 6682, 6680, 6679,
                        6644, 6668, 6666, 6665, 6669, 6667, 6664, 6655, 6662,
                        6659, 6658, 6663, 6661, 6660, 6657, 6670, 6676, 6674,
                        6673, 6677, 6675, 6672, 6018, 6341, 6338, 6019, 6017,
                        6591, 6339, 6553, 7871, 6343, 6340, 6342, 6021, 6334,
                        6331, 6022, 6020, 6593, 6332, 7872, 6336, 6333, 6335,
                        7745, 6346, 6345, 6015, 6016, 6325, 6327, 6328, 6329,
                        6330, 6337, 6344, 6656, 6671, 7869, 7870, 6324, 7834,
                        7737, 6350, 6251, 6655, 6326
                    )
                    THEN 1
                WHEN cod_pct >= 200
                    AND stao_tppr_dk IN (
                        6682, 6669, 6018, 6341, 6338, 6019, 6017, 6591, 6339,
                        7871, 6343, 6340, 6342, 7745, 6346, 7915, 6272, 6253,
                        6392, 6377, 6378, 6359, 6362, 6361, 6436, 6524, 7737,
                        7811, 6625, 6718, 7834, 6350
                    )
                    THEN 1
                ELSE null
                END AS is_arquivamento
            FROM documento
            LEFT JOIN {1}.atualizacao_pj_pacote ON id_orgao = docu_orgi_orga_dk_responsavel
            JOIN vista ON vist_docu_dk = docu_dk
            JOIN {0}.mcpr_andamento ON vist_dk = pcao_vist_dk
            JOIN {0}.mcpr_sub_andamento ON stao_pcao_dk = pcao_dk
            JOIN {0}.mcpr_tp_andamento ON tppr_dk = stao_tppr_dk
        ) A ON vist_docu_dk = docu_dk AND is_arquivamento IS NOT NULL
       WHERE NOT (item_docu_dk IS NULL AND vist_docu_dk IS NULL)
        AND docu_fsdc_dk = 1
        AND docu_tpst_dk != 11
        AND docu_orgi_orga_dk_responsavel IS NOT NULL
    """.format(schema_exadata, schema_exadata_aux))

    table_name = "{}.{}".format(schema_exadata_aux, table_name)
    table.write.mode("overwrite").saveAsTable(table_name)

    execute_compute_stats(table_name)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Create table acervo")
    parser.add_argument('-e','--schemaExadata', metavar='schemaExadata', type=str, help='')
    parser.add_argument('-a','--schemaExadataAux', metavar='schemaExadataAux', type=str, help='')
    parser.add_argument('-i','--impalaHost', metavar='impalaHost', type=str, help='')
    parser.add_argument('-o','--impalaPort', metavar='impalaPort', type=str, help='')
    parser.add_argument('-t','--tableName', metavar='tableName', type=str, help='')
    args = parser.parse_args()

    options = {
                    'schema_exadata': args.schemaExadata,
                    'schema_exadata_aux': args.schemaExadataAux,
                    'impala_host' : args.impalaHost,
                    'impala_port' : args.impalaPort,
                    'table_name' : args.tableName,
                }

    execute_process(options)