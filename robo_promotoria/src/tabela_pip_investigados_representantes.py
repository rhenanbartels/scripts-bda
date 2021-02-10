#-*-coding:utf-8-*-
import difflib
import re
import unicodedata
import pyspark
import argparse

from generic_utils import execute_compute_stats


# Sem espaços pois a função clean_name remove os espaços dos nomes
REGEX_EXCLUSAO_ORGAOS = (
    "(^MP$|MINIST[EÉ]RIOP[UÚ]BLICO|DEFENSORIAP[UÚ]BLICA"
    "|MINSTERIOPUBLICO|MPRJ|MINIT[EÉ]RIOP[UÚ]BLICO|JUSTI[ÇC]AP[UÚ]BLICA"
    "|PROMOTORIA(DETUTELA|DEJUSTICA|DEINVESTIGACAO|MILITAR|CRIMINAL|DACIDADANIA|DAINFANCIA|PUBLICA|CIVEL|DEMEIOAMBIENTE|DEPROTECAOAOIDOSO)"
    "|(PRIMEIRA|SEGUNDA)PROMOTORIA"
    "|PROCURADORI?A?DEJUSTICA)"
)

def clean_name(value):
    if not isinstance(value, str) and not isinstance(value, unicode):
        return None
    text = unicodedata.normalize('NFD', unicode(value) or "")
    text = text.encode('ascii', 'ignore')
    text = text.decode("utf-8")
    text = text.upper()
    text = re.sub(r"[^A-Z]", "", text)
    if not text:
        return None
    return text

def name_similarity(name_left, name_right):
    if not name_left or not name_right:
        return 0

    return difflib.SequenceMatcher(None, name_left, name_right).ratio()

def remove_mp(value):
    if not isinstance(value, str) and not isinstance(value, unicode):
        return 0
    if re.search(REGEX_EXCLUSAO_ORGAOS, value):
        return 1
    return name_similarity(value[:17], "MINISTERIOPUBLICO")


def execute_process(options):

    spark = pyspark.sql.session.SparkSession \
            .builder \
            .appName("criar_tabela_pip_investigados_representantes") \
            .enableHiveSupport() \
            .getOrCreate()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    spark.udf.register("name_similarity", name_similarity)
    spark.udf.register("clean_name", clean_name)
    spark.udf.register("remove_mp", remove_mp)

    schema_exadata = options['schema_exadata']
    schema_exadata_aux = options['schema_exadata_aux']
    LIMIAR_SIMILARIDADE = options["limiar_similaridade"]

    PERS_DOCS_PIPS = spark.sql("""
        SELECT DISTINCT cast(pers_pess_dk as int) as pers_pess_dk
        FROM {0}.mcpr_personagem
        --WHERE pers_tppe_dk IN (290, 7, 21, 317, 20, 14, 32, 345, 40, 5, 24)
    """.format(schema_exadata))
    PERS_DOCS_PIPS.createOrReplaceTempView('PERS_DOCS_PIPS')
    #spark.catalog.cacheTable('PERS_DOCS_PIPS')

    investigados_fisicos_pip_total = spark.sql("""
        SELECT *
        FROM (
            SELECT 
                cast(pesf_pess_dk as int) as pesf_pess_dk,
                clean_name(pesf_nm_pessoa_fisica) as pesf_nm_pessoa_fisica,
                regexp_replace(pesf_cpf, '[^0-9]', '') as pesf_cpf,
                clean_name(pesf_nm_mae) as pesf_nm_mae,
                pesf_dt_nasc,
                regexp_replace(pesf_nr_rg, '[^0-9]', '') as pesf_nr_rg
            FROM PERS_DOCS_PIPS
            JOIN {0}.mcpr_pessoa_fisica ON pers_pess_dk = pesf_pess_dk
        )
        WHERE remove_mp(pesf_nm_pessoa_fisica) < {LIMIAR_SIMILARIDADE}
    """.format(schema_exadata, LIMIAR_SIMILARIDADE=LIMIAR_SIMILARIDADE))
    investigados_fisicos_pip_total.createOrReplaceTempView("INVESTIGADOS_FISICOS_PIP_TOTAL")
    spark.catalog.cacheTable('INVESTIGADOS_FISICOS_PIP_TOTAL')

    investigados_juridicos_pip_total = spark.sql("""
        SELECT *
        FROM (
            SELECT
                cast(pesj_pess_dk as int) as pesj_pess_dk,
                clean_name(pesj_nm_pessoa_juridica) as pesj_nm_pessoa_juridica,
                pesj_cnpj
            FROM PERS_DOCS_PIPS
            JOIN {0}.mcpr_pessoa_juridica ON pers_pess_dk = pesj_pess_dk
        )
        WHERE (pesj_cnpj IS NULL OR pesj_cnpj != '28305936000140') -- Sem o IS NULL OR ele tira os valores NULL
        AND remove_mp(pesj_nm_pessoa_juridica) < {LIMIAR_SIMILARIDADE}
    """.format(schema_exadata, REGEX_EXCLUSAO_ORGAOS=REGEX_EXCLUSAO_ORGAOS, LIMIAR_SIMILARIDADE=LIMIAR_SIMILARIDADE))
    investigados_juridicos_pip_total.createOrReplaceTempView("INVESTIGADOS_JURIDICOS_PIP_TOTAL")
    spark.catalog.cacheTable('INVESTIGADOS_JURIDICOS_PIP_TOTAL')

    # PARTITION BY substring(pesf_nm_pessoa_fisica, 1, 1)
    similarity_nome_dtnasc = spark.sql("""
        SELECT pess_dk, MIN(pess_dk) OVER(PARTITION BY grupo, fl) AS representante_dk
        FROM (
            SELECT
                pess_dk,
                substring(pesf_nm_pessoa_fisica, 1, 1) as fl,
                SUM(col_grupo) OVER(PARTITION BY substring(pesf_nm_pessoa_fisica, 1, 1) ORDER BY pesf_dt_nasc, pesf_nm_pessoa_fisica, pess_dk ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as grupo
            FROM (
                SELECT
                    pesf_pess_dk as pess_dk,
                    pesf_nm_pessoa_fisica,
                    pesf_dt_nasc,
                    CASE
                        name_similarity(
                            pesf_nm_pessoa_fisica,
                            LAG(pesf_nm_pessoa_fisica) OVER(PARTITION BY pesf_dt_nasc ORDER BY pesf_dt_nasc, pesf_nm_pessoa_fisica, pesf_pess_dk)
                            ) <= {LIMIAR_SIMILARIDADE}
                        WHEN true THEN 1 ELSE 0 END as col_grupo
                FROM INVESTIGADOS_FISICOS_PIP_TOTAL
                WHERE pesf_dt_nasc IS NOT NULL) t
            ) t2
    """.format(LIMIAR_SIMILARIDADE=LIMIAR_SIMILARIDADE))
    similarity_nome_dtnasc.createOrReplaceTempView("SIMILARITY_NOME_DTNASC")

    similarity_nome_nomemae = spark.sql("""
        SELECT pess_dk, MIN(pess_dk) OVER(PARTITION BY grupo, fl) AS representante_dk
        FROM (
            SELECT
                pess_dk,
                substring(pesf_nm_pessoa_fisica, 1, 1) as fl,
                SUM(col_grupo + col_grupo_mae) OVER(PARTITION BY substring(pesf_nm_pessoa_fisica, 1, 1) ORDER BY pesf_nm_pessoa_fisica, pesf_nm_mae, pess_dk ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as grupo
            FROM (
                SELECT
                    pesf_pess_dk as pess_dk,
                    pesf_nm_pessoa_fisica,
                    pesf_nm_mae,
                    CASE
                        name_similarity(
                            pesf_nm_pessoa_fisica,
                            LAG(pesf_nm_pessoa_fisica) OVER(PARTITION BY substring(pesf_nm_pessoa_fisica, 1, 1) ORDER BY pesf_nm_pessoa_fisica, pesf_nm_mae, pesf_pess_dk)
                            ) <= {LIMIAR_SIMILARIDADE}
                        WHEN true THEN 1 ELSE 0 END as col_grupo,
                    CASE
                        name_similarity(
                            pesf_nm_mae,
                            LAG(pesf_nm_mae) OVER(PARTITION BY substring(pesf_nm_mae, 1, 1) ORDER BY pesf_nm_pessoa_fisica, pesf_nm_mae, pesf_pess_dk)
                            ) <= {LIMIAR_SIMILARIDADE}
                        WHEN true THEN 1 ELSE 0 END as col_grupo_mae
                FROM INVESTIGADOS_FISICOS_PIP_TOTAL
                WHERE pesf_nm_mae IS NOT NULL AND pesf_nm_mae != ''
                AND pesf_nm_mae NOT REGEXP 'IDENTIFICAD[OA]|IGNORAD[OA]|DECLARAD[OA]'
                ) t
            ) t2
    """.format(LIMIAR_SIMILARIDADE=LIMIAR_SIMILARIDADE))
    similarity_nome_nomemae.createOrReplaceTempView("SIMILARITY_NOME_NOMEMAE")

    similarity_nome_rg = spark.sql("""
        SELECT pess_dk, MIN(pess_dk) OVER(PARTITION BY grupo, fl) AS representante_dk
        FROM (
            SELECT
                pess_dk,
                substring(pesf_nm_pessoa_fisica, 1, 1) as fl,
                SUM(col_grupo) OVER(PARTITION BY substring(pesf_nm_pessoa_fisica, 1, 1) ORDER BY pesf_nr_rg, pesf_nm_pessoa_fisica, pess_dk ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as grupo
            FROM (
                SELECT
                    pesf_pess_dk as pess_dk,
                    pesf_nm_pessoa_fisica,
                    pesf_nr_rg,
                    CASE
                        name_similarity(
                            pesf_nm_pessoa_fisica,
                            LAG(pesf_nm_pessoa_fisica) OVER(PARTITION BY pesf_nr_rg ORDER BY pesf_nr_rg, pesf_nm_pessoa_fisica, pesf_pess_dk)
                            ) <= {LIMIAR_SIMILARIDADE}
                        WHEN true THEN 1 ELSE 0 END as col_grupo
                FROM INVESTIGADOS_FISICOS_PIP_TOTAL
                WHERE length(pesf_nr_rg) = 9 AND pesf_nr_rg != '000000000') t
            ) t2
    """.format(LIMIAR_SIMILARIDADE=LIMIAR_SIMILARIDADE))
    similarity_nome_rg.createOrReplaceTempView("SIMILARITY_NOME_RG")

    T0 = spark.sql("""
        SELECT pesf_pess_dk as pess_dk, pesf_pess_dk as representante_dk
        FROM INVESTIGADOS_FISICOS_PIP_TOTAL
        UNION ALL
        SELECT pesf_pess_dk as pess_dk, MIN(pesf_pess_dk) OVER(PARTITION BY pesf_cpf) as representante_dk
        FROM INVESTIGADOS_FISICOS_PIP_TOTAL
        WHERE pesf_cpf IS NOT NULL 
        AND pesf_cpf NOT IN ('00000000000', '') -- valores invalidos de CPF
        UNION ALL
        SELECT pesf_pess_dk as pess_dk, MIN(pesf_pess_dk) OVER(PARTITION BY pesf_nr_rg, pesf_dt_nasc) as representante_dk
        FROM INVESTIGADOS_FISICOS_PIP_TOTAL
        WHERE pesf_dt_nasc IS NOT NULL
        AND length(pesf_nr_rg) = 9 AND pesf_nr_rg != '000000000'
        UNION ALL
        SELECT pess_dk, representante_dk
        FROM SIMILARITY_NOME_DTNASC
        UNION ALL
        SELECT pess_dk, representante_dk
        FROM SIMILARITY_NOME_NOMEMAE
        UNION ALL
        SELECT pess_dk, representante_dk
        FROM SIMILARITY_NOME_RG
    """)
    T0.createOrReplaceTempView("T0")
    spark.catalog.cacheTable("T0")

    # Pode haver "vácuos horizontais" entre os representantes de um grupo
    # Esses JOINs estão aqui para evitar isso
    # Fazer os JOINs 3 vezes garante que, para todos os casos, o problema é sanado (porém é custoso...)
    # Ele é, então, feito uma única vez, que vai pegar a maioria dos casos.
    T1 = spark.sql("""
        SELECT DISTINCT A.representante_dk as r1, B.representante_dk as r2
        FROM T0 A
        JOIN T0 B ON A.pess_dk = B.pess_dk AND A.representante_dk != B.representante_dk
    """)
    T1.createOrReplaceTempView("T1")

    T2 = spark.sql("""
        -- SELECT A.r2 as r1, B.r2 as r2
        -- FROM T1 A
        -- JOIN T1 B ON A.r1 = B.r1 AND A.r2 != B.r2
        -- UNION ALL
        -- SELECT *
        -- FROM T1
        SELECT r1 as pess_dk, r2 as representante_dk
        FROM T1
        UNION ALL
        SELECT pess_dk, representante_dk
        FROM T0
    """)
    T2.createOrReplaceTempView("T2")
    spark.catalog.cacheTable("T2")

    # T3 = spark.sql("""
    #     SELECT A.r2 as pess_dk, B.r2 as representante_dk
    #     FROM T2 A
    #     JOIN T2 B ON A.r1 = B.r1 AND A.r2 != B.r2
    #     UNION ALL
    #     SELECT pess_dk, representante_dk
    #     FROM T0
    # """)
    # T3.createOrReplaceTempView("T3")

    pessoas_fisicas_representativas_1 = spark.sql("""
        SELECT t.pess_dk, min(t.representante_dk) as representante_dk
        FROM T2 t
        GROUP BY t.pess_dk
    """)

    pessoas_juridicas_representativas_1 = spark.sql("""
        SELECT t.pess_dk, min(t.representante_dk) as representante_dk
        FROM (
            SELECT pesj_pess_dk as pess_dk, pesj_pess_dk as representante_dk
            FROM INVESTIGADOS_JURIDICOS_PIP_TOTAL
            UNION ALL
            SELECT pesj_pess_dk as pess_dk, MIN(pesj_pess_dk) OVER(PARTITION BY pesj_cnpj) as representante_dk
            FROM INVESTIGADOS_JURIDICOS_PIP_TOTAL B
            WHERE pesj_cnpj IS NOT NULL
            AND pesj_cnpj != '00000000000000'
            AND pesj_cnpj != '00000000000'
        ) t
        GROUP BY t.pess_dk
    """)
    pessoas_fisicas_representativas_1.createOrReplaceTempView("REPR_FISICO_1")
    pessoas_juridicas_representativas_1.createOrReplaceTempView("REPR_JURIDICO_1")

    repr_1 = spark.sql("""
        SELECT * FROM REPR_FISICO_1
        UNION ALL
        SELECT * FROM REPR_JURIDICO_1
    """)
    repr_1.createOrReplaceTempView("REPR_1")

    # Se 1 e representante de 2, e 2 e representante de 3, entao 1 deve ser representante de 3
    pessoas_representativas_2 = spark.sql("""
        SELECT A.pess_dk, B.representante_dk,
        pesf_nm_pessoa_fisica as pess_pesf_nm_pessoa_fisica,
        pesf_nm_mae as pess_pesf_nm_mae,
        pesf_cpf as pess_pesf_cpf,
        pesf_nr_rg as pess_pesf_nr_rg,
        pesf_dt_nasc as pess_pesf_dt_nasc,
        pesj_nm_pessoa_juridica as pess_pesj_nm_pessoa_juridica,
        pesj_cnpj as pess_pesj_cnpj
        FROM REPR_1 A
        JOIN REPR_1 B ON A.representante_dk = B.pess_dk
        LEFT JOIN {0}.mcpr_pessoa_fisica ON A.pess_dk = pesf_pess_dk
        LEFT JOIN {0}.mcpr_pessoa_juridica ON A.pess_dk = pesj_pess_dk
    """.format(schema_exadata))
    pessoas_representativas_2.createOrReplaceTempView('T_FINAL')

    resultado = spark.sql("""
        SELECT
            pess_dk,
            representante_dk,
            pess_pesf_nm_pessoa_fisica,
            pess_pesf_nm_mae,
            pess_pesf_cpf,
            pess_pesf_nr_rg,
            pess_pesf_dt_nasc,
            pess_pesj_nm_pessoa_juridica,
            pess_pesj_cnpj,
            cast(substring(cast(representante_dk as string), -1, 1) as int) as rep_last_digit
        FROM T_FINAL
        UNION ALL
        SELECT
            pers_pess_dk as pess_dk,
            pers_pess_dk as representante_dk,
            NULL AS pess_pesf_nm_pessoa_fisica,
            NULL AS pess_pesf_nm_mae,
            NULL AS pess_pesf_cpf,
            NULL AS pess_pesf_nr_rg,
            NULL AS pess_pesf_dt_nasc,
            pess_nm_pessoa as pess_pesj_nm_pessoa_juridica,
            NULL AS pess_pesj_cnpj,
            cast(substring(cast(pers_pess_dk as string), -1, 1) as int) as rep_last_digit
        FROM (SELECT DISTINCT cast(pers_pess_dk as int) as pers_pess_dk FROM {0}.mcpr_personagem WHERE pers_pesf_dk IS NULL AND pers_pesj_dk IS NULL) T
        JOIN {0}.mcpr_pessoa B ON B.pess_dk = T.pers_pess_dk
        WHERE remove_mp(clean_name(pess_nm_pessoa)) < {LIMIAR_SIMILARIDADE}
        """.format(schema_exadata, LIMIAR_SIMILARIDADE=LIMIAR_SIMILARIDADE))

    table_name = options['table_name']
    table_name = "{}.{}".format(schema_exadata_aux, table_name)
    resultado.repartition('rep_last_digit').write.mode("overwrite").saveAsTable("temp_table_pip_investigados_representantes")
    spark.catalog.clearCache()
    temp_table = spark.table("temp_table_pip_investigados_representantes")
    temp_table.repartition(15).write.mode("overwrite").partitionBy('rep_last_digit').saveAsTable(table_name)
    spark.sql("drop table temp_table_pip_investigados_representantes")

    execute_compute_stats(table_name)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Create table tabela pip_investigados_representantes")
    parser.add_argument('-e','--schemaExadata', metavar='schemaExadata', type=str, help='')
    parser.add_argument('-a','--schemaExadataAux', metavar='schemaExadataAux', type=str, help='')
    parser.add_argument('-i','--impalaHost', metavar='impalaHost', type=str, help='')
    parser.add_argument('-o','--impalaPort', metavar='impalaPort', type=str, help='')
    parser.add_argument('-l','--limiarSimilaridade', metavar='limiarSimilaridade', type=float, default=0.85, help='')
    parser.add_argument('-t','--tableName', metavar='tableName', type=str, help='')
    args = parser.parse_args()

    options = {
                    'schema_exadata': args.schemaExadata, 
                    'schema_exadata_aux': args.schemaExadataAux,
                    'impala_host' : args.impalaHost,
                    'impala_port' : args.impalaPort,
                    'limiar_similaridade': args.limiarSimilaridade,
                    "table_name": args.tableName,
                }

    execute_process(options)