#-*-coding:utf-8-*-
import argparse
import difflib
import pyspark
import re
import unicodedata

from datetime import datetime, timedelta
from generic_utils import execute_compute_stats

def name_similarity(name_left, name_right):
    if not name_left or not name_right:
        return 0

    def remove_accents_n(value):
        text = unicodedata.normalize('NFD', value)
        text = text.encode('ascii', 'ignore')
        text = text.decode("utf-8")
        return text

    name_left = remove_accents_n(re.sub(r"\s+", "", name_left or ""))
    name_right = remove_accents_n(re.sub(r"\s+", "", name_right or ""))
    return difflib.SequenceMatcher(None, name_left, name_right).ratio()


def execute_process(options):

    spark = pyspark.sql.session.SparkSession \
            .builder \
            .appName("criar_tabela_andamento_processos") \
            .enableHiveSupport() \
            .getOrCreate()

    spark.udf.register("name_similarity", name_similarity)

    schema_exadata = options['schema_exadata']
    schema_exadata_aux = options['schema_exadata_aux']
    personagens_cutoff = options['personagens_cutoff']
    nb_past_days = options['nb_past_days']
    LIMIAR_SIMILARIDADE = options["limiar_similaridade"]
    table_name = options['table_name']

    dt_inicio = datetime.now() - timedelta(nb_past_days)

    spark.sql("""
        SELECT D.*
        FROM {0}.mcpr_documento D
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
            FROM {0}.mcpr_documento
            LEFT JOIN {1}.atualizacao_pj_pacote ON id_orgao = docu_orgi_orga_dk_responsavel
            JOIN {0}.mcpr_vista ON vist_docu_dk = docu_dk
            JOIN {0}.mcpr_andamento ON vist_dk = pcao_vist_dk
            JOIN {0}.mcpr_sub_andamento ON stao_pcao_dk = pcao_dk
            JOIN {0}.mcpr_tp_andamento ON tppr_dk = stao_tppr_dk
        ) A ON vist_docu_dk = docu_dk AND is_arquivamento IS NOT NULL
        WHERE item_docu_dk IS NULL
        AND vist_docu_dk IS NULL
        AND docu_fsdc_dk = 1
        AND docu_tpst_dk != 11
    """.format(schema_exadata, schema_exadata_aux)).createOrReplaceTempView("DOCU_ATIVOS")

    spark.sql(
        """
        SELECT
            docu_orgi_orga_dk_responsavel as orgao_dk,
            cldc_ds_classe as classe_documento,
            DOCU_NR_MP,
            DOCU_NR_EXTERNO,
            docu_tx_etiqueta,
            PCAO_DT_ANDAMENTO,
            TPPR_DESCRICAO,
            TPPR_DK,
            DOCU_DK
        FROM DOCU_ATIVOS
        JOIN {0}.MCPR_VISTA ON VIST_DOCU_DK = DOCU_DK
        JOIN {0}.MCPR_ANDAMENTO ON PCAO_VIST_DK = VIST_DK
        JOIN {0}.MCPR_SUB_ANDAMENTO ON STAO_PCAO_DK = PCAO_DK
        JOIN {0}.MCPR_TP_ANDAMENTO ON TPPR_DK = STAO_TPPR_DK
        JOIN {1}.atualizacao_pj_pacote ON docu_orgi_orga_dk_responsavel = id_orgao
        JOIN {1}.tb_regra_negocio_processo
            ON cod_pct = cod_atribuicao
            AND classe_documento = docu_cldc_dk
        JOIN {0}.mcpr_classe_docto_mp ON cldc_dk = docu_cldc_dk
        WHERE pcao_dt_cancelamento IS NULL
        """.format(schema_exadata, schema_exadata_aux)
    ).createOrReplaceTempView('DOCU_TOTAIS')
    spark.catalog.cacheTable("DOCU_TOTAIS")

    spark.sql(
        """
        WITH PERSONAGENS AS (
            SELECT
                docu_nr_mp,
                pess_dk,
                pess_nm_pessoa,
                LEAD(pess_nm_pessoa) OVER (PARTITION BY docu_nr_mp ORDER BY pess_nm_pessoa) proximo_nome
            FROM DOCU_TOTAIS
            JOIN {0}.mcpr_personagem ON pers_docu_dk = docu_dk
            JOIN {0}.mcpr_pessoa ON pers_pess_dk = pess_dk
            JOIN {0}.mcpr_tp_personagem ON pers_tppe_dk = tppe_dk
            WHERE pers_tppe_dk IN (290, 7, 21, 317, 20, 14, 32, 345, 40, 5, 24)
            GROUP BY docu_nr_mp, pess_nm_pessoa, pess_dk -- remove duplicação de personagens com mesmo
        ),
        PERSONAGENS_SIMILARIDADE AS (
            SELECT docu_nr_mp,
            pess_dk,
            pess_nm_pessoa,
                CASE
                    WHEN name_similarity(pess_nm_pessoa, proximo_nome) > {LIMIAR_SIMILARIDADE} THEN false
                    ELSE true
                END AS primeira_aparicao
            FROM PERSONAGENS
        )
        SELECT
            docu_nr_mp,
            pess_nm_pessoa,
            B.representante_dk,
            row_number() OVER (PARTITION BY docu_nr_mp ORDER BY A.pess_dk DESC) as nr_pers
        FROM PERSONAGENS_SIMILARIDADE A
        JOIN {1}.tb_pip_investigados_representantes B ON A.pess_dk = B.pess_dk
        WHERE primeira_aparicao = true
        """.format(
            schema_exadata, schema_exadata_aux,
            LIMIAR_SIMILARIDADE=LIMIAR_SIMILARIDADE
        )
    ).createOrReplaceTempView("PERSONAGENS_SIMILARIDADE")

    spark.sql(
        """
        SELECT
            docu_nr_mp,
            concat_ws(', ', collect_list(nm_personagem)) as personagens,
            MAX(representante_dk) as representante_dk
        FROM (
            SELECT
                docu_nr_mp,
                CASE
                    WHEN nr_pers = {1} THEN 'e outros...'
                    ELSE pess_nm_pessoa END
                AS nm_personagem,
                nr_pers,
                CASE
                    WHEN nr_pers = 1 THEN representante_dk
                    ELSE NULL END
                AS representante_dk
            FROM
            PERSONAGENS_SIMILARIDADE
            WHERE nr_pers <= {1})
        GROUP BY docu_nr_mp
        """.format(schema_exadata, personagens_cutoff)
    ).createOrReplaceTempView('DOCU_PERSONAGENS')

    spark.sql(
        """
        SELECT
            DOCU_NR_MP,
            MAX(PCAO_DT_ANDAMENTO) AS DT_ULTIMO
        FROM DOCU_TOTAIS
        GROUP BY DOCU_NR_MP
        """
    ).createOrReplaceTempView('DTS_ULTIMOS_ANDAMENTOS')


    lista_processos = spark.sql(
        """
        SELECT
            A.orgao_dk,
            A.classe_documento as cldc_dk,
            A.docu_nr_mp,
            A.docu_nr_externo,
            A.docu_tx_etiqueta,
            P.personagens,
            P.representante_dk,
            A.pcao_dt_andamento as dt_ultimo_andamento,
            concat_ws(', ', collect_list(A.tppr_descricao)) as ultimo_andamento,
            CASE WHEN length(docu_nr_externo) = 20 THEN
                concat('http://www4.tjrj.jus.br/numeracaoUnica/faces/index.jsp?numProcesso=',
                    concat(concat(concat(concat(concat(concat(
                    concat(substr(docu_nr_externo, 1, 7), '-')),
                    concat(substr(docu_nr_externo, 8, 2), '.')),
                    concat(substr(docu_nr_externo, 10, 4), '.')),
                    concat(substr(docu_nr_externo, 14, 1), '.')),
                    concat(substr(docu_nr_externo, 15, 2), '.')),
                    substr(docu_nr_externo, 17, 4)))
                ELSE NULL
            END as url_tjrj
        FROM DOCU_TOTAIS A
        JOIN DTS_ULTIMOS_ANDAMENTOS ULT
            ON A.DOCU_NR_MP = ULT.DOCU_NR_MP
            AND A.PCAO_DT_ANDAMENTO = ULT.DT_ULTIMO
        LEFT JOIN DOCU_PERSONAGENS P ON P.DOCU_NR_MP = A.DOCU_NR_MP
        GROUP BY A.orgao_dk, A.classe_documento, A.docu_nr_mp,
            A.docu_nr_externo, A.docu_tx_etiqueta, P.personagens, P.representante_dk,
            A.pcao_dt_andamento
        """
    )

    table_name = "{}.{}".format(schema_exadata_aux, table_name)

    lista_processos.write.mode("overwrite").saveAsTable("temp_table_lista_processos")
    temp_table = spark.table("temp_table_lista_processos")

    temp_table.write.mode("overwrite").saveAsTable(table_name)
    spark.sql("drop table temp_table_lista_processos")

    execute_compute_stats(table_name)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Create table distribuicao entradas")
    parser.add_argument('-e','--schemaExadata', metavar='schemaExadata', type=str, help='')
    parser.add_argument('-a','--schemaExadataAux', metavar='schemaExadataAux', type=str, help='')
    parser.add_argument('-i','--impalaHost', metavar='impalaHost', type=str, help='')
    parser.add_argument('-o','--impalaPort', metavar='impalaPort', type=str, help='')
    parser.add_argument('-c','--personagensCutoff', metavar='personagensCutoff', type=int, default=2, help='')
    parser.add_argument('-p','--nbPastDays', metavar='nbPastDays', type=int, default=7, help='')
    parser.add_argument('-l','--limiarSimilaridade', metavar='limiarSimilaridade', type=float, default=0.85, help='')
    parser.add_argument('-t','--tableName', metavar='tableName', type=str, help='')
    
    args = parser.parse_args()

    options = {
                    'schema_exadata': args.schemaExadata,
                    'schema_exadata_aux': args.schemaExadataAux,
                    'impala_host' : args.impalaHost,
                    'impala_port' : args.impalaPort,
                    'personagens_cutoff' : args.personagensCutoff,
                    'nb_past_days': args.nbPastDays,
                    'limiar_similaridade': args.limiarSimilaridade,
                    'table_name' : args.tableName,
                }

    execute_process(options)
