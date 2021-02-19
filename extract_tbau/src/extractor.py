#-*-coding:utf-8-*-
import argparse
import pyspark

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

from generic_utils import execute_compute_stats

def extract_tbau_documento(spark):
    columns = [
		col("DOCU_DK").alias("DOAT_DOCU_DK"),
		col("DOCU_NR_EXTERNO").alias("DOAT_DOCU_NR_EXTERNO"),
		col("DOCU_NR_MP").alias("DOAT_DOCU_NR_MP"),
		col("DOCU_DT_CADASTRO").alias("DOAT_DOCU_DT_CADASTRO"),
		col("DOCU_DT_FATO").alias("DOAT_DOCU_DT_FATO"),
		col("DOCU_ORGI_ORGA_DK_RESPONSAVEL").alias("DOAT_ORGI_DK_RESPONSAVEL"),
		col("DOCU_ORGI_ORGA_DK_CARGA").alias("DOAT_ORGI_DK_CARGA"),
		col("DOCU_ORGA_DK_ORIGEM").alias("DOAT_ORGA_DK_ORIGEM"),
		col("DOCU_ORGE_ORGA_DK_DELEG_FATO").alias("DOAT_ORGE_DK_DELEG_FATO"),
		col("DOCU_ORGE_ORGA_DK_DELEG_ORIGEM").alias("DOAT_ORGE_DK_ORIGEM"),
		col("DOCU_ORGE_ORGA_DK_VARA").alias("DOAT_ORGE_DK_VARA"),
		col("DOCU_NR_DISTRIBUICAO").alias("DOAT_DOCU_NR_DISTRIBUICAO"),
		col("DOCU_DT_DISTRIBUICAO").alias("DOAT_DOCU_DT_DISTRIBUICAO"),
		col("DOCU_IN_DOCUMENTO_ELETRONICO").alias("DOAT_DOCU_IN_DOC_ELETRONICO"),
		col("DOCU_CLDC_DK").alias("DOAT_CLDC_DK"),
		col("NISI_DS_NIVEL_SIGILO").alias("DOAT_NISI_DS_NIVEL_SIGILO"),
		col("MATE_DESCRICAO").alias("DOAT_MATE_ATRIBUICAO_DOC"),
		col("TPDC_SIGLA").alias("DOAT_TPDC_SIGLA_DOC"),
		col("TPDC_DESCRICAO").alias("DOAT_TPDC_DS_DOCUMENTO"),
		col("DOAT_ORGAO_RESPONSAVEL"),
		col("DOAT_CRAAI_OR"),
		col("DOAT_COMARCA_OR"),
		col("DOAT_FORO_OR"),
		col("DOAT_ORGAO_TP_OR"),
		col("DOAT_ORGAO_A_E_OR"),
		col("DOAT_JUIZO_UNICO_OR"),
		col("DOAT_DT_INICIO_OR"),
		col("DOAT_DT_FIM_OR"),
		col("DOAT_DET_CRIACAO_OR"),
		col("DOAT_ORGAO_CARGA"),
		col("DOAT_CRAAI_CG"),
		col("DOAT_COMARCA_CG"),
		col("DOAT_ORGAO_TP_CG"),
		col("DOAT_ORGAO_A_E_CG"),
		col("DOAT_JUIZO_UNICO_CG"),
		col("DOAT_DT_FIM_CG"),
		col("DOAT_NM_ORGAO_EXTERNO"),
		col("TPOE_DESCRICAO").alias("DOAT_TP_ORGAO_EXTERNO"),
		col("DOAT_NM_DELEF_FATO"),
		col("DOAT_NM_DELEG_ORIGEM"),
		col("DOAT_NM_VARA"),
		col("TPST_DS_TP_SITUACAO").alias("DOAT_TPST_DS_TP_SITUACAO"),
		col("FSDC_DS_FASE").alias("DOAT_FSDC_DS_FASE"),
		col("cldc_cd_classe").alias("DOAT_CD_CLASSE"),
		col("cldc_ds_classe").alias("DOAT_CLASSE"),
		col("cldc_ds_hierarquia").alias("DOAT_CLASSE_HIERARQUIA"),
		col("DOAA_DT_ALTERACAO").alias("DOAT_DT_ALTERACAO"),
	]
    documento = spark.table("%s.mcpr_documento" % options["schema_exadata"]).\
		filter("DOCU_DT_CANCELAMENTO IS NULL")
    sigilo = spark.table("%s.mcpr_nivel_sigilo" % options["schema_exadata"])
    materia = spark.table("%s.mprj_materia_mgp" % options["schema_exadata"])
    tipo_doc = spark.table("%s.mcpr_tp_documento" % options["schema_exadata"])
    alteracao = spark.table("%s.mcpr_documento_alteracao" % options["schema_exadata"])
    sit_doc = spark.table("%s.mcpr_tp_situacao_documento" % options["schema_exadata"])
    fase_doc = spark.table("%s.mcpr_fases_documento" % options["schema_exadata"])
    orgao_origem = spark.table("%s.mprj_orgao_ext" % options["schema_exadata"]).select([
        col("ORGE_ORGA_DK").alias("ORG_EXT_ORIGEM_DK"),
		col("ORGE_TPOE_DK").alias("ORG_EXT_TPOE_DK"),
		col("ORGE_NM_ORGAO").alias("DOAT_NM_ORGAO_EXTERNO"),
	])
    orgao_dp_fato = spark.table("%s.mprj_orgao_ext" % options["schema_exadata"]).select([
		col("ORGE_ORGA_DK").alias("ORG_EXT_DP_FATO_DK"),
		col("ORGE_NM_ORGAO").alias("DOAT_NM_DELEF_FATO"),
	])
    orgao_dp_origem = spark.table("%s.mprj_orgao_ext" % options["schema_exadata"]).select([
		col("ORGE_ORGA_DK").alias("ORG_EXT_DP_ORIGEM_DK"),
		col("ORGE_NM_ORGAO").alias("DOAT_NM_DELEG_ORIGEM"),
	])
    orgao_vara = spark.table("%s.mprj_orgao_ext" % options["schema_exadata"]).select([
		col("ORGE_ORGA_DK").alias("ORG_EXT_VARA_DK"),
		col("ORGE_NM_ORGAO").alias("DOAT_NM_VARA"),
	])
    tp_orgao_ext = spark.table("%s.mprj_tp_orgao_ext" % options["schema_exadata"])
    classe_doc = spark.table("%s.mmps_classe_hierarquia" % options["schema_exadata_aux"])
    local_resp = spark.table("%s.orgi_vw_orgao_local_atual" % options["schema_exadata"]).select([
		col("ORLW_DK").alias("LOC_RESP_DK"),
		col("ORLW_ORGI_TPOR_DK").alias("LOC_RESP_TPOR_DK"),
		col("ORLW_ORGI_NM_ORGAO").alias("DOAT_ORGAO_RESPONSAVEL"),
		col("ORLW_REGI_NM_REGIAO").alias("DOAT_CRAAI_OR"),
		col("ORLW_CMRC_NM_COMARCA").alias("DOAT_COMARCA_OR"),
		col("ORLW_COFO_NM_FORO").alias("DOAT_FORO_OR"),
		col("ORLW_ORGI_IN_JUIZO_UNICO").alias("DOAT_JUIZO_UNICO_OR"),
		col("ORLW_ORGI_DT_INICIO").alias("DOAT_DT_INICIO_OR"),
		col("ORLW_ORGI_DT_FIM").alias("DOAT_DT_FIM_OR"),
		col("ORLW_ORGI_DET_CRIACAO").alias("DOAT_DET_CRIACAO_OR"),
	])
    local_carga = spark.table("%s.orgi_vw_orgao_local_atual" % options["schema_exadata"]).select([
		col("ORLW_DK").alias("LOC_CARGA_DK"),
		col("ORLW_ORGI_TPOR_DK").alias("LOC_CARGA_TPOR_DK"),
		col("ORLW_ORGI_NM_ORGAO").alias("DOAT_ORGAO_CARGA"),
		col("ORLW_REGI_NM_REGIAO").alias("DOAT_CRAAI_CG"),
		col("ORLW_CMRC_NM_COMARCA").alias("DOAT_COMARCA_CG"),
		col("ORLW_ORGI_IN_JUIZO_UNICO").alias("DOAT_JUIZO_UNICO_CG"),
		col("ORLW_ORGI_DT_FIM").alias("DOAT_DT_FIM_CG"),
	])
    tp_local_resp = spark.table("%s.orgi_tp_orgao" % options["schema_exadata"]).select([
		col("TPOR_DK").alias("TP_LOC_RESP_DK"),
		col("TPOR_DS_TP_ORGAO").alias("DOAT_ORGAO_TP_OR"),
		col("TPOR_CLASSIFICACAO").alias("DOAT_ORGAO_A_E_OR"),
	])
    tp_local_carga = spark.table("%s.orgi_tp_orgao" % options["schema_exadata"]).select([
		col("TPOR_DK").alias("TP_LOC_CARGA_DK"),
		col("TPOR_DS_TP_ORGAO").alias("DOAT_ORGAO_TP_CG"),
		col("TPOR_CLASSIFICACAO").alias("DOAT_ORGAO_A_E_CG"),
	])
    
    doc_sigilo = documento.join(sigilo, documento.DOCU_NISI_DK == sigilo.NISI_DK, "left")
    doc_materia = doc_sigilo.join(materia, doc_sigilo.DOCU_MATE_DK == materia.MATE_DK, "left")
    doc_tipo = doc_materia.join(tipo_doc, doc_materia.DOCU_TPDC_DK == tipo_doc.TPDC_DK, "inner")
    # doc_tipo = doc_sigilo.join(tipo_doc, doc_sigilo.DOCU_TPDC_DK == tipo_doc.TPDC_DK, "inner")
    doc_alteracao = doc_tipo.join(alteracao, alteracao.DOAA_DOCU_DK == doc_tipo.DOCU_DK, "inner")
    doc_sit = doc_alteracao.join(sit_doc, doc_alteracao.DOCU_TPST_DK == sit_doc.TPST_DK, "left")
    # doc_sit = doc_tipo.join(sit_doc, doc_tipo.DOCU_TPST_DK == sit_doc.TPST_DK, "left")
    doc_fase = doc_sit.join(fase_doc, doc_sit.DOCU_FSDC_DK == fase_doc.FSDC_DK, "left")
    doc_origem = doc_fase.join(orgao_origem, doc_fase.DOCU_ORGA_DK_ORIGEM == orgao_origem.ORG_EXT_ORIGEM_DK, "left")
    doc_tp_ext = doc_origem.join(tp_orgao_ext, doc_origem.ORG_EXT_TPOE_DK == tp_orgao_ext.TPOE_DK , "left")
    doc_classe = doc_tp_ext.join(classe_doc, doc_tp_ext.DOCU_CLDC_DK == classe_doc.cldc_dk , "left")
    # doc_classe = doc_origem.join(classe_doc, doc_origem.DOCU_CLDC_DK == classe_doc.cldc_dk , "left")
    doc_loc_resp = doc_classe.join(local_resp, doc_classe.DOCU_ORGI_ORGA_DK_RESPONSAVEL == local_resp.LOC_RESP_DK , "left")
    doc_tp_loc_resp = doc_loc_resp.join(tp_local_resp, doc_loc_resp.LOC_RESP_TPOR_DK == tp_local_resp.TP_LOC_RESP_DK , "left")
    doc_loc_carga = doc_tp_loc_resp.join(local_carga, doc_tp_loc_resp.DOCU_ORGI_ORGA_DK_CARGA == local_carga.LOC_CARGA_DK , "left")
    doc_tp_carga_resp = doc_loc_carga.join(tp_local_carga, doc_loc_carga.LOC_CARGA_TPOR_DK == tp_local_carga.TP_LOC_CARGA_DK , "left")
    doc_dp_fato = doc_tp_carga_resp.join(orgao_dp_fato, doc_tp_carga_resp.DOCU_ORGE_ORGA_DK_DELEG_FATO == orgao_dp_fato.ORG_EXT_DP_FATO_DK, "left")
    doc_dp_origem = doc_dp_fato.join(orgao_dp_origem, doc_dp_fato.DOCU_ORGE_ORGA_DK_DELEG_ORIGEM == orgao_dp_origem.ORG_EXT_DP_ORIGEM_DK, "left")
    doc_dp_vara = doc_dp_origem.join(orgao_vara, doc_dp_origem.DOCU_ORGE_ORGA_DK_VARA == orgao_vara.ORG_EXT_VARA_DK, "left")
    
    return doc_dp_vara.select(columns)


def extract_tbau_andamento(spark):
	pass


def extract_tbau_assunto(spark):
    columns = [
		col("ASDO_DOCU_DK").alias("DASN_DOCU_DK"),
		col("ASDO_DK").alias("DASN_DK"),
		col("ASSU_TX_DISPOSITIVO_LEGAL").alias("DASN_TP_LEGAL"),
		col("ASSU_NM_ASSUNTO").alias("DASN_NM_ASSUNTO"),
		col("ASSU_CD_CNJ").alias("DASN_CD_CNJ"),
		col("ASSU_CD_ASSUNTO").alias("DASN_CD_ASSUNTO"),
		col("ASSU_DK").alias("DASN_ASSU_DK"),
		col("ASSU_ASSU_DK").alias("DASN_ASSU_ASSU_DK_PAI"),
	]

    assunto_documento = spark.table("%s.mcpr_assunto_documento" % options["schema_exadata"])
    assunto = spark.table("%s.mcpr_assunto" % options["schema_exadata"])
    doc_assunto_join = assunto_documento.join(assunto, assunto_documento.ASDO_ASSU_DK == assunto.ASSU_DK, "inner")
    
    return doc_assunto_join.select(columns).distinct()

def extract_tbau_movimentacao(spark):
	pass


def extract_tbau_personagem(spark):
	sf1_columns = [
		col("PESF_PESS_DK").alias("PESFDK"),
        col("PESF_SEXO").alias("SEX"),
		col("ESCO_DESCRICAO").alias("ESCOL"),
		col("ECIV_DESCRICAO").alias("ECIVIL"),
		col("CORP_DESCRICAO").alias("CPELE"),
		col("PESF_DT_NASC").alias("DT_NASC"),
	]

	sf2_columns = [
		col("ENPE_PESS_DK").alias("ENPEDK"),
        col("ENDC_CEP").alias("ECEP"),
		col("ECIDA"),
		col("EUFED"),
		col("EBAIR"),
	]

	columns = [
		col("PERS_PESS_DK").alias("DPSG_PERS_PESS_DK"),
        col("PERS_DOCU_DK").alias("DPSG_DOCU_DK"),
		col("PESS_NM_PESSOA").alias("DPSG_PESS_NM_PESSOA"),
		col("TPPE_DESCRICAO").alias("DPSG_TPPE_DESCRICAO"),
		col("TPAT_DS_AUTORIDADE").alias("DPSG_TPAT_DS_AUTORIDADE"),
		col("REND_DESCRICAO").alias("DPSG_REND_DESCRICAO"),
		col("PESS_IN_TP_PESSOA").alias("DPSG_PESS_IN_TP_PESSOA"),
        col("SEX").alias("DPSG_SEXO"),
		col("ESCOL").alias("DPSG_ESCOLARIDADE"),
		col("ECIVIL").alias("DPSG_ESTADO_CIVIL"),
		col("CPELE").alias("DPSG_COR_PELE"),
		col("DT_NASC").alias("DPSG_DT_NASCIMENTO"),
		col("ECEP").alias("DPSG_CEP_PERSONAGEM"),
		col("EBAIR").alias("DPSG_BAIRRO_PERSONAGEM"),
		col("ECIDA").alias("DPSG_CIDADE_PERSONAGEM"),
		col("EUFED").alias("DPSG_UF_PERSONAGEM"),
	]

	pessoa_fisica = spark.table("%s.mcpr_pessoa_fisica" % options["schema_exadata"])
	escolaridade = spark.table("%s.mcpr_escolaridade" % options["schema_exadata"])
	pessoa_escolaridade = pessoa_fisica.join(escolaridade, pessoa_fisica.PESF_ESCO_DK == escolaridade.ESCO_DK, "left")
	estado_civil = spark.table("%s.mcpr_estado_civil" % options["schema_exadata"])
	pessoa_estado_civil = pessoa_escolaridade.join(estado_civil, pessoa_escolaridade.PESF_ECIV_DK == estado_civil.ECIV_DK, "left")
	cor_pele = spark.table("%s.mcpr_cor_pele" % options["schema_exadata"])
	pessoa_cor_pele = pessoa_estado_civil.join(cor_pele, pessoa_estado_civil.PESF_CORP_DK == cor_pele.CORP_DK, "left")
	sf1 = pessoa_cor_pele.select(sf1_columns).distinct()

	end_pes = spark.table("%s.mcpr_endereco_pessoa" % options["schema_exadata"])
	endereco = spark.table("%s.mcpr_enderecos" % options["schema_exadata"]).select(
		["ENDC_DK", "ENDC_BAIR_DK", "ENDC_NM_BAIRRO", "ENDC_CEP", "ENDC_CIDA_DK", "ENDC_NM_CIDADE", "ENDC_NM_ESTADO", "ENDC_UFED_DK"]
	)
	endereco_pessoa = end_pes.join(endereco, end_pes.ENPE_ENDC_DK == endereco.ENDC_DK, "left")
	cidade = spark.table("%s.mprj_cidade" % options["schema_exadata"])
	endereco_cidade = endereco_pessoa.join(cidade, endereco_pessoa.ENDC_CIDA_DK == cidade.CIDA_DK, "left")
	uf = spark.table("%s.mprj_uf" % options["schema_exadata"])
	endereco_uf = endereco_cidade.join(uf, endereco_cidade.CIDA_UFED_DK == uf.UFED_DK, "left")
	bairro = spark.table("%s.mprj_bairro" % options["schema_exadata"])
	endereco_bairro = endereco_uf.join(
		bairro,
		[
			endereco_uf.ENDC_CIDA_DK == bairro.BAIR_CIDA_DK,
			endereco_uf.ENDC_BAIR_DK == bairro.BAIR_DK,
		],
		"left"
	).withColumn(
		'ECIDA',
        coalesce(
			col('CIDA_NM_CIDADE'),
            col('ENDC_NM_CIDADE')
        )
    ).withColumn(
		'EUFED',
        coalesce(
			col('UFED_SIGLA'),
            col('ENDC_NM_ESTADO')
        )
    ).withColumn(
		'EBAIR',
        coalesce(
			col('BAIR_NM_BAIRRO'),
            col('ENDC_NM_BAIRRO')
        )
    )
	sf2 = endereco_bairro.select(sf2_columns).distinct()

	personagem = spark.table("%s.mcpr_personagem" % options["schema_exadata"])
	tipo_personagem = spark.table("%s.mcpr_tp_personagem" % options["schema_exadata"])
	personagem_tipo = personagem.join(tipo_personagem, personagem.PERS_TPPE_DK == tipo_personagem.TPPE_DK, "inner")
	pessoa = spark.table("%s.mcpr_pessoa" % options["schema_exadata"])
	personagem_pessoa = personagem_tipo.join(pessoa, personagem_tipo.PERS_PESS_DK == pessoa.PESS_DK, "inner")
	tipo_autoridade = spark.table("%s.mcpr_tp_autoridade" % options["schema_exadata"])
	personagem_autoridade = personagem_pessoa.join(tipo_autoridade, personagem_pessoa.PERS_TPAT_DK == tipo_autoridade.TPAT_DK, "left")
	perfil = spark.table("%s.mcpr_perfil" % options["schema_exadata"])
	personagem_perfil = personagem_autoridade.join(perfil, personagem_autoridade.PERS_PESF_DK == perfil.PERF_PESF_PESS_DK, "left")
	renda = spark.table("%s.mcpr_faixa_renda" % options["schema_exadata"])
	personagem_renda = personagem_perfil.join(renda, personagem_perfil.PERF_REND_DK == renda.REND_DK, "left")
	personagem_sf1 = personagem_renda.join(
		sf1,
		[
			sf1.PESFDK == personagem_renda.PERS_PESS_DK,
			personagem_renda.PESS_IN_TP_PESSOA.isin(['I', 'J']) == False, 
		],
		"left"
	)
	personagem_sf2 = personagem_sf1.join(
		sf2,
		[
			sf2.ENPEDK == personagem_sf1.PERS_PESS_DK,
			personagem_sf1.PESS_IN_TP_PESSOA != 'I',
		],
		"left"
	)

	return personagem_sf2.filter("PERS_DT_FIM IS NULL").select(columns).distinct()


def extract_tbau_consumo(spark):
	pass


def extract_tbau_endereco(spark):
	columns = [
		col("EDOC_DOCU_DK").alias("DODR_DOCU_DK"),
		concat(
			col("TPLO_DS_LOGRADOURO"),
			lit(" "),
			col("ENDC_LOGRADOURO"),
			lit(" "),
			col("ENDC_NUMERO")
		).alias("DODR_ENDERECO"),
		col("DODR_NM_BAIRRO"),
        col("ENDC_CEP").alias("DODR_CEP"),
        col("DODR_NM_CIDADE"),
        col("DODR_UFED"),
	]

	doc_endereco = spark.table("%s.mcpr_endereco_documento" % options["schema_exadata"])
	endereco = spark.table("%s.mcpr_enderecos" % options["schema_exadata"]).filter("ENDC_CIDA_DK IS NOT NULL")
	cidade = spark.table("%s.mprj_cidade" % options["schema_exadata"])
	estado = spark.table("%s.mprj_uf" % options["schema_exadata"])
	bairro = spark.table("%s.mprj_bairro" % options["schema_exadata"])
	tipo_logradouro = spark.table("%s.mprj_tp_logradouro" % options["schema_exadata"])

	end_documento = doc_endereco.join(endereco, doc_endereco.EDOC_ENDC_DK == endereco.ENDC_DK, "inner")
	end_cidade = end_documento.join(cidade, end_documento.ENDC_CIDA_DK == cidade.CIDA_DK, "left")
	end_estado = end_cidade.join(estado, end_cidade.CIDA_UFED_DK == estado.UFED_DK, "left")
	end_bairro = end_estado.join(bairro, [
		end_estado.ENDC_CIDA_DK == bairro.BAIR_CIDA_DK,
	 	end_estado.ENDC_BAIR_DK == bairro.BAIR_DK,
	], "left")
	end_tp_logradouro = end_bairro.join(tipo_logradouro, end_bairro.ENDC_TPLO_DK == tipo_logradouro.TPLO_DK, "left").\
		withColumn(
            'DODR_NM_BAIRRO',
            coalesce(
                col('BAIR_NM_BAIRRO'),
                col('ENDC_NM_BAIRRO')
            )
        ).\
		withColumn(
            'DODR_NM_CIDADE',
            coalesce(
                col('CIDA_NM_CIDADE'),
                col('ENDC_NM_CIDADE')
            )
        ).\
		withColumn(
            'DODR_UFED',
            coalesce(
                col('UFED_SIGLA'),
                col('ENDC_NM_ESTADO')
            )
        )

	return end_tp_logradouro.select(columns).distinct()


def generate_tbau(spark, generator, schema, table_name):
	dataframe = generator(spark)
	full_table_name = "{}.{}".format(schema, table_name)
	
	table_df = spark.createDataFrame(dataframe)
	table_df.coalesce(20).write.format('parquet').saveAsTable(full_table_name, mode='overwrite')
	
	execute_compute_stats(full_table_name)
	print("{} gravada".format(table_name))


def execute_process(options):

    spark = pyspark.sql.session.SparkSession\
        .builder\
        .appName("tabelas_tbau")\
        .enableHiveSupport()\
        .getOrCreate()

    sc = spark.sparkContext

    # schema_exadata = options['schema_exadata']
    schema_exadata_aux = options['schema_exadata_aux']
	
    generate_tbau(spark, extract_tbau_documento, schema_exadata_aux, "tbau_documento")
    # generate_tbau(spark, extract_tbau_andamento, schema_exadata_aux, "tbau_documento_andamento")
    generate_tbau(spark, extract_tbau_assunto, schema_exadata_aux, "tbau_documento_assunto")
    # generate_tbau(spark, extract_tbau_movimentacao, schema_exadata_aux, "tbau_documento_movimentacao")
    generate_tbau(spark, extract_tbau_personagem, schema_exadata_aux, "tbau_documento_personagem")
    # generate_tbau(spark, extract_tbau_consumo, schema_exadata_aux, "tbau_material_consumo")
    # generate_tbau(spark, extract_tbau_endereco, schema_exadata_aux, "tbau_documento_endereco")
	#TODO Adicionar mprj_tp_logradouro para liberar os endereços


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Create tables tbau")
    parser.add_argument('-e','--schemaExadata', metavar='schemaExadata', type=str, help='')
    parser.add_argument('-a','--schemaExadataAux', metavar='schemaExadataAux', type=str, help='')
    parser.add_argument('-i','--impalaHost', metavar='impalaHost', type=str, help='')
    parser.add_argument('-o','--impalaPort', metavar='impalaPort', type=str, help='')
    args = parser.parse_args()

    options = {
        'schema_exadata': args.schemaExadata, 
        'schema_exadata_aux': args.schemaExadataAux,
        'impala_host' : args.impalaHost,
        'impala_port' : args.impalaPort
    }

    execute_process(options)