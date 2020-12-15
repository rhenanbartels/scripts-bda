params = {
    "driver": "org.postgresql.Driver",
    "tables": [
        # Schema MCPR
        {
            "table_jdbc": """
                (SELECT objectid::int, document_id::text, proc_id, proc_numero, depo_id, cod_cisp, depo_nome, depo_circ_id, 
                data_liberacao, depo_circ_nome, catg_id, catg_descricao, stts_id, stts_descricao, proc_data::timestamp
                FROM seg_pub.in_pol_procedimento) t""",
            "pk_table_jdbc": "objectid",
            "update_date_table_jdbc": "",
            "table_hive": "seg_pub_in_pol_procedimento"
        },
        {
            "table_jdbc": """
                (SELECT objectid::int, document_id::text, proc_id, proc_numero, ocor_id, end_logradouro, 
                end_numero, end_bairro, end_municipio, end_uf, endereco, tipo_delito, tipo_delito_desc, etit, agregado_isp, 
                ocor_dt_inicial::timestamp, ocor_hr_inicial, periodo, ocorcapitulacao
                FROM seg_pub.in_pol_ocorrencia) t""",
            "pk_table_jdbc": "objectid",
            "update_date_table_jdbc": "",
            "table_hive": "seg_pub_in_pol_ocorrencia"
        },
        {
            "table_jdbc": "plataforma.amb_saneamento_snis_info_indic_agua",
            "pk_table_jdbc": "objectid",
            "update_date_table_jdbc": "",
            "table_hive": "plataforma_amb_saneamento_snis_info_indic_agua"
        },
        {
            "table_jdbc": "plataforma.amb_saneamento_snis_info_indic_drenagem",
            "pk_table_jdbc": "objectid",
            "update_date_table_jdbc": "",
            "table_hive": "plataforma_amb_saneamento_snis_info_indic_drenagem"
        },
        {
            "table_jdbc": "plataforma.amb_saneamento_snis_info_indic_esgoto",
            "pk_table_jdbc": "objectid",
            "update_date_table_jdbc": "",
            "table_hive": "plataforma_amb_saneamento_snis_info_indic_esgoto"
        },
        {
            "table_jdbc": "institucional.orgaos_meio_ambiente",
            "pk_table_jdbc": "id",
            "update_data_table_jdbc": "",
            "table_hive": "institucional_orgaos_meio_ambiente"
        },
        {
            "table_jdbc": "meio_ambiente.amb_saneamento_snis_drenagem_info_indic_2018",
            "pk_table_jdbc": "cod_mun",
            "update_data_table_jdbc": "",
            "table_hive": "meio_ambiente_amb_saneamento_snis_drenagem_info_indic_2018"
        }
    ]
}
