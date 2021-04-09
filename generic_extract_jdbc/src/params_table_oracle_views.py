params = {
    "driver": "oracle.jdbc.driver.OracleDriver",
    "tables": [
        # schema ORGI
        {
            "table_jdbc": "ORGI.ORGI_VW_ORGAO_GT",
            "pk_table_jdbc": "",
            "update_date_table_jdbc": "",
            "no_lower_upper_bound": "true",
            "table_hive": "orgi_vw_orgao_gt"
        },
        # schema ASIN
        {
            "table_jdbc": "ASIN.AX_V_REQUISICAO_CONSULTA",
            "pk_table_jdbc": "",
            "update_date_table_jdbc": "",
            "no_lower_upper_bound": "true",
            "table_hive": "asin_ax_v_requisicao_consulta"
        },
        {
            "table_jdbc": "ASIN.AX_SITUACAO_REQ",
            "pk_table_jdbc": "cd_situacao_req",
            "update_date_table_jdbc": "",
            "no_lower_upper_bound": "true",
            "table_hive": "asin_ax_situacao_req"
        },
        {
            "table_jdbc": "ASIN.CR_UA",
            "pk_table_jdbc": "",
            "update_date_table_jdbc": "",
            "no_lower_upper_bound": "true",
            "table_hive": "asin_cr_ua"
        },
        {
            "table_jdbc": "ASIN.CR_BEM_SERVICO",
            "pk_table_jdbc": "cd_bem_servico",
            "update_date_table_jdbc": "",
            "no_lower_upper_bound": "true",
            "table_hive": "asin_cr_bem_servico"
        },
        {
            "table_jdbc": "ASIN.CR_BEM_GENERICO",
            "pk_table_jdbc": "cd_bem_generico",
            "update_date_table_jdbc": "",
            "no_lower_upper_bound": "true",
            "table_hive": "asin_cr_bem_generico"
        },
        {
            "table_jdbc": "ASIN.CR_UM",
            "pk_table_jdbc": "cd_um",
            "update_date_table_jdbc": "",
            "no_lower_upper_bound": "true",
            "table_hive": "asin_cr_um"
        }
    ]
}