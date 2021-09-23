# -*- coding: utf-8 -*-
import pandas as pd

TRAIN_QUERIES = [
    # RJ
    """
        SELECT DISTINCT B.SNCA_DK, B.SNCA_DS_FATO,
        D.DMDE_MDEC_DK
        FROM SILD.SILD_ATIVIDADE_SINDICANCIA A
        INNER JOIN SILD.SILD_SINDICANCIA B
            ON A.ATSD_SNCA_DK = B.SNCA_DK
        INNER JOIN SILD.SILD_DESAPARE_MOT_DECLARADO D
            ON D.DMDE_SDES_DK = B.SNCA_DK
        WHERE (A.ATSD_TPSN_DK = 22 OR A.ATSD_TPSN_DK = 17)
        AND B.SNCA_DS_FATO IS NOT NULL
        AND B.SNCA_UFED_DK = 33
        AND B.SNCA_DK NOT IN (
            SELECT DISTINCT B.SNCA_DK
            FROM SILD.SILD_ATIVIDADE_SINDICANCIA A
            INNER JOIN SILD.SILD_SINDICANCIA B ON A.ATSD_SNCA_DK = B.SNCA_DK
            INNER JOIN (
                SELECT ATSD_SNCA_DK, MAX(ATSD_DT_REGISTRO) AS DT_ULTIMA_REALIZAR
                FROM SILD.SILD_ATIVIDADE_SINDICANCIA
                WHERE (ATSD_TPSN_DK = 22 OR ATSD_TPSN_DK = 17)
                GROUP BY ATSD_SNCA_DK
            ) C ON C.ATSD_SNCA_DK = A.ATSD_SNCA_DK
            AND A.ATSD_DT_REGISTRO > C.DT_ULTIMA_REALIZAR
            WHERE A.ATSD_CPF_RESP_CTRL = '07037032778'
            AND A.ATSD_TPSN_DK = 5)
    """,
    # GO
    """
        SELECT DISTINCT B.SNCA_DK, B.SNCA_DS_FATO,
        D.DMDE_MDEC_DK
        FROM SILD.SILD_ATIVIDADE_SINDICANCIA A
        INNER JOIN SILD.SILD_SINDICANCIA B
            ON A.ATSD_SNCA_DK = B.SNCA_DK
        INNER JOIN SILD.SILD_DESAPARE_MOT_DECLARADO D
            ON D.DMDE_SDES_DK = B.SNCA_DK
        WHERE A.ATSD_TPSN_DK = 2
        AND B.SNCA_DS_FATO IS NOT NULL
        AND B.SNCA_UFED_DK = 52
    """,
    # SP PONTUAL
    """
        SELECT DISTINCT B.SNCA_DK, B.SNCA_DS_FATO, D.DMDE_MDEC_DK
        FROM SILD.SILD_ATIVIDADE_SINDICANCIA A
        INNER JOIN SILD.SILD_SINDICANCIA B
            ON A.ATSD_SNCA_DK = B.SNCA_DK
        INNER JOIN SILD.SILD_DESAPARE_MOT_DECLARADO D
            ON D.DMDE_SDES_DK = B.SNCA_DK
        WHERE (A.ATSD_TPSN_DK = 22 OR A.ATSD_TPSN_DK = 17)
        AND B.SNCA_DS_FATO IS NOT NULL
        -- AND B.SNCA_UFED_DK = 35
        AND (B.SNCA_IDENTIFICADOR_SINALID LIKE '%2014SPDS%'
        OR B.SNCA_IDENTIFICADOR_SINALID LIKE '%2015SPDS%'
        OR B.SNCA_IDENTIFICADOR_SINALID LIKE '%2016SPDS%'
        OR B.SNCA_IDENTIFICADOR_SINALID LIKE '%2018SPDS%')
    """
]

PREDICT_QUERIES = [
    # RJ
    """
        SELECT DISTINCT B.SNCA_DK, B.SNCA_DS_FATO, D.DMDE_MDEC_DK
        FROM SILD.SILD_ATIVIDADE_SINDICANCIA A
        INNER JOIN SILD.SILD_SINDICANCIA B
            ON A.ATSD_SNCA_DK = B.SNCA_DK
        LEFT JOIN SILD.SILD_DESAPARE_MOT_DECLARADO D
            ON D.DMDE_SDES_DK = B.SNCA_DK
        WHERE A.ATSD_TPSN_DK = 2 AND B.SNCA_DS_FATO IS NOT NULL
        AND B.SNCA_UFED_DK = 33
	AND B.SNCA_IDENTIFICADOR_SINALID LIKE '%DS%'
        AND NOT EXISTS (
            SELECT ATSD_SNCA_DK
            FROM SILD.SILD_ATIVIDADE_SINDICANCIA B
            WHERE B.ATSD_SNCA_DK = A.ATSD_SNCA_DK
            AND (B.ATSD_TPSN_DK = 22 OR B.ATSD_TPSN_DK = 23 OR B.ATSD_TPSN_DK = 17))
    """,
    """
        SELECT DISTINCT B.SNCA_DK, B.SNCA_DS_FATO, D.DMDE_MDEC_DK
        FROM SILD.SILD_ATIVIDADE_SINDICANCIA A
        INNER JOIN SILD.SILD_SINDICANCIA B ON A.ATSD_SNCA_DK = B.SNCA_DK
        LEFT JOIN SILD.SILD_DESAPARE_MOT_DECLARADO D ON D.DMDE_SDES_DK = B.SNCA_DK
        WHERE A.ATSD_CPF_RESP_CTRL = '07037032778'
        AND A.ATSD_TPSN_DK = 5
        AND B.SNCA_DS_FATO IS NOT NULL
        AND B.SNCA_UFED_DK = 33
	AND B.SNCA_IDENTIFICADOR_SINALID LIKE '%DS%'
        AND A.ATSD_SNCA_DK NOT IN (
            SELECT ATSD_SNCA_DK
            FROM SILD.SILD_ATIVIDADE_SINDICANCIA
            WHERE ATSD_TPSN_DK = 23
        )
        AND A.ATSD_SNCA_DK NOT IN (
            SELECT DISTINCT A.ATSD_SNCA_DK
            FROM SILD.SILD_ATIVIDADE_SINDICANCIA A
            INNER JOIN (
                SELECT ATSD_SNCA_DK, MAX(ATSD_DT_REGISTRO) AS DT_ULTIMA_REALIZAR
                FROM SILD.SILD_ATIVIDADE_SINDICANCIA
                WHERE (ATSD_TPSN_DK = 22 OR ATSD_TPSN_DK = 17)
                GROUP BY ATSD_SNCA_DK) C
            ON C.ATSD_SNCA_DK = A.ATSD_SNCA_DK
            AND A.ATSD_DT_REGISTRO < C.DT_ULTIMA_REALIZAR
            WHERE A.ATSD_CPF_RESP_CTRL = '07037032778'
            AND A.ATSD_TPSN_DK = 5
        )
    """,
    # GO
    """
        SELECT DISTINCT B.SNCA_DK, B.SNCA_DS_FATO, D.DMDE_MDEC_DK
        FROM SILD.SILD_ATIVIDADE_SINDICANCIA A
        INNER JOIN SILD.SILD_SINDICANCIA B
            ON A.ATSD_SNCA_DK = B.SNCA_DK
        LEFT JOIN SILD.SILD_DESAPARE_MOT_DECLARADO D
            ON D.DMDE_SDES_DK = B.SNCA_DK
        WHERE A.ATSD_TPSN_DK = 1 AND B.SNCA_DS_FATO IS NOT NULL
        AND B.SNCA_UFED_DK = 52
        AND B.SNCA_IDENTIFICADOR_SINALID LIKE '%DS%'
        AND NOT EXISTS (
            SELECT ATSD_SNCA_DK
            FROM SILD.SILD_ATIVIDADE_SINDICANCIA B
            WHERE B.ATSD_SNCA_DK = A.ATSD_SNCA_DK
            AND (B.ATSD_TPSN_DK = 2 OR B.ATSD_TPSN_DK = 23))
    """,
    # SP PONTUAL
    # """
    #     SELECT DISTINCT B.SNCA_DK, B.SNCA_DS_FATO, D.DMDE_MDEC_DK
    #     FROM SILD.SILD_ATIVIDADE_SINDICANCIA A
    #     INNER JOIN SILD.SILD_SINDICANCIA B
    #         ON A.ATSD_SNCA_DK = B.SNCA_DK
    #     LEFT JOIN SILD.SILD_DESAPARE_MOT_DECLARADO D
    #         ON D.DMDE_SDES_DK = B.SNCA_DK
    #     WHERE (B.SNCA_IDENTIFICADOR_SINALID LIKE '%2014SPDS%'
    #     OR B.SNCA_IDENTIFICADOR_SINALID LIKE '%2015SPDS%'
    #     OR B.SNCA_IDENTIFICADOR_SINALID LIKE '%2016SPDS%'
    #     OR B.SNCA_IDENTIFICADOR_SINALID LIKE '%2018SPDS%')
    #     AND D.DMDE_MDEC_DK = 13
    # """
]

POSSIBLE_CLASSES_QUERY = """
    SELECT DISTINCT DMDE_MDEC_DK
    FROM SILD.SILD_DESAPARE_MOT_DECLARADO
    ORDER BY DMDE_MDEC_DK ASC
"""

# EVALUATE_QUERY = """
#     SELECT DISTINCT A.DMDE_SDES_DK AS SNCA_DK, A.DMDE_MDEC_DK AS MDEC_DK
#     FROM SILD.SILD_DESAPARE_MOT_DECLARADO A
#     INNER JOIN SILD.SILD_ATIVIDADE_SINDICANCIA D
#         ON A.DMDE_SDES_DK = D.ATSD_SNCA_DK
#     WHERE D.ATSD_TPSN_DK = 22
# """

EVALUATE_QUERIES = [
    # RJ
    """
        SELECT B.SNCA_DK, B.SNCA_IDENTIFICADOR_SINALID, D.DMDE_MDEC_DK AS MDEC_DK,
        MAX(A.ATSD_DT_REGISTRO) AS DT_VALIDACAO
        FROM SILD.SILD_ATIVIDADE_SINDICANCIA A
        INNER JOIN SILD.SILD_SINDICANCIA B
            ON A.ATSD_SNCA_DK = B.SNCA_DK
        INNER JOIN SILD.SILD_DESAPARE_MOT_DECLARADO D
            ON D.DMDE_SDES_DK = B.SNCA_DK
        WHERE (A.ATSD_TPSN_DK = 22 OR A.ATSD_TPSN_DK = 17)
        AND B.SNCA_DS_FATO IS NOT NULL
        AND B.SNCA_UFED_DK = 33
        AND B.SNCA_DK NOT IN (
            SELECT DISTINCT B.SNCA_DK
            FROM SILD.SILD_ATIVIDADE_SINDICANCIA A
            INNER JOIN SILD.SILD_SINDICANCIA B ON A.ATSD_SNCA_DK = B.SNCA_DK
            INNER JOIN (
                SELECT ATSD_SNCA_DK, MAX(ATSD_DT_REGISTRO) AS DT_ULTIMA_REALIZAR
                FROM SILD.SILD_ATIVIDADE_SINDICANCIA
                WHERE ATSD_TPSN_DK = 22 OR ATSD_TPSN_DK = 17
                GROUP BY ATSD_SNCA_DK
            ) C ON C.ATSD_SNCA_DK = A.ATSD_SNCA_DK
            AND A.ATSD_DT_REGISTRO > C.DT_ULTIMA_REALIZAR
            WHERE A.ATSD_CPF_RESP_CTRL = '07037032778'
            AND A.ATSD_TPSN_DK = 5)
        GROUP BY B.SNCA_DK, B.SNCA_IDENTIFICADOR_SINALID, D.DMDE_MDEC_DK
    """,
    # GO
    """
        SELECT B.SNCA_DK, B.SNCA_IDENTIFICADOR_SINALID, D.DMDE_MDEC_DK AS MDEC_DK,
        MAX(A.ATSD_DT_REGISTRO) AS DT_VALIDACAO
        FROM SILD.SILD_ATIVIDADE_SINDICANCIA A
        INNER JOIN SILD.SILD_SINDICANCIA B
            ON A.ATSD_SNCA_DK = B.SNCA_DK
        INNER JOIN SILD.SILD_DESAPARE_MOT_DECLARADO D
            ON D.DMDE_SDES_DK = B.SNCA_DK
        WHERE A.ATSD_TPSN_DK = 2
        AND B.SNCA_DS_FATO IS NOT NULL
        AND B.SNCA_UFED_DK = 52
        GROUP BY B.SNCA_DK, B.SNCA_IDENTIFICADOR_SINALID, D.DMDE_MDEC_DK
    """,
    # CE
    """
        SELECT B.SNCA_DK, B.SNCA_IDENTIFICADOR_SINALID, D.DMDE_MDEC_DK AS MDEC_DK,
        MAX(A.ATSD_DT_REGISTRO) AS DT_VALIDACAO
        FROM SILD.SILD_ATIVIDADE_SINDICANCIA A
        INNER JOIN SILD.SILD_SINDICANCIA B
            ON A.ATSD_SNCA_DK = B.SNCA_DK
        INNER JOIN SILD.SILD_DESAPARE_MOT_DECLARADO D
            ON D.DMDE_SDES_DK = B.SNCA_DK
        WHERE (A.ATSD_TPSN_DK = 22 OR A.ATSD_TPSN_DK = 17)
        AND B.SNCA_DS_FATO IS NOT NULL
        AND B.SNCA_UFED_DK = 23
        GROUP BY B.SNCA_DK, B.SNCA_IDENTIFICADOR_SINALID, D.DMDE_MDEC_DK
    """,
    # SP PONTUAL
    """
        SELECT DISTINCT B.SNCA_DK, B.SNCA_DS_FATO, D.DMDE_MDEC_DK
        FROM SILD.SILD_ATIVIDADE_SINDICANCIA A
        INNER JOIN SILD.SILD_SINDICANCIA B
            ON A.ATSD_SNCA_DK = B.SNCA_DK
        INNER JOIN SILD.SILD_DESAPARE_MOT_DECLARADO D
            ON D.DMDE_SDES_DK = B.SNCA_DK
        WHERE (A.ATSD_TPSN_DK = 22 OR A.ATSD_TPSN_DK = 17)
        AND B.SNCA_DS_FATO IS NOT NULL
        -- AND B.SNCA_UFED_DK = 35
        AND (B.SNCA_IDENTIFICADOR_SINALID LIKE '%2014SPDS%'
        OR B.SNCA_IDENTIFICADOR_SINALID LIKE '%2015SPDS%'
        OR B.SNCA_IDENTIFICADOR_SINALID LIKE '%2016SPDS%'
        OR B.SNCA_IDENTIFICADOR_SINALID LIKE '%2018SPDS%')
    """
]

GET_SINALID_QUERY = """
    SELECT SNCA_DK, SNCA_IDENTIFICADOR_SINALID FROM SILD.SILD_SINDICANCIA
"""

SET_MODULE_QUERY = ("CALL dbms_application_info.set_module("
                    "'SILD', 'Funcionalidade')")
SET_CLIENT_QUERY = "CALL dbms_application_info.set_client_info(?)"

ATIV_SINDICANCIA_QUERY = """
    INSERT INTO SILD.SILD_ATIVIDADE_SINDICANCIA
    (ATSD_DK, ATSD_SNCA_DK, ATSD_TPSN_DK, ATSD_DT_REGISTRO,
    ATSD_DS_MOTIVO_ATIVIDADE, ATSD_NM_RESP_CTRL, ATSD_CPF_RESP_CTRL)
    VALUES (SILD_SQ_ATSD_DK.nextval, ?, 23, SYSDATE, 'CLASSIFICACAO FEITA PELO ROBO', ?, ?)
"""

DELETE_MOT_DECLARADO_QUERY = """
    DELETE FROM SILD.SILD_DESAPARE_MOT_DECLARADO WHERE DMDE_SDES_DK = ?
"""

INSERT_MOT_DECLARADO_QUERY = """
    INSERT INTO SILD.SILD_DESAPARE_MOT_DECLARADO
    (DMDE_SDES_DK, DMDE_MDEC_DK)
    VALUES (?, ?)
"""


def get_train_data(cursor, start_date=None, end_date=None):
    """Get the data that will be used to train the model.

    Parameters:
        cursor: The jdbc cursor to execute the queries.
        UFED_DK: The dk corresponding to the state to get data from.
        start_date: The date after which the data should be retrieved.

    Returns:
        A Pandas DataFrame containing the training data.
    """

    query = ""

    for q in TRAIN_QUERIES:
        # Additional conditions to be added at the end of the query
        if start_date:
            q += " AND A.ATSD_DT_REGISTRO >= TO_DATE('{}', 'YYYY-MM-DD')"\
                .format(start_date)
        if end_date:
            q += " AND A.ATSD_DT_REGISTRO <= TO_DATE('{}', 'YYYY-MM-DD')"\
                .format(end_date)
        if query:
            query += " UNION {}".format(q)
        else:
            query += q

    cursor.execute(query)

    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    return df.astype({'SNCA_DK': int, 'DMDE_MDEC_DK': int})


def get_list_of_classes(cursor):
    """Get the list of possible classes in the database.

    Parameters:
        cursor: The jdbc cursor to execute the queries.

    Returns:
        List containing the possible classes, in ascending order.
    """
    query = POSSIBLE_CLASSES_QUERY

    cursor.execute(query)
    return [int(x[0]) for x in cursor.fetchall()]


def get_predict_data(cursor, only_null_class=True,
                     start_date='', end_date=''):
    """Get the data that will be used for the predictions.

    Parameters:
        cursor: The jdbc cursor to execute the queries.
        UFED_DK: The dk corresponding to the state to get data from.
        start_date: The date after which the data should be retrieved.

    Returns:
        A Pandas DataFrame containing the data to predict labels for.
    """

    query = ""

    for q in PREDICT_QUERIES:
        if only_null_class:
            q += " AND D.DMDE_MDEC_DK = 13"
        if start_date:
            q += " AND A.ATSD_DT_REGISTRO >= TO_DATE('{}', 'YYYY-MM-DD')"\
                .format(start_date)
        if end_date:
            q += " AND A.ATSD_DT_REGISTRO <= TO_DATE('{}', 'YYYY-MM-DD')"\
                .format(end_date)
        if query:
            query += " UNION {}".format(q)
        else:
            query += q

    cursor.execute(query)

    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    df['DMDE_MDEC_DK'] = df['DMDE_MDEC_DK'].apply(lambda x: int(x) if not pd.isna(x) else x)
    return df.astype({'SNCA_DK': int})


def get_evaluate_data(cursor, keys):
    """Get the data that will be used to evaluate the model.

    Parameters:
        cursor: The jdbc cursor to execute the queries.
        keys: The keys to use for evaluation.

    Returns:
        A Pandas DataFrame containing the evaluation data.
    """
    query = " UNION ".join(EVALUATE_QUERIES)
    cursor.execute(query)

    columns = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(cursor.fetchall(), columns=columns)
    result['DT_ACAO'] = result['DT_VALIDACAO'].apply(
        lambda x: "{}/{}/{}".format(x[8:10], x[5:7], x[:4]))
    result = result.drop('DT_VALIDACAO', axis=1)
    result = result.astype({'SNCA_DK': int, 'MDEC_DK': int})

    return result[result['SNCA_DK'].isin(keys)]


def get_id_sinalid(cursor, keys):
    """Get the Sinalid ID for each key in keys.

    Parameters:
        cursor: The jdbc cursor to execute the queries.
        keys: The keys to search the sinalid ID for.

    Returns:
        A Pandas DataFrame contaning the keys and their sinalid IDs.
    """
    cursor.execute(GET_SINALID_QUERY)

    columns = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(cursor.fetchall(), columns=columns)
    result = result.astype({'SNCA_DK': int, 'SNCA_IDENTIFICADOR_SINALID': str})

    return result[result['SNCA_DK'].isin(keys)]


def set_module_and_client(cursor, client_name):
    """Sets the module and client info on the database.

    Parameters:
        cursor: The jdbc cursor to execute the queries.
        client_name: The client name to set the client info to.
    """
    cursor.execute(SET_MODULE_QUERY)
    cursor.execute(SET_CLIENT_QUERY, (client_name,))


# Costumava ser usada para pegar o max_dk da tabela de atividade de sindicancia
# Esse uso foi substituido pelo uso de uma sequence do banco de dados
def get_max_dk(cursor, table_name, column_name):
    """Get the max value for a given column in the table.

    Parameters:
        cursor: The jdbc cursor to execute the queries.
        table_name: The table to get the max value from.
        column_name: The column to get the max value from.

    Returns:
        An int corresponding to the max value.
    """
    cursor.execute("SELECT MAX({}) FROM {}".format(column_name, table_name))
    return int(cursor.fetchall()[0][0])


def update_atividade_sindicancia(cursor, snca_dk,
                                 user_name, user_number):
    """Updates the data in the ATIVIDADE_SINDICANCIA table.

    Parameters:
        cursor: The jdbc cursor to execute the queries.
        ativ_dk: The dk to set the new row to.
        snca_dk: The dk relative to the document being updated.
        user_name: The name of the user making the update.
        user_number: The number of the user making the update.
    """
    cursor.execute(ATIV_SINDICANCIA_QUERY,
                   (int(snca_dk), user_name, user_number))


def update_motivo_declarado(cursor, snca_dk, labels):
    """Updates the data in the DESAPARE_MOT_DECLARADO table.

    Parameters:
        cursor: The jdbc cursor to execute the queries.
        snca_dk: The dk relative to the document being updated.
        labels: The labels the given document will be set to.
    """
    cursor.execute(DELETE_MOT_DECLARADO_QUERY,
                   (int(snca_dk),))
    for label in labels:
        cursor.execute(INSERT_MOT_DECLARADO_QUERY,
                       (int(snca_dk), int(label)))


def get_motivos_declarados(cursor):
    """Gets the list of motivos_declarados in the database.

    Parameters:
        cursor: The jdbc cursor to execute the queries.
    
    Returns:
        A dict from the keys to the names.
    """
    cursor.execute("SELECT * FROM SILD.SILD_MOTIVO_DECLARADO")
    return {int(el[0]):el[1] for el in cursor.fetchall()}
