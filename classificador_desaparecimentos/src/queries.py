# -*- coding: utf-8 -*-
import pandas as pd


TRAIN_QUERY = """
    SELECT DISTINCT B.SNCA_DK, B.SNCA_DS_FATO, D.DMDE_MDEC_DK
    FROM SILD.SILD_ATIVIDADE_SINDICANCIA A
    INNER JOIN SILD.SILD_SINDICANCIA B
        ON A.ATSD_SNCA_DK = B.SNCA_DK
    INNER JOIN SILD.SILD_DESAPARE_MOT_DECLARADO D
        ON D.DMDE_SDES_DK = B.SNCA_DK
    WHERE A.ATSD_TPSN_DK = 22
"""

POSSIBLE_CLASSES_QUERY = """
    SELECT DISTINCT DMDE_MDEC_DK
    FROM SILD.SILD_DESAPARE_MOT_DECLARADO
    ORDER BY DMDE_MDEC_DK ASC
"""

PREDICT_QUERY = """
    SELECT DISTINCT B.SNCA_DK, B.SNCA_DS_FATO, D.DMDE_MDEC_DK
    FROM SILD.SILD_ATIVIDADE_SINDICANCIA A
    INNER JOIN SILD.SILD_SINDICANCIA B
        ON A.ATSD_SNCA_DK = B.SNCA_DK
    INNER JOIN SILD.SILD_DESAPARE_MOT_DECLARADO D
        ON D.DMDE_SDES_DK = B.SNCA_DK
    WHERE A.ATSD_TPSN_DK = 2 AND D.DMDE_MDEC_DK = 13
    AND NOT EXISTS (
        SELECT ATSD_SNCA_DK
        FROM SILD.SILD_ATIVIDADE_SINDICANCIA B
        WHERE B.ATSD_SNCA_DK = A.ATSD_SNCA_DK
        AND (B.ATSD_TPSN_DK = 22 OR B.ATSD_TPSN_DK = 23))
"""

EVALUATE_QUERY = """
    SELECT DISTINCT A.DMDE_SDES_DK AS SNCA_DK, A.DMDE_MDEC_DK AS MDEC_DK
    FROM SILD.SILD_DESAPARE_MOT_DECLARADO A
    INNER JOIN SILD.SILD_ATIVIDADE_SINDICANCIA D
        ON A.DMDE_SDES_DK = D.ATSD_SNCA_DK
    WHERE D.ATSD_TPSN_DK = 22
"""

SET_MODULE_QUERY = ("CALL dbms_application_info.set_module("
                    "'SILD', 'Funcionalidade')")
SET_CLIENT_QUERY = "CALL dbms_application_info.set_client_info(?)"

ATIV_SINDICANCIA_QUERY = """
    INSERT INTO SILD.SILD_ATIVIDADE_SINDICANCIA
    (ATSD_DK, ATSD_SNCA_DK, ATSD_TPSN_DK, ATSD_DT_REGISTRO,
    ATSD_DS_MOTIVO_ATIVIDADE, ATSD_NM_RESP_CTRL, ATSD_CPF_RESP_CTRL)
    VALUES (?, ?, 23, SYSDATE, 'CLASSIFICACAO FEITA PELO ROBO', ?, ?)
"""

DELETE_MOT_DECLARADO_QUERY = """
    DELETE FROM SILD.SILD_DESAPARE_MOT_DECLARADO WHERE DMDE_SDES_DK = ?
"""

INSERT_MOT_DECLARADO_QUERY = """
    INSERT INTO SILD.SILD_DESAPARE_MOT_DECLARADO
    (DMDE_SDES_DK, DMDE_MDEC_DK)
    VALUES (?, ?)
"""


def get_train_data(cursor, UFED_DK=None, start_date=None):
    """Get the data that will be used to train the model.

    Parameters:
        cursor: The jdbc cursor to execute the queries.
        UFED_DK: The dk corresponding to the state to get data from.
        start_date: The date after which the data should be retrieved.

    Returns:
        A Pandas DataFrame containing the training data.
    """
    if UFED_DK is not None:
        try:
            int(UFED_DK)
        except ValueError:
            raise TypeError('UFED_DK must be None or integer!')

    query = TRAIN_QUERY

    if UFED_DK:
        query += " AND B.SNCA_UFED_DK = {}".format(UFED_DK)
    if start_date:
        query += " AND A.ATSD_DT_REGISTRO >= TO_DATE('{}', 'YYYY-MM-DD')"\
            .format(start_date)

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


def get_predict_data(cursor, UFED_DK=None, start_date=None):
    """Get the data that will be used for the predictions.

    Parameters:
        cursor: The jdbc cursor to execute the queries.
        UFED_DK: The dk corresponding to the state to get data from.
        start_date: The date after which the data should be retrieved.

    Returns:
        A Pandas DataFrame containing the data to predict labels for.
    """
    if UFED_DK is not None:
        try:
            int(UFED_DK)
        except ValueError:
            raise TypeError('UFED_DK must be None or integer!')

    query = PREDICT_QUERY

    if UFED_DK:
        query += " AND B.SNCA_UFED_DK = {}".format(UFED_DK)
    if start_date:
        query += " AND A.ATSD_DT_REGISTRO >= TO_DATE('{}', 'YYYY-MM-DD')"\
            .format(start_date)

    cursor.execute(query)

    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    return df.astype({'SNCA_DK': int, 'DMDE_MDEC_DK': int})


def get_evaluate_data(cursor, keys):
    """Get the data that will be used to evaluate the model.

    Parameters:
        cursor: The jdbc cursor to execute the queries.
        keys: The keys to use for evaluation.

    Returns:
        A Pandas DataFrame containing the evaluation data.
    """
    cursor.execute(EVALUATE_QUERY)

    columns = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(cursor.fetchall(), columns=columns)
    result = result.astype({'SNCA_DK': int, 'MDEC_DK': int})

    return result[result['SNCA_DK'].isin(keys)]


def set_module_and_client(cursor, client_name):
    """Sets the module and client info on the database.

    Parameters:
        cursor: The jdbc cursor to execute the queries.
        client_name: The client name to set the client info to.
    """
    cursor.execute(SET_MODULE_QUERY)
    cursor.execute(SET_CLIENT_QUERY, (client_name,))


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


def update_atividade_sindicancia(cursor, ativ_dk, snca_dk,
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
                   (int(ativ_dk), int(snca_dk), user_name, user_number))


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