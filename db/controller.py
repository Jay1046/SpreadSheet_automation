from sqlalchemy import create_engine
import pandas as pd

def rdb_pandas_extractor(db_connector, query):
    
    engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'\
        .format(\
            user = db_connector.user
            , password = db_connector.password
            , host = db_connector.host
            , port = db_connector.port
            , database = db_connector.database)
            )
    
    result_df = pd.read_sql(
        sql = query,
        con = engine,
    )

    engine.dispose()
    
    return result_df



def rdb_cursor_extractor(db_connector, _query, dataframe=False):
    with db_connector as connected:
        cur = connected.conn.cursor()
        cur.execute(_query)
        result = cur.fetchall()

        if dataframe == True:
            result = pd.DataFrame(result)

    
    return result