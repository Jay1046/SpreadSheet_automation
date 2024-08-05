import psycopg2 as pgsql

class DBConnector:

    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.conn = None

        try:
            self.enter_connect = self._postgre_connect

        except Exception as e:
            raise print("Error occured : ", e)

    def __enter__(self):
        self.enter_connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try :
            self.conn.close()
        except:
            pass

    def _postgre_connect(self):
        self.conn = pgsql.connect(
            host = self.host,
            port = self.port, 
            user = self.user, 
            password = self.password, 
            dbname = self.database
        )