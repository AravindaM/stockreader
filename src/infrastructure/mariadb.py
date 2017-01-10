from mysql.connector.pooling import MySQLConnectionPool

class MariaDB:

    def __init__(self, mariadb_config):
        self.mariadb_config = mariadb_config
        pool_name = mariadb_config["pool_name"]
        pool_size = mariadb_config["pool_size"]
        username = mariadb_config["username"]
        password = mariadb_config["password"]
        database = mariadb_config["database"]
        self.pool = MySQLConnectionPool(pool_name=pool_name, pool_size=pool_size,
                                          user=username, password=password,
                                          # database=database)
                                          database=database, autocommit=True)

    def get_connection(self):
        return self.pool.get_connection()

    def close(self, cursor, connection):
        if cursor:
            cursor.close()
        if connection:
            connection.close()
