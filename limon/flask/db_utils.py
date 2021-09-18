import mysql.connector as mariadb
import pytest

__author__ = "StevenGuarino"
__version__ = "0.1"

class Database_obj():
  def __init__(self,
               host: str,
               port: int,
               user: str,
               password: str,
               database: str,
               keep_existing: bool,
               logger):
      self.host = host
      self.port = port
      self.user = user
      self.password = password
      self.database = database
      self.logger = logger
      self._mk_connection()
      # case where not using existing and something is there
      if not keep_existing and self.check_db_exists(self.database):
        self._drop_db(self.database)
        self._create_db()
      # case where nothing is there
      elif not self.check_db_exists(self.database):
        self._create_db()
      # case where something is there and we are keeping
      elif keep_existing and self.check_db_exists(self.database):
        # TODO port and ip address
        self.mariadb_connection = mariadb.connect(host=self.host,
                                                  port=self.port,
                                                  user=self.user,
                                                  passwd=self.password,
                                                  database=self.database)
  # end

  def _mk_connection(self):
    self.mariadb_connection = mariadb.connect(host=self.host,
                                              port=self.port,
                                              user=self.user,
                                              passwd=self.password)
    self.cursor = self.mariadb_connection.cursor()
  # end

  def _create_db(self):
    self.cursor.execute("CREATE DATABASE {}".format(self.database))
    self.mariadb_connection = mariadb.connect(host=self.host,
                                              port=self.port,
                                              user=self.user,
                                              passwd=self.password,
                                              database=self.database)
    self.cursor = self.mariadb_connection.cursor()
    self.logger.info("creating database: {}".format(self.database))
  # end

  def create_table(self,
                   table_name: str,
                   cols: str):
    cols =  "id int NOT NULL AUTO_INCREMENT, "+cols+", PRIMARY KEY (id)"
    self.cursor.execute("CREATE TABLE {} ({})".format(table_name,
                                                      cols))
    self.logger.info("creating table: {}".format(table_name))
  # end

  def check_db_exists(self,
                      db_name: str) -> bool:
    check = "SHOW DATABASES"
    self.cursor.execute(check)
    found = False
    for database in self.cursor:
      if database[0] == db_name:
          self.logger.info("database: {} exists".format(db_name))
          found = True
    return found
  # end

  def check_table_exists(self,
                         table_name: str) -> bool:
    check = "SHOW TABLES"
    self.cursor.execute(check)

    found = False
    for table in self.cursor:
      if table[0] == table_name:
          self.logger.info("table: {} exists".format(table_name))
          found = True
    return found
  # end

  def insert_into_table(self,
                        table_name: str,
                        cols: list,
                        vals: list):
    sql = "INSERT INTO {} ({}) VALUES ({})".format(table_name,
                                                   cols,
                                                   str("%s, "*len(vals[0]))[:-2])
    print(sql)
    self.cursor.executemany(sql, vals)
    self.mariadb_connection.commit()
    self.logger.info("{} were inserted".format(self.cursor.rowcount))
  # end

  def delete_from_table(self,
                        table_name: str,
                        delete_interval: int,
                        date_col: str):
    sql = "DELETE FROM {} WHERE {} < (NOW() - INTERVAL {} SECOND)".format(table_name,
                                                                         date_col,
                                                                         delete_interval)
    self.cursor.execute(sql)
    self.logger.info("deleted records older than: {}".format(delete_interval))
  # end

  def delete_by_id_from_table(self,
                              table_name: str,
                              col: int,
                              id_: str):
    sql = "DELETE FROM {} WHERE {}={}".format(table_name,
                                              col,
                                              id_)
    self.cursor.execute(sql)
    self.logger.info("deleted records with id: {}".format(id_))
  # end

  def select_from_table(self,
                        table_name: str,
                        cols: list):
    self.cursor.execute("SELECT {} FROM {}".format(", ".join(cols), table_name))
    return self.cursor.fetchall()
  # end

  def _drop_db(self,
              db_name: str):
    use_statement = "USE {}".format(db_name)
    show_tables_statement = "SHOW TABLES"
    drop_db_statement = "DROP DATABASE {}".format(db_name)
    self.cursor.execute(use_statement)
    self.cursor.execute(show_tables_statement)
    for table in self.cursor:
      self._drop_table(table[0])
    self.cursor.execute(drop_db_statement)
    self.logger.info("dropped database: {}".format(db_name))
  # end

  def _drop_table(self,
                 table_name: str):
    drop_tables_statement = "DROP TABLES {}".format(table_name)
    self.cursor.execute(drop_tables_statement)
    self.logger.info("dropped table: {}".format(table_name))
  # end
# end
