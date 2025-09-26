import psycopg2
import sqlite3


class DBConnector:
    def __init__(
        self, db_type, host=None, db_name=None, user=None, pwd=None, port=None
    ):
        self.db_type = db_type.lower()
        self.host = host
        self.db_name = db_name
        self.port = port
        self.user = user
        self.pwd = pwd
        self.connection = None
        self.cursor = None

    def open_session(self):
        try:
            if self.db_type == "postgresql":
                self.connection = psycopg2.connect(
                    dbname=self.db_name,
                    host=self.host,
                    user=self.user,
                    password=self.pwd,
                    port=self.port or 5432,
                )
            elif self.db_type == "sqlite":
                self.connection = sqlite3.connect(self.db_name)
            else:
                raise ValueError(f"Unsupported DB type: {self.db_type}")

            return self.return_operation("Connection successful")
        except Exception as e:
            return self.return_operation(f"Connection failed: {e}", 404)

    def start_cursor(self):
        self.cursor = self.connection.cursor()

    def commit(self):
        try:
            self.connection.commit()
            self.connection.close()
            return self.return_operation("Commit successful")
        except Exception:
            self.connection.rollback()
            self.connection.close()
            return self.return_operation("Cannot commit operation, rolling back", 500)

    def return_operation(self, result_description, result_code=200, data=[]):
        return {"result": result_description, "result_code": result_code, "data": data}

    def execute_read(self, query):
        try:
            self.cursor.execute(query)
            data = self.cursor.fetchall()
            description = self.cursor.description
            self.connection.close()
            return {
                "read_data": data,
                "read_description_data": description,
                "status_code": 200,
                "exception": "",
            }
        except Exception as e:
            return {
                "read_data": [],
                "read_description_data": [],
                "status_code": 500,
                "exception": str(e),
            }

    def read_sql_table(
        self, table_name: str, columns=["*"], schema=None, where_clause=""
    ) -> dict:
        self.open_session()
        self.start_cursor()

        if self.db_type == "postgresql" and schema:
            select_query = f"SELECT {', '.join(columns)} FROM {schema}.{table_name}"
        else:
            select_query = f"SELECT {', '.join(columns)} FROM {table_name}"

        if where_clause:
            if where_clause.strip().upper().startswith("WHERE"):
                query = select_query + " " + where_clause
            else:
                query = select_query + " WHERE " + where_clause
        else:
            query = select_query

        read_result = self.execute_read(query)

        if read_result["status_code"] == 200:
            return self.return_operation("Reading successful", data=read_result)
        else:
            return self.return_operation(
                read_result["exception"], result_code=read_result["status_code"]
            )
