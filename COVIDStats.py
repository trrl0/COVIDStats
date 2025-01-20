import subprocess
import mysql.connector as sql
import time
import pandas as pd
from scipy.stats import chi2_contingency
import matplotlib.pyplot as plt



class SQLDatabase:
    def __init__(self, path, database_name):

        self.path = path
        self.database_name = database_name
        self.process = None
        self.connection = None

        self.connect_database()

    def connect_database(self):

        # Start Dolt SQL server
        try:
            self.process = subprocess.Popen(["dolt", "sql-server"], cwd=self.path)
            time.sleep(0.5)  # Pause for server startup

            self.connection = sql.connect(
                host="localhost",
                port=3306,         # Default MySQL port
                user="root",       # Default Dolt user
                password="",       # Default password is empty
                database=self.database_name    # Default database name
            )

        except Exception as error:
            print(f"Connection error: {error}")

    def query(self, query_str):
        return pd.read_sql(query_str, self.connection)

    def close(self):
        self.connection.close()

        # Stop the Dolt server process
        self.process.terminate()


def test_variable(test_data):

    data_columns = test_data.columns.tolist()
    dep_var = data_columns[0]
    indep_var = data_columns[1]
    table = pd.crosstab(test_data[dep_var], test_data[indep_var])

    chi2, p, dof, expected = chi2_contingency(table)
    print(f"Chi-Square test p-value[{dep_var} to {indep_var}]: {p:.2e}")


def main():

    path = "./dolt-repo"
    database_name = "corona-virus"
    table_name = "case_details"

    database = SQLDatabase(
        path=path,
        database_name=database_name
    )

    test_vars = ("sex", "current_status")

    query_str = f"SELECT {', '.join(test_vars)} FROM {table_name}"
    test_data = database.query(query_str)

    test_variable(test_data)

    database.close()


if __name__ == "__main__":
    main()
