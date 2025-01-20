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

    print(test_data)

    #plot_data(dep_var, indep_var, combined_frame)


def plot_data(dep_var, indep_var, combined_frame):
    combined_frame.plot(
        kind="bar",
        figsize=(8, 6),
        width=0.8
    )

    indep_var_label = convert_label(indep_var)
    dep_var_label = convert_label(dep_var)

    plt.title(f"{indep_var_label} by {dep_var_label}")
    plt.xlabel(indep_var_label)
    plt.ylabel("Count")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def convert_label(text_str):
    return ' '.join(text_str.split("_")).title()


def main():

    path = "./dolt-repo"
    database_name = "corona-virus"
    table_name = "case_details"

    db = SQLDatabase(
        path=path,
        database_name=database_name
    )

    test_vars = ("sex", "current_status")

    # Retrieves data for M and F sexes, groups and counts entries, and orders them
    sex_data = db.query(f"""
        SELECT
            age,
                CASE
                    WHEN age BETWEEN 0 AND 17 THEN '0-17'
                    WHEN age BETWEEN 18 AND 25 THEN '18-25'
                    WHEN age BETWEEN 26 AND 35 THEN '26-35'
                    WHEN age BETWEEN 36 AND 50 THEN '36-50'
                    WHEN age BETWEEN 51 AND 65 THEN '51-65'
                    WHEN age > 65 THEN '65+'
                    ELSE 'unknown'
                END AS age_group,
            sex,
            nationality,
            current_status,
                CASE
                    WHEN current_status IN ('admitted', 'isolated', 'quarantined', 'in hospital') THEN 'infected'
                    WHEN current_status IN ('dead', 'deceased', 'died') THEN 'deceased'
                    WHEN current_status IN ('recovered') THEN 'recovered'
                    ELSE 'unknown'
                END AS infection_status,
            COUNT(*) as count
        FROM {table_name}
        GROUP BY
            age_group,
            sex,
            nationality,
            infection_status            
    ;""")

    test_variable(sex_data)

    db.close()


if __name__ == "__main__":
    main()
