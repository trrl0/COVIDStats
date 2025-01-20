import subprocess
import mysql.connector as sql
import time
import pandas as pd
from scipy import stats
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


def plot_data(dataframe, dep_var, indep_var):


    combined_frame = pd.DataFrame({
        entry: dataframe[dataframe[dep_var] == entry][indep_var].value_counts()
        for entry in dataframe[dep_var].unique() if entry is not None
    }).fillna(0) # Fill NaN with 0 for categories not present in one group

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


def analyze_stats(dataframe):

    test_vars = ["age_group", "sex", "nationality"]
    results = {
        test_var: stats.chi2_contingency(
            dataframe.pivot_table(
                index=test_var,
                columns="infection_status",
                values="count",
                aggfunc="sum",
                fill_value=0
            )
        ) for test_var in test_vars
    }

    results_table = pd.DataFrame({
        "Characteristic": test_var,
        "Chi2": results[test_var][0],
        "p": results[test_var][1]
    } for test_var in test_vars)

    print("Results for category")
    print(results_table)


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
    case_data = db.query(f"""
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
        ORDER BY
            age_group,
            sex,
            nationality,
            infection_status     
    ;""")

    analyze_stats(case_data)

    plot_data(
        dataframe=case_data,
        dep_var="sex",
        indep_var="infection_status"
    )

    db.close()


if __name__ == "__main__":
    main()
