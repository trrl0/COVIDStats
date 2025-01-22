import subprocess
import mysql.connector as sql
import time
import pandas as pd
from pandasgui import show as pdshow
from scipy import stats
import matplotlib.pyplot as plt
import seaborn as sns


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

            print(f"Dolt server connected successfully")

        except Exception as error:
            print(f"Dolt server connection error: {error}")

    def query(self, query_str):

        try:
            query_result = pd.read_sql(query_str, self.connection)
            print("SQL queried successfully")
            return query_result

        except Exception as error:
            print(f"SQL query error: {error}")

    def close(self):

        try:
            self.connection.close()

            # Stop the Dolt server process
            self.process.terminate()
            print(f"Dolt server terminated successfully")

        except Exception as error:
            print(f"Dolt server connection error: {error}")


def plot_data(data, dep_var, indep_var):

    plt.figure(figsize=(10, 6))
    sns.barplot(
        data=data,
        x="Value",  # Groups (e.g., "M" and "F")
        y="Proportion",  # Height of the bars
        hue="Outcome",  # Different bars within each group
    )

    group_number = data["Outcome"].unique().shape[0]  # How many different bars will be generated, used for label pos

    # Currently does not plot the sig_mark correctly if there are not an equal number of outcome rows for each dep_var. Need to fill in rows otherwise or calculate sig_mark pos differently

    for index, row in data.iterrows():

        sig_mark = "*" if row["p-Value"] < 0.05 else ""

        plt.text(
            x=index // group_number + (index % group_number),  # Adjust x-position for each bar
            y=row["Proportion"] + 0.01,  # Slightly above the bar
            s=sig_mark,  # Format p-value
            ha='center', va='bottom', fontsize=9  # Center alignment
        )

    # Add labels and title
    plt.title(f"Proportions of Outcomes by {dep_var.title()}", fontsize=16)
    plt.xlabel(dep_var.title(), fontsize=12)
    plt.ylabel("Proportions", fontsize=12)
    plt.legend(title="Outcome")
    plt.xticks(rotation=60, ha='right', va='top')
    plt.tight_layout()

    # Show the plot
    plt.show()


def convert_label(text_str):
    return ' '.join(text_str.split("_")).title()


def analyze_stats(data, dep_var, indep_var):

    dep_uniques = data[dep_var].unique()
    indep_uniques = data[indep_var].unique()

    results = []

    for dep_unique in dep_uniques:
        for indep_unique in indep_uniques:

            # Create a contingency table to test individual values vs individual outcomes in infection_status
            table = pd.DataFrame({
                "Selected": [  # The subset of rows where value tested matches dep_unique
                    data[(data[dep_var] == dep_unique) & (data[indep_var] == indep_unique)].shape[0],
                    data[(data[dep_var] == dep_unique) & (data[indep_var] != indep_unique)].shape[0]
                ],
                "Not Selected": [  # The subset of rows where value tested does not match dep_unique
                    data[(data[dep_var] != dep_unique) & (data[indep_var] == indep_unique)].shape[0],
                    data[(data[dep_var] != dep_unique) & (data[indep_var] != indep_unique)].shape[0]
                ]
            })

            if (table == 0).any().any():  # if there is a 0 in the contingency table, chi2 will not work
                print(f"Warning: Contingency table [d:{dep_unique},i:{indep_unique}] contains zeroes and cannot be analyzed.")
            else:

                chi2, p, _, _ = stats.chi2_contingency(table)

                results.append({
                    "Column": dep_var,
                    "Value": dep_unique,
                    "Outcome": indep_unique,
                    "Chi2": chi2,
                    "p-Value": p,
                    "Proportion": table["Selected"][0] / (table["Selected"][0]+table["Selected"][1])
                })

    return pd.DataFrame(results)


def main():

    path = "./dolt-repo"
    database_name = "corona-virus"
    table_name = "case_details"

    db = SQLDatabase(
        path=path,
        database_name=database_name
    )

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
    db.close()

    dep_vars = ["age_group", "sex", "nationality"]
    indep_var = "infection_status"

    analysis_result = analyze_stats(
        data=case_data,
        dep_var=dep_vars[1],
        indep_var=indep_var
    )

    plot_data(
        data=analysis_result,
        dep_var=dep_vars[1],
        indep_var=indep_var
    )


if __name__ == "__main__":
    main()
