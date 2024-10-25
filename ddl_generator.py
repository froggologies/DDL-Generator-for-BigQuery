import csv
import argparse
from datetime import datetime
import re


count_unknown_data_type = 0


def convert_data_type(data_type, data_length, data_precision, data_scale):
    """Converts Oracle data types to BigQuery equivalents.
       Handles data_length, data_precision, and data_scale where applicable.

    Args:
        data_type: Oracle data type (e.g., 'integer', 'varchar(50)', 'numeric(10,2)').
        data_length: Length of the data type (if applicable).
        data_precision: Precision of the data type (if applicable).
        data_scale: Scale of the data type (if applicable).

    Returns:
        A string representing the equivalent BigQuery data type, or None if no direct mapping is found.
    """

    global count_unknown_data_type

    type_mapping = {
        # Oracle:
        "BLOB": "BYTES",
        "CHAR": "STRING",
        "CLOB": "STRING",
        "DATE": "DATE",
        "NUMBER": "NUMERIC",
        "RAW": "BYTES",
        "TIMESTAMP": "TIMESTAMP",
        "VARCHAR2": "STRING",
        "FLOAT": "FLOAT64",
        # PostgreSQL:
        "CHARACTER VARYING": "STRING",
        "CHARACTER": "STRING",
        "DATE": "DATE",
        "NUMERIC": "NUMERIC",
        "TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP",
    }

    data_type = re.sub(r"\(.*?\)", "", data_type).strip().upper()

    bq_type = type_mapping.get(data_type)

    # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#parameterized_data_types

    # Handle specific cases
    if bq_type == "NUMERIC":
        if data_precision != "" and data_scale != "":
            if int(data_precision) >= 30 or int(data_scale) >= 10:
                bq_type = f"BIGNUMERIC({data_precision}, {data_scale})"
            else:
                bq_type = f"NUMERIC({data_precision}, {data_scale})"
        elif data_precision != "":
            bq_type = f"NUMERIC({data_precision}) -- Default scale"
    elif bq_type == "STRING" and data_length != "":
        bq_type = f"STRING({data_length})"
    elif bq_type is None:
        count_unknown_data_type += 1
        bq_type = f"STRING"
        print(f"Unknown data type: {data_type}")

    return bq_type


def generate_ddl(csv_file):
    """Generates BigQuery DDL from a CSV file."""
    ddl = ""
    count_table = 0
    count_total_column = 0

    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        current_table = ""
        count_table_column = 0
        for row in reader:
            table_name = row["BQ_ODS"]
            if table_name != current_table:
                if current_table:
                    ddl += f"); -- Column: {str(count_table_column)}\n\n"  # Close previous table definition
                    count_table_column = 0
                ddl += f"CREATE OR REPLACE TABLE `{table_name}` (\n"
                current_table = table_name
                count_table += 1
            column_name = row["COLUMN_NAME"].replace("$", "")
            data_type = convert_data_type(
                row["DATA_TYPE"],
                row["DATA_LENGTH"],
                row["DATA_PRECISION"],
                row["DATA_SCALE"],
            )
            ddl += f"  `{column_name}` {data_type}"

            padding = 40 - len(data_type) - len(column_name) - 3

            if row["NULLABLE"] == "false":
                ddl += f" NOT NULL"
                padding -= 9

            ddl += (
                f", {" " * padding}"
                f"-- {row['DATA_TYPE']} {int(row['DATA_LENGTH']) if row['DATA_LENGTH'] else 'N'}-"
                f"{int(row['DATA_PRECISION']) if row['DATA_PRECISION'] else 'N'}-"
                f"{int(row['DATA_SCALE']) if row['DATA_SCALE'] else 'N'}\n"
            )
            count_total_column += 1
            count_table_column += 1
        ddl += f"); -- Column: {str(count_table_column)}"  # Close the last table definition
    iso_date = datetime.now().isoformat()
    ddl_info = (
        f"/*\n"
        f"Generated at: {iso_date}\n"
        f"Total table: {str(count_table)}\n"
        f"Total column: {str(count_total_column)}\n"
        f"Total unknown data type: {str(count_unknown_data_type)}\n"
        f"comment: -- DATA_TYPE DATA_LENGTH-DATA_PRECISION-DATA_SCALE\n"
        f"*/\n\n"
    )
    ddl = ddl_info + ddl
    return ddl


if __name__ == "__main__":

    formatted_date = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    parser = argparse.ArgumentParser(
        description="Generate BigQuery DDL from a CSV file."
    )
    parser.add_argument("csv_file", help="Path to the CSV file.")

    args = parser.parse_args()

    # Generate the DDL file
    ddl_output = generate_ddl(args.csv_file)

    # Save the DDL to a file
    output_filename = f"{args.csv_file.split('.')[0]}_{formatted_date}.sql"
    with open(output_filename, "w") as outfile:
        outfile.write(ddl_output)
    print(f"DDL saved to: {output_filename}")
