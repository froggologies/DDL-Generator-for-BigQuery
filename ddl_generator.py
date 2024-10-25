import csv
import argparse
from datetime import datetime
import re
import logging
from collections import Counter

# Use a logger for better control and output destination
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Global variable tracking unknown data types
count_unknown_data_type = 0


def convert_data_type(data_type, data_length, data_precision, data_scale):
    """
    Converts Oracle/PostgreSQL data type to BigQuery data type.

    Args:
        data_type (str): Oracle/PostgreSQL data type.
        data_length (str): Length of the data type.
        data_precision (str): Precision of the data type.
        data_scale (str): Scale of the data type.

    Returns:
        str: BigQuery data type.

    Raises:
        None
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
        "TEXT": "STRING",
        "BIGINT": "NUMERIC",
        "NUMERIC": "NUMERIC",
        "INTEGER": "NUMERIC",
        "DATE": "DATE",
        "TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP",
        "TIMESTAMP WITH TIME ZONE": "TIMESTAMP",
    }

    data_type = re.sub(r"\(.*?\)", "", data_type).strip().upper()

    bq_type = type_mapping.get(data_type)

    # https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#parameterized_data_types

    # Handle specific cases
    if bq_type == "NUMERIC":
        if data_precision != "" and data_scale != "":
            if int(data_precision) <= 38 and int(data_scale) <= 9:
                bq_type = f"NUMERIC({data_precision}, {data_scale})"
            elif int(data_precision) <= 76 or int(data_scale) <= 38:
                bq_type = f"BIGNUMERIC({data_precision}, {data_scale})"
            else:
                bq_type = f"FLOAT64"
                logging.warning(
                    f"Data type: {data_type}, precision: {data_precision}, scale: {data_scale}, using FLOAT64."
                )
        elif data_precision != "":
            bq_type = f"NUMERIC({data_precision})"
    elif bq_type == "STRING" and data_length != "":
        bq_type = f"STRING({data_length})"
    elif bq_type is None:
        count_unknown_data_type += 1
        bq_type = f"STRING"
        logging.warning(f"Unknown data type: {data_type}, using STRING as default.")

    return bq_type


def generate_ddl(csv_file):
    """
    Generate a BigQuery DDL from a CSV file.

    Args:
        csv_file (str): Path to the CSV file.

    Returns:
        str: BigQuery DDL.

    Raises:
        None

    Notes:
        The CSV file must contain the following columns:
            - BQ_ODS: The name of the BigQuery table.
            - COLUMN_NAME: The name of the column.
            - DATA_TYPE: The data type of the column in Oracle/PostgreSQL format.
            - DATA_LENGTH: The length of the data type.
            - DATA_PRECISION: The precision of the data type.
            - DATA_SCALE: The scale of the data type.
            - NULLABLE: Whether the column is nullable.
    """

    ddl = ""
    count_table = 0
    count_total_column = 0
    data_type_counts = Counter()
    bq_ods_counts = Counter()

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
                bq_ods_counts[row["BQ_ODS"]] += 1
                current_table = table_name
                count_table += 1

            column_name = row["COLUMN_NAME"].replace("$", "")
            data_type = convert_data_type(
                row["DATA_TYPE"],
                row["DATA_LENGTH"],
                row["DATA_PRECISION"],
                row["DATA_SCALE"],
            )

            data_type_counts[row["DATA_TYPE"]] += 1

            ddl += f"  `{column_name}` {data_type}"

            padding = 40 - len(f"  `{column_name}` {data_type}") - 1

            if row["NULLABLE"] == "false":
                ddl += f" NOT NULL"
                padding -= 9

            ddl += (
                f", {" " * padding}"
                f"-- {row['DATA_TYPE']} "
                f"{row['DATA_LENGTH']}-"
                f"{row['DATA_PRECISION']}-"
                f"{row['DATA_SCALE']}\n"
            )
            count_total_column += 1
            count_table_column += 1
        ddl += f"); -- Column: {str(count_table_column)}"  # Close the last table definition

    # Identify and format BQ_ODS counts that are more than 1
    duplicated_tables = "\n".join(
        [f"- {bq_ods}: {count}" for bq_ods, count in bq_ods_counts.items() if count > 1]
    )

    # Format data type counts for ddl_info
    data_type_info = "\n".join(
        [f"- {data_type}: {count}" for data_type, count in data_type_counts.items()]
    )

    ddl_info = (
        f"/*\n"
        f"Generated at: {datetime.now().isoformat()}\n"
        f"Total table: {str(count_table)}\n"
        f"Total column: {str(count_total_column)}\n"
        f"Total unknown data type: {str(count_unknown_data_type)}\n\n"
        f"Duplicated BQ_ODS:"
        f"{" None" if not duplicated_tables else "\n" + duplicated_tables}\n\n"
        f"Data type counts:\n"
        f"{data_type_info}\n"
        f"*/\n\n"
    )

    return ddl_info + ddl


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

    logging.info(f"DDL saved to: {output_filename}")
