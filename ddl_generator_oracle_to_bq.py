import csv
import argparse
from datetime import datetime


def convert_data_type(data_type, data_length, data_precision, data_scale):
    """Converts data types from Oracle to BigQuery."""
    data_type = data_type.upper()
    if data_type == "VARCHAR2":
        return f"STRING({data_length})"
    elif data_type == "NUMBER":
        if not data_precision:
            data_precision = "1"
        if not data_scale:
            data_scale = "0"
        if int(data_precision) >= 30 or int(data_scale) >= 10:
            return f"BIGNUMERIC({data_precision}, {data_scale})"
        return f"NUMERIC({data_precision}, {data_scale})"
    elif data_type == "FLOAT":
        return f"FLOAT({data_precision}, {data_scale})"
    elif data_type == "CHAR":
        return f"STRING({data_length})"
    elif data_type == "DATE":
        return "DATE"
    elif data_type == "BLOB":
        return "BYTES"
    elif data_type == "CLOB":
        return "STRING"
    elif data_type == "RAW":
        return "BYTES"
    elif data_type == "TIMESTAMP(6)":
        return "TIMESTAMP"
    else:
        return f"STRING -- Type unknown"  # Default to STRING for unknown types


def generate_ddl(csv_file, dataset_name, project_id):
    """Generates BigQuery DDL from a CSV file."""
    ddl = ""
    count_table = 0
    count_total_column = 0
    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        current_table = ""
        count_table_column = 0
        for row in reader:
            table_name = row["TABLE_NAME"].replace("$", "").lower()
            if table_name != current_table:
                if current_table:
                    ddl += f"); -- Column: {str(count_table_column)}\n\n"  # Close previous table definition
                    count_table_column = 0
                if dataset_name:
                    if project_id:
                        ddl += f"CREATE OR REPLACE TABLE `{project_id}.{dataset_name}.{table_name}` (\n"
                    else:
                        ddl += (
                            f"CREATE OR REPLACE TABLE `{dataset_name}.{table_name}` (\n"
                        )
                else:
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
            if row["NULLABLE"] == "N":
                nullable = "NOT NULL"
                ddl += f"  `{column_name}` {data_type} {nullable},\n"
            else:
                ddl += f"  `{column_name}` {data_type},\n"
            count_total_column += 1
            count_table_column += 1
        ddl += f"); -- Column: {str(count_table_column)}"  # Close the last table definition
    ddl_info = f"/*\nTotal table: {str(count_table)}\nTotal column: {str(count_total_column)}\n*/\n\n"
    ddl = ddl_info + ddl
    return ddl


if __name__ == "__main__":

    # Format the current date and time as "YYYY-MM-DD-HH-MM-SS"
    formatted_date = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    parser = argparse.ArgumentParser(
        description="Generate BigQuery DDL from a CSV file."
    )
    parser.add_argument("csv_file", help="Path to the CSV file.")
    parser.add_argument("--dataset_name", help="Name of the BigQuery dataset to use.")
    parser.add_argument("--project_id", help="Project ID to use.")
    args = parser.parse_args()

    # Generate the DDL file
    ddl_output = generate_ddl(args.csv_file, args.dataset_name, args.project_id)

    # Save the DDL to a file
    output_filename = f"{args.csv_file.split('.')[0]}_ddl-{formatted_date}.sql"
    with open(output_filename, "w") as outfile:
        outfile.write(ddl_output)
    print(f"DDL saved to: {output_filename}")
