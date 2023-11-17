import csv
import os

class MyFunctions:
    def __init__(self, cli):
        self.cli = cli
        self.active_database = 'testdb.csv'
        self.active_table = 'testtable'
    def add_record(self, record, filename):
        if os.path.exists(filename):
            with open(filename, 'a', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow([record])
            print(f"Record '{record}' added to '{filename}'.")
        else:
            print(f"File '{filename}' does not exist.")
    def create_or_set_database(self, dbname):
        filename = f"{dbname}.csv"
        if os.path.exists(filename):
            print(f"Database '{dbname}' already exists.")
            self.active_database = filename
            print(f"Active database set to '{filename}'.")
        else:
            with open(filename, 'w', newline='') as csvfile:
                # No header row written
                pass

            print(f"Database '{dbname}' created in a new CSV file '{filename}'.")
            self.active_database = filename
            print(f"Active database set to '{filename}'.")
    def create_or_set_table(self, dbtable):
        if not self.active_database:
            print("No active database. Use '! db dbname' to set the active database.")
            return

        folder_path = f"{dbtable}"
        if os.path.exists(folder_path):
            self.active_table = folder_path
            print(f"Active database set to '{folder_path}'.")
            print(f"Table '{dbtable}' folder already exists.")
        else:
            os.makedirs(folder_path)
            print(f"Table '{dbtable}' folder created at '{folder_path}'.")
            self.active_table = folder_path
            self.add_record(folder_path, self.active_database)
            print(f"Active table set to '{folder_path}'.")
    def import_csv_into_table(self, path, dbtable, chunk_size):
        if not os.path.exists(path):
            print(f"File '{path}' does not exist.")
            return

        if not os.path.exists(dbtable):
            os.makedirs(dbtable)
            print(f"Folder '{dbtable}' created.")

        if os.path.isdir(dbtable):
            chunk_number = 0
            with open(path, 'r') as csvfile:
                csvreader = csv.reader(csvfile)

                # Copy header
                header = next(csvreader, None)

                # Load data in chunks
                chunk_rows = []
                total_size = 0

                for row in csvreader:
                    row_size = sum(len(str(cell)) for cell in row)

                    # Check if adding the row exceeds the chunk size
                    if total_size + row_size > chunk_size and chunk_rows:
                        # Process the chunk of rows
                        folder_path = os.path.join(dbtable, f"{dbtable}_chunk_{chunk_number}.csv")
                        self.process_chunk(folder_path, chunk_rows, header)

                        # Prepare for next chunk
                        chunk_number += 1
                        folder_path = os.path.join(dbtable, f"{dbtable}_chunk_{chunk_number}.csv")
                        chunk_rows = [row]
                        total_size = row_size
                    else:
                        # Add the row to the current chunk
                        chunk_rows.append(row)
                        total_size += row_size

                # Process the last chunk (if any)
                if chunk_rows:
                    folder_path = os.path.join(dbtable, f"{dbtable}_chunk_{chunk_number}.csv")
                    self.process_chunk(folder_path, chunk_rows, header, first_chunk=(chunk_number == 0))

            print(f"Data loaded into chunks within folder '{dbtable}'.")

    def process_chunk(self, chunk_path, chunk_data, header):
        with open(chunk_path, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(header)
            csvwriter.writerows(chunk_data)

    def show_active(self):
        print(f"Active Database: {self.active_database}")
        print(f"Active Table: {self.active_table}")
    def add_records_to_table(self, records, dbtable):
        def get_num_columns(table_path):
            first_chunk_path = os.path.join(table_path, f"{table_path}_chunk_1.csv")
            if os.path.exists(first_chunk_path):
                with open(first_chunk_path, 'r') as chunk_csv:
                    csvreader = csv.reader(chunk_csv)
                    header = next(csvreader, None)
                    if header:
                        return len(header)

            return 0  # Return 0 if unable to determine the number of columns

        folder_path = f"{dbtable}"
        if os.path.exists(folder_path):
            last_chunk_path = self.get_last_chunk_path(folder_path)
            if last_chunk_path:
                last_chunk_size = os.path.getsize(last_chunk_path)
                chunk_size_limit = 500

                # Check if the number of records matches the number of columns
                num_columns = get_num_columns(dbtable)
                if len(records) != num_columns:
                    print("Number of records does not match the number of columns in the table.")
                    return

                if last_chunk_size + len(','.join(records)) <= chunk_size_limit:
                    self.append_to_chunk(last_chunk_path, records)
                else:
                    new_chunk_path = self.create_new_chunk(folder_path, records)
                    print(f"Records added to a new chunk: {new_chunk_path}")
            else:
                new_chunk_path = self.create_new_chunk(folder_path, records)
                print(f"Records added to a new chunk: {new_chunk_path}")
        else:
            print(f"Table '{dbtable}' does not exist.")

    def get_last_chunk_path(self, folder_path):
        chunk_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
        if chunk_files:
            try:
                last_chunk_path = os.path.join(folder_path, max(chunk_files, key=lambda f: int(f.split('_chunk_')[1].split('.')[0])))
                return last_chunk_path
            except (ValueError, IndexError):
                # Handle the case where the numeric part couldn't be extracted or converted
                return None
        else:
            return None

    def create_new_chunk(self, folder_path, records):
        def get_header(table_path):
            first_chunk_path = os.path.join(table_path, next(os.walk(table_path))[2][0])
            if os.path.exists(first_chunk_path):
                with open(first_chunk_path, 'r') as chunk_csv:
                    csvreader = csv.reader(chunk_csv)
                    header = next(csvreader, None)
                    return header
            return []

        header = get_header(folder_path)
        chunk_number = len([f for f in os.listdir(folder_path) if f.endswith(".csv")])
        chunk_path = os.path.join(folder_path, f"{self.active_table}_chunk_{chunk_number}.csv")

        with open(chunk_path, 'w', newline='') as chunk_csv:
            csvwriter = csv.writer(chunk_csv)
            if header:
                csvwriter.writerow(header)
            csvwriter.writerow(records)


        return chunk_path

    def append_to_chunk(self, chunk_path, records):
        with open(chunk_path, 'a', newline='') as chunk_csv:
            csvwriter = csv.writer(chunk_csv)
            csvwriter.writerow(records)

        print(f"Records appended to the last chunk: {chunk_path}")
    def search_in_folder(self, column, value, dbtable=None, display_columns=None):
        if not dbtable:
            dbtable = self.active_table

        folder_path = f"{dbtable}"
        if os.path.exists(folder_path):
            matching_records = []
            found_columns = []

            for file_name in os.listdir(folder_path):
                if file_name.endswith(".csv"):
                    file_path = os.path.join(folder_path, file_name)
                    with open(file_path, 'r', newline='') as csvfile:
                        csvreader = csv.reader(csvfile)
                        header = next(csvreader, None)

                        if header:
                            if column not in header:
                                print(f"Column '{column}' does not exist in the table.")
                                return

                            if display_columns:
                                found_columns = [col for col in display_columns if col in header]
                                if not found_columns:
                                    print("None of the specified display columns exist in the table.")
                                    return
                                display_indexes = [header.index(col) for col in found_columns]
                            else:
                                display_indexes = range(len(header))

                            for row in csvreader:
                                if row[header.index(column)] == value:
                                    matching_records.append([row[i] for i in display_indexes])

            if matching_records:
                if found_columns:
                    print(f"Matching records in '{dbtable}' (Columns: {', '.join(found_columns)}):")
                else:
                    print(f"Matching records in '{dbtable}':")

                for record in matching_records:
                    print(record)
            else:
                print(f"No matching records found in '{dbtable}'.")
        else:
            print(f"Table '{dbtable}' does not exist.")
    def show_entire_table(self, dbtable):
        folder_path = f"{dbtable}"
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            for file_name in os.listdir(folder_path):
                if file_name.endswith(".csv"):
                    file_path = os.path.join(folder_path, file_name)
                    with open(file_path, 'r', newline='') as csvfile:
                        csvreader = csv.reader(csvfile)
                        for row in csvreader:
                            print(', '.join(row))
        else:
            print(f"Table '{dbtable}' does not exist or is not a directory.")
    def delete_records(self, column, value, dbtable):
        folder_path = f"{dbtable}"
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            for file_name in os.listdir(folder_path):
                if file_name.endswith(".csv"):
                    file_path = os.path.join(folder_path, file_name)
                    with open(file_path, 'r', newline='') as csvfile:
                        rows = list(csv.reader(csvfile))

                    # Check if the column exists in the table
                    header = rows[0] if rows else []
                    if column not in header:
                        print(f"Column '{column}' does not exist in '{dbtable}'.")
                        continue

                    column_index = header.index(column)
                    updated_rows = [row for row in rows if row[column_index] != value]

                    # Write the updated rows back to the CSV file
                    with open(file_path, 'w', newline='') as csvfile:
                        csvwriter = csv.writer(csvfile)
                        csvwriter.writerows(updated_rows)

            print(f"Records where '{column}' equals '{value}' have been deleted from '{dbtable}'.")
        else:
            print(f"Table '{dbtable}' does not exist or is not a directory.")

    def sum_numeric_column(self, dbtable, column):
        folder_path = f"{dbtable}"
        total_sum = 0
        max_decimal_places = 0
        is_numeric = True
        first_file = True

        if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
            print(f"Table '{dbtable}' does not exist or is not a directory.")
            return None

        for file_name in os.listdir(folder_path):
            if file_name.endswith(".csv"):
                file_path = os.path.join(folder_path, file_name)
                with open(file_path, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    header = next(csvreader, None)
                    if first_file and column not in header:
                        print(f"Column '{column}' does not exist in '{dbtable}'.")
                        return None
                    first_file = False

                    col_index = header.index(column)

                    for row in csvreader:
                        try:
                            value_str = row[col_index]
                            value = float(value_str)
                            total_sum += value

                            # Determine the number of decimal places
                            if '.' in value_str:
                                decimal_places = len(value_str.split('.')[1])
                                max_decimal_places = max(max_decimal_places, decimal_places)
                        except ValueError:
                            is_numeric = False
                            break

        if is_numeric:
            # Adjust the total sum for floating point precision based on max_decimal_places
            return round(total_sum, max_decimal_places)
        else:
            return None

    def count_records(self, dbtable):
        folder_path = f"{dbtable}"
        record_count = 0

        if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
            print(f"Table '{dbtable}' does not exist or is not a directory.")
            return 0

        for file_name in os.listdir(folder_path):
            if file_name.endswith(".csv"):
                file_path = os.path.join(folder_path, file_name)
                with open(file_path, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    next(csvreader, None)  # Skip the header row
                    for row in csvreader:
                        record_count += 1

        return record_count
    def average_numeric_column(self, dbtable, column):
        total_sum = self.sum_numeric_column(dbtable, column)
        if total_sum is not None:
            record_count = self.count_records(dbtable)
            if record_count > 0:
                average = total_sum / record_count
                return average
            else:
                print(f"No records found in '{dbtable}'.")
                return None
        else:
            print(f"Column '{column}' contains non-numeric values or is empty.")
            return None
    def find_max_in_column(self, dbtable, column):
        folder_path = f"{dbtable}"
        max_value = None
        is_numeric = True
        first_file = True

        if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
            print(f"Table '{dbtable}' does not exist or is not a directory.")
            return None

        for file_name in os.listdir(folder_path):
            if file_name.endswith(".csv"):
                file_path = os.path.join(folder_path, file_name)
                with open(file_path, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    header = next(csvreader, None)
                    if first_file and column not in header:
                        print(f"Column '{column}' does not exist in '{dbtable}'.")
                        return None
                    first_file = False

                    col_index = header.index(column)

                    for row in csvreader:
                        try:
                            value = float(row[col_index])
                            if max_value is None or value > max_value:
                                max_value = value
                        except ValueError:
                            is_numeric = False
                            break

        if is_numeric:
            return max_value
        else:
            return None
    def find_min_in_column(self, dbtable, column):
        folder_path = f"{dbtable}"
        min_value = None
        is_numeric = True
        first_file = True

        if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
            print(f"Table '{dbtable}' does not exist or is not a directory.")
            return None

        for file_name in os.listdir(folder_path):
            if file_name.endswith(".csv"):
                file_path = os.path.join(folder_path, file_name)
                with open(file_path, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    header = next(csvreader, None)
                    if first_file and column not in header:
                        print(f"Column '{column}' does not exist in '{dbtable}'.")
                        return None
                    first_file = False

                    col_index = header.index(column)

                    for row in csvreader:
                        try:
                            value = float(row[col_index])
                            if min_value is None or value < min_value:
                                min_value = value
                        except ValueError:
                            is_numeric = False
                            break

        if is_numeric:
            return min_value
        else:
            return None
    def group_by_column(self, dbtable, column, chunk_size=500):
        folder_path = dbtable
        if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
            print(f"Table '{dbtable}' does not exist or is not a directory.")
            return

        grouped_data = {}
        header = None

        for file_name in os.listdir(folder_path):
            if file_name.endswith(".csv"):
                file_path = os.path.join(folder_path, file_name)
                with open(file_path, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)

                    if header is None:
                        header = next(csvreader, None)
                        col_index = header.index(column)
                    else:
                        next(csvreader, None)  # Skip header in subsequent files

                    for row in csvreader:
                        key = row[col_index]
                        grouped_data.setdefault(key, []).append(row)

        new_folder = f"{dbtable}_groupby_{column}"
        os.makedirs(new_folder, exist_ok=True)

        for key, rows in grouped_data.items():
            # Chunking grouped data
            for i in range(0, len(rows), chunk_size):
                chunk_data = rows[i:i + chunk_size]
                chunk_path = os.path.join(new_folder, f"group_{key}_{i // chunk_size}.csv")
                with open(chunk_path, 'w', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile)
                    csvwriter.writerow(header)
                    csvwriter.writerows(chunk_data)

        print(f"Grouped data saved in chunks in '{new_folder}'")

    def order_by_column(self, dbtable, column, chunk_size=500):
        folder_path = dbtable
        if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
            print(f"Table '{dbtable}' does not exist or is not a directory.")
            return

        all_data = []
        header = None

        for file_name in os.listdir(folder_path):
            if file_name.endswith(".csv"):
                file_path = os.path.join(folder_path, file_name)
                with open(file_path, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)

                    if header is None:
                        header = next(csvreader, None)
                        col_index = header.index(column)
                    else:
                        next(csvreader, None)  # Skip header in subsequent files

                    all_data.extend(csvreader)

        all_data.sort(key=lambda x: x[col_index])

        new_folder = f"{dbtable}_orderby_{column}"
        os.makedirs(new_folder, exist_ok=True)

        for i in range(0, len(all_data), chunk_size):
            chunk_data = all_data[i:i + chunk_size]
            chunk_path = os.path.join(new_folder, f"chunk_{i // chunk_size}.csv")
            with open(chunk_path, 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)
                if i == 0:
                    csvwriter.writerow(header)  # Header only in the first chunk
                csvwriter.writerows(chunk_data)

        print(f"Ordered data saved in chunks in '{new_folder}'")

    def print_and_delete_folder(self, folder_path):
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            for file_name in sorted(os.listdir(folder_path)):
                file_path = os.path.join(folder_path, file_name)
                if file_name.endswith(".csv"):
                    # Print file contents
                    with open(file_path, 'r', newline='') as csvfile:
                        csvreader = csv.reader(csvfile)
                        for row in csvreader:
                            print(', '.join(row))

                    # Delete file
                    #os.remove(file_path)

            # Delete folder after all files are deleted
            #os.rmdir(folder_path)
            print(f"Folder '{folder_path}' has been deleted.")
        else:
            print(f"Folder '{folder_path}' does not exist.")

    