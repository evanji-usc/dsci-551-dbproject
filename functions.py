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
                    self.process_chunk(folder_path, chunk_rows, header)

            print(f"Data loaded into chunks within folder '{dbtable}'.")

    def process_chunk(self, folder_path, chunk_rows, header):
        with open(folder_path, 'w', newline='') as folder_csv:
            csvwriter = csv.writer(folder_csv)
            if header:
                csvwriter.writerow(header)  # Write the header in every chunk
            csvwriter.writerows(chunk_rows)

    def show_active(self):
        print(f"Active Database: {self.active_database}")
        print(f"Active Table: {self.active_table}")
    def add_records_to_table(self, records, dbtable):
        def get_num_columns(table_path):
            # Implement the logic to get the number of columns in the table
            # You can read the header of the first chunk file to determine the columns
            # For simplicity, I'm assuming that the first chunk file contains the header
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
                chunk_size_limit = 5000

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
        chunk_number = len([f for f in os.listdir(folder_path) if f.endswith(".csv")]) + 1
        chunk_path = os.path.join(folder_path, f"{chunk_number}.csv")

        with open(chunk_path, 'w', newline='') as chunk_csv:
            csvwriter = csv.writer(chunk_csv)
            csvwriter.writerow(records)

        return chunk_path

    def append_to_chunk(self, chunk_path, records):
        with open(chunk_path, 'a', newline='') as chunk_csv:
            csvwriter = csv.writer(chunk_csv)
            csvwriter.writerow(records)

        print(f"Records appended to the last chunk: {chunk_path}")
    def search_in_folder(self, column, value, dbtable=None):
        if not dbtable:
            dbtable = self.active_table

        folder_path = f"{dbtable}"
        if os.path.exists(folder_path):
            matching_records = []

            for file_name in os.listdir(folder_path):
                if file_name.endswith(".csv"):
                    file_path = os.path.join(folder_path, file_name)
                    with open(file_path, 'r', newline='') as csvfile:
                        csvreader = csv.reader(csvfile)
                        header = next(csvreader, None)

                        if column in header:
                            column_index = header.index(column)

                            for row in csvreader:
                                if row[column_index] == value:
                                    matching_records.append(row)

            if matching_records:
                print(f"Matching records in '{dbtable}':")
                for record in matching_records:
                    print(record)
            else:
                print(f"No matching records found in '{dbtable}'.")
        else:
            print(f"Table '{dbtable}' does not exist.")
