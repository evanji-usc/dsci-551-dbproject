import datetime
import csv
import os
import shutil
import re
import heapq
class MyFunctions:
    def __init__(self, cli):
        self.cli = cli
        self.active_database = None
        self.active_table = None
    def add_record(self, record, filename):
        if os.path.exists(filename):
            with open(filename, 'a', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow([record])
            print(f"Record '{record}' added to '{filename}'.")
        else:
            print(f"File '{filename}' does not exist.")
    def create_or_set_database(self, dbname):
        database_folder = "databases"  # Define the folder name
        if not os.path.exists(database_folder):
            os.makedirs(database_folder)  # Create the folder if it doesn't exist

        filename = os.path.join(database_folder, f"{dbname}.csv")  # Adjust the path to include the folder
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
            metadata_record = f"{dbtable}, Created: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            self.add_record(metadata_record, self.active_database)
            print(f"Active table set to '{folder_path}'.")
    def import_csv_into_table(self, path, dbtable, chunk_size):
        if not os.path.exists(path):
            print(f"File '{path}' does not exist.")
            return
        if os.path.exists(dbtable) and os.path.isdir(dbtable):
            # Here you can decide how to handle existing tables
            # For example, you can ask the user if they want to overwrite or append
            print(f"Table '{dbtable}' already exists. Please choose a different table name or delete the existing one.")
            return
        if not os.path.exists(dbtable):
            self.create_or_set_table(dbtable)
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

    def process_chunk(self, chunk_path, chunk_data, header, first_chunk=False):
        with open(chunk_path, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(header)  # Write the header only for the first chunk
            csvwriter.writerows(chunk_data)

    def show_active(self):
        print(f"Active Database: {self.active_database}")
        print(f"Active Table: {self.active_table}")
    def add_records_to_table(self, records, dbtable):
        folder_path = f"{dbtable}"
        if os.path.exists(folder_path):
            last_chunk_path = self.get_last_chunk_path(folder_path)
            if last_chunk_path:
                last_chunk_size = os.path.getsize(last_chunk_path)
                chunk_size_limit = 500

                # Check if the number of records matches the number of columns
                num_columns = self.get_num_columns(dbtable)
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
    
    def get_num_columns(self,table_path):
            first_chunk_path = os.path.join(table_path, f"{table_path}_chunk_1.csv")
            if os.path.exists(first_chunk_path):
                with open(first_chunk_path, 'r') as chunk_csv:
                    csvreader = csv.reader(chunk_csv)
                    header = next(csvreader, None)
                    if header:
                        return len(header)

            return 0  # Return 0 if unable to determine the number of columns

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
    def search_with_condition(self, column, value, operator, dbtable=None, display_columns=None, save_results=False):
        folder_path = f"{dbtable}"
        if os.path.exists(folder_path):
            matching_records = []
            header = None

            for file_name in os.listdir(folder_path):
                if file_name.endswith(".csv"):
                    file_path = os.path.join(folder_path, file_name)
                    with open(file_path, 'r', newline='') as csvfile:
                        csvreader = csv.reader(csvfile)
                        if header is None:
                            header = next(csvreader, None)
                            display_indices = [header.index(col) for col in display_columns] if display_columns else range(len(header))

                        for row in csvreader:
                            cell_value = row[header.index(column)]
                            if self.compare_values(cell_value, value, operator):
                                matching_records.append([row[i] for i in display_indices])

            if save_results:
                self.save_search_results(matching_records, header, dbtable, column, value)
            print(", ".join([header[i] for i in display_indices]))
            for record in matching_records:
                print(", ".join(record))
        else:
            print(f"Table '{dbtable}' does not exist.")

    def compare_values(self, cell_value, value, operator):
        try:
            cell_value = float(cell_value)
            value = float(value)
        except ValueError:
            return False

        if operator == '>':
            return cell_value > value
        elif operator == '<':
            return cell_value < value
        elif operator == '>=':
            return cell_value >= value
        elif operator == '<=':
            return cell_value <= value
        elif operator == '=':
            return cell_value == value
        else:
            return False
    def show_entire_table(self, dbtable):
        folder_path = f"{dbtable}"
        files = os.listdir(folder_path)
        files.sort(key=lambda f: int(re.search(r"chunk_(\d+)", f).group(1)))
        first_file_path = os.path.join(folder_path, files[0])
        with open(first_file_path, 'r', newline='') as csvfile:
            csvreader = csv.reader(csvfile)
            header = next(csvreader, None)
            if header:
                print(', '.join(header))

        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            for file_name in files:
                if file_name.endswith(".csv"):
                    file_path = os.path.join(folder_path, file_name)
                    with open(file_path, 'r', newline='') as csvfile:
                        csvreader = csv.reader(csvfile)
                        next(csvreader, None)
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
    
    def group_by_column(self, dbtable, column, chunk_size=5000):
        if not self.active_table:
            print("No active table set. Use '! table dbtable' to set the active table.")
            return
        folder_path = dbtable
        if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
            print(f"Table '{dbtable}' does not exist or is not a directory.")
            return

        grouped_data = {}
        header = None
        col_index = None

        try:
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
        except ValueError:
            print(f"Column '{column}' not found in table '{dbtable}'.")
            return

        # Write the grouped data from all chunks
        new_folder = f"{dbtable}_groupby_{column}"
        self.create_or_set_table(new_folder)
        self.write_grouped_data_in_chunks(grouped_data, header, new_folder, chunk_size)
        print(f"Grouped data saved in chunks in '{new_folder}'")

    def order_by_column(self, dbtable, column, chunk_size=500):
        if not self.active_table:
            print("No active table set. Use '! table dbtable' to set the active table.")
            return
        folder_path = dbtable
        if not os.path.exists(folder_path) or not os.path.isdir(folder_path):
            print(f"Table '{dbtable}' does not exist or is not a directory.")
            return

        temp_files = []
        header = None
        col_index = None

        # Sort each chunk and write to a temp file
        try:
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

                        chunk_data = sorted(csvreader, key=lambda x: x[col_index])
                        temp_file_path = os.path.join(folder_path, f"temp_sorted_{file_name}")
                        with open(temp_file_path, 'w', newline='') as temp_file:
                            csvwriter = csv.writer(temp_file)
                            csvwriter.writerows(chunk_data)
                        temp_files.append(temp_file_path)
        except ValueError:
            print(f"Column '{column}' not found in table '{dbtable}'.")
            return

        # Merge sorted chunks and write in new chunks
        new_folder = f"{dbtable}_orderby_{column}"
        self.create_or_set_table(new_folder)

        # Initialize heap
        sorted_data = []
        file_pointers = []
        try:
            for temp_file in temp_files:
                fp = open(temp_file, 'r')
                reader = csv.reader(fp)
                file_pointers.append(fp)
                next(reader, None)  # Skip header
                for row in reader:
                    heapq.heappush(sorted_data, (row[col_index], row))

            # Write the merged sorted data in chunks based on size
            chunk_count = 0
            current_chunk_size = 0
            chunk = []

            while sorted_data:
                _, row = heapq.heappop(sorted_data)
                row_string = ','.join(row) + '\n'
                row_size = sum(len(str(cell)) for cell in row)

                if current_chunk_size + row_size > chunk_size and chunk:
                    chunk_file = os.path.join(new_folder, f"chunk_{chunk_count}.csv")
                    self.process_chunk(chunk_file, chunk, header, first_chunk=(chunk_count == 0))
                    chunk_count += 1
                    chunk = []
                    current_chunk_size = 0

                chunk.append(row)
                current_chunk_size += row_size

            # Write the last chunk if it has data
            if chunk:
                chunk_file = os.path.join(new_folder, f"chunk_{chunk_count}.csv")
                self.process_chunk(chunk_file, chunk, header, first_chunk=(chunk_count == 0))
        finally:
            for fp in file_pointers:
                fp.close()

        # Clean up temp files
        for temp_file in temp_files:
            os.remove(temp_file)

        print(f"Ordered data saved in chunks in '{new_folder}'")

    def print_and_delete_folder(self, folder_path):
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            # Sort files by chunk number
            files = os.listdir(folder_path)
            files.sort(key=lambda f: int(re.search(r"chunk_(\d+)", f).group(1)))

            for file_name in files:
                file_path = os.path.join(folder_path, file_name)
                if file_name.endswith(".csv"):
                    # Print file contents, skipping the header
                    with open(file_path, 'r', newline='') as csvfile:
                        csvreader = csv.reader(csvfile)
                        next(csvreader, None)  # Skip the header row
                        for row in csvreader:
                            print(', '.join(row))

                    # Delete file (uncomment if deletion is needed)
                    #os.remove(file_path)

            # Delete folder after all files are deleted (uncomment if deletion is needed)
            #os.rmdir(folder_path)
            self.delete_table(folder_path)
            
            print(f"Folder '{folder_path}' has been deleted.")
        else:
            print(f"Folder '{folder_path}' does not exist.")
    def create_chunks(self, all_data, header, new_folder, chunk_size):
        chunk_data = []
        current_size = 0
        chunk_index = 0

        for row in all_data:
            row_size = sum(len(str(cell)) for cell in row)
            if current_size + row_size > chunk_size and chunk_data:
                chunk_path = os.path.join(new_folder, f"chunk_{chunk_index}.csv")
                self.process_chunk(chunk_path, chunk_data, header)
                chunk_data = [row]
                current_size = row_size
                chunk_index += 1
            else:
                chunk_data.append(row)
                current_size += row_size

        if chunk_data:
            chunk_path = os.path.join(new_folder, f"chunk_{chunk_index}.csv")
            self.process_chunk(chunk_path, chunk_data, header)
    
    def save_search_results(self, records, header, dbtable, column, value):
        search_folder = f"{dbtable}_search_{column}_{value}"
        self.create_or_set_table(search_folder)

        chunk_size = 500
        chunk_number = 0
        chunk_rows = []

        for row in records:
            chunk_rows.append(row)
            if len(chunk_rows) == chunk_size:
                chunk_file = os.path.join(search_folder, f"chunk_{chunk_number}.csv")
                self.process_chunk(os.path.join(search_folder, f"chunk_{chunk_number}.csv"), chunk_rows, header)
                chunk_rows = []
                chunk_number += 1

        if chunk_rows:
            # Save remaining rows in a new chunk
            chunk_file = os.path.join(search_folder, f"chunk_{chunk_number}.csv")
            self.process_chunk(os.path.join(search_folder, f"chunk_{chunk_number}.csv"), chunk_rows, header)

        print(f"Search results saved in folder '{search_folder}'.")   
    
    def show_tables(self):
        if self.active_database is None:
            print("No active database is set.")
            return
        if os.path.exists(self.active_database):
            with open(self.active_database, 'r', newline='') as csvfile:
                csvreader = csv.reader(csvfile)
                tables = [row[0] for row in csvreader if row]
                
            if tables:
                print(f"Tables in the active database '{self.active_database}':")
                for table in tables:
                    print(table)
            else:
                print(f"The active database '{self.active_database}' is empty.")
        else:
            print(f"Active database file '{self.active_database}' does not exist.")

        
    def delete_table(self, dbtable):
        folder_path = f"{dbtable}"
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            # Delete the folder associated with the table
            shutil.rmtree(folder_path)
            print(f"Deleted table folder '{folder_path}'") 

            # Remove the record from the active database
            self.remove_table_record(dbtable)
            if self.active_table == dbtable:
                self.active_table = None
                print("Active table was deleted. Make sure to set a new active table.")

        else:
            print(f"Table '{dbtable}' does not exist.")

    def remove_table_record(self, dbtable):
        # Read all records except the one to delete
        updated_records = []
        if os.path.exists(self.active_database):
            with open(self.active_database, 'r', newline='') as csvfile:
                csvreader = csv.reader(csvfile)
                updated_records = [
                    row for row in csvreader 
                    if len(row) > 0 and not row[0].startswith(dbtable)
                ]

            # Write the updated records back to the CSV file
            with open(self.active_database, 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerows(updated_records)
            print(f"Removed '{dbtable}' record from the active database.")
    
    def update_records(self, old_values, new_values):
        folder_path = self.active_table
        if not os.path.exists(folder_path):
            print(f"Active table '{folder_path}' does not exist.")
            return

        count_updated = 0
        for file_name in os.listdir(folder_path):
            if file_name.endswith('.csv'):
                file_path = os.path.join(folder_path, file_name)
                updated_rows = []
                with open(file_path, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    for row in csvreader:
                        if row[:len(old_values)] == old_values:
                            updated_rows.append(new_values + row[len(old_values):])
                            count_updated += 1
                        else:
                            updated_rows.append(row)

                # Write the updated rows back to the CSV file
                with open(file_path, 'w', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile)
                    csvwriter.writerows(updated_rows)

        print(f"Updated {count_updated} rows with new values.")
    
    def write_grouped_data_in_chunks(self, data, header, folder_path, chunk_size):
        chunk_number = 0

        if isinstance(data, dict):  # If data is grouped (dictionary)
            for group in data.values():
                chunk_number = self.process_data_group(group, folder_path, header, chunk_size, chunk_number)
        elif isinstance(data, list):  # If data is sorted (list)
            self.process_data_group(data, folder_path, header, chunk_size, chunk_number)

    def process_data_group(self, group, folder_path, header, chunk_size, chunk_number):
        chunk_rows = []
        total_size = 0

        for row in group:
            row_size = sum(len(str(cell)) for cell in row)
            if total_size + row_size > chunk_size and chunk_rows:
                chunk_path = os.path.join(folder_path, f"chunk_{chunk_number}.csv")
                self.process_chunk(chunk_path, chunk_rows, header)
                chunk_rows = [row]
                total_size = row_size
                chunk_number += 1
            else:
                chunk_rows.append(row)
                total_size += row_size
        if chunk_rows:
            chunk_path = os.path.join(folder_path, f"chunk_{chunk_number}.csv")
            self.process_chunk(chunk_path, chunk_rows, header)
            chunk_number += 1

        return chunk_number
    
    def join_tables(self, dbtable1, dbtable2, column1, column2, output_table=None):
        try:
            table1_data = self.load_table_data(dbtable1, column1)
        except ValueError:
            print(f"Column '{column1}' not found in table '{dbtable1}'.")
            return

        if output_table:
            header = self.get_combined_header(dbtable1, dbtable2, column1, column2)
            self.create_or_set_table(output_table)

        folder_path2 = dbtable2
        chunk_number = 0
        current_chunk_size = 0
        joined_data = []

        for file_name in os.listdir(folder_path2):
            if file_name.endswith(".csv"):
                file_path = os.path.join(folder_path2, file_name)
                with open(file_path, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    header2 = next(csvreader, None)
                    try:
                        col_index2 = header2.index(column2)
                    except ValueError:
                        print(f"Column '{column2}' not found in table '{dbtable2}'.")
                        return

                    for row in csvreader:
                        key = row[col_index2]
                        if key in table1_data:
                            for table1_row in table1_data[key]:
                                row_without_column2 = row[:col_index2] + row[col_index2 + 1:]
                                joined_row = table1_row + row_without_column2
                                joined_data.append(joined_row)
                                current_chunk_size += sum(len(str(cell)) for cell in joined_row)

                                if current_chunk_size >= 500:
                                    chunk_file = os.path.join(output_table, f"chunk_{chunk_number}.csv")
                                    self.process_chunk(chunk_file, joined_data, header)
                                    chunk_number += 1
                                    joined_data = []
                                    current_chunk_size = 0

        # Process the last chunk if there's any remaining data
        if output_table and joined_data:
            chunk_file = os.path.join(output_table, f"chunk_{chunk_number}.csv")
            self.process_chunk(chunk_file, joined_data, header)

        if output_table:
            print(f"Joined data saved in chunks in '{output_table}'")

    def load_table_data(self, dbtable, join_column):
        table_data = {}
        folder_path = dbtable
        for file_name in os.listdir(folder_path):
            if file_name.endswith(".csv"):
                file_path = os.path.join(folder_path, file_name)
                with open(file_path, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    header = next(csvreader, None)
                    col_index = header.index(join_column)

                    for row in csvreader:
                        key = row[col_index]
                        if key not in table_data:
                            table_data[key] = []
                        table_data[key].append(row)
        return table_data
    def get_combined_header(self, dbtable1, dbtable2,column1, column2):
        header1 = self.get_header_from_first_chunk(dbtable1)
        header2 = self.get_header_from_first_chunk(dbtable2)

        combined_column_name = column1 + "+" + column2

        # Replace column names in headers with the combined column name
        header1[header1.index(column1)] = combined_column_name
        header2.remove(column2)

        combined_header = header1 + header2
        return combined_header

    def get_header_from_first_chunk(self, dbtable):
        folder_path = dbtable
        for file_name in os.listdir(folder_path):
            if file_name.endswith(".csv"):
                file_path = os.path.join(folder_path, file_name)
                with open(file_path, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    header = next(csvreader, None)
                    return header
        return None
    
    def show_databases(self):
        databases_folder = "databases"
        if os.path.exists(databases_folder) and os.path.isdir(databases_folder):
            databases = [f for f in os.listdir(databases_folder) if f.endswith(".csv")]
            if databases:
                print("Available Databases:")
                for db in databases:
                    print(db)
            else:
                print("No databases found.")
        else:
            print(f"Folder '{databases_folder}' does not exist. No databases exist currently. (You can create a database using the ! db <name> command)")
    
    def delete_database(self, dbname):
        # Delete all tables associated with the database
        database_file = f"databases/{dbname}.csv"
        if os.path.exists(database_file):
            with open(database_file, 'r') as dbfile:
                csvreader = csv.reader(dbfile)
                for row in csvreader:
                    table_folder = row[0]
                    if os.path.exists(table_folder) and os.path.isdir(table_folder):
                        shutil.rmtree(table_folder)  # Delete the table folder

            # Delete the database file itself
            os.remove(database_file)
            print(f"Database '{dbname}' and all associated tables have been deleted.")

            # Reset active database and table if they were deleted
            if self.active_database == database_file:
                self.active_database = None
            if self.active_table in [row[0] for row in csvreader]:
                self.active_table = None
        else:
            print(f"Database '{dbname}' does not exist.")
    
    def remove_records(self, old_values):
        folder_path = self.active_table
        if not os.path.exists(folder_path):
            print(f"Active table '{folder_path}' does not exist.")
            return

        # Check if the number of values matches the number of columns
        num_columns = self.get_num_columns(folder_path)
        if len(old_values) != num_columns:
            print(f"Number of values does not match the number of columns in the table.{self.active_table} has {num_columns} columns, {old_values} were given.")
            return
        
        count_removed = 0
        for file_name in os.listdir(folder_path):
            if file_name.endswith('.csv'):
                file_path = os.path.join(folder_path, file_name)
                remaining_rows = []
                with open(file_path, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    for row in csvreader:
                        if row[:len(old_values)] != old_values:
                            remaining_rows.append(row)
                        else:
                            count_removed += 1

                # Write the remaining rows back to the CSV file
                with open(file_path, 'w', newline='') as csvfile:
                    csvwriter = csv.writer(csvfile)
                    csvwriter.writerows(remaining_rows)

        if count_removed > 0:
            print(f"Removed {count_removed} records from '{folder_path}'.")
        else:
            print("No matching records found to remove.")

    def join_tables_inner(self, dbtable1, dbtable2, column1, column2,output_table=None):
                chunk_size=500
                chunk_number=0
                # Create a dictionary to store rows from the first table, keyed by the join column
                try:
                    table1_data = self.load_table_data(dbtable1, column1)
                except ValueError:
                    print(f"Column '{column1}' not found in table '{dbtable1}'.")
                    return
                joined_data = []
                # Now iterate through chunks of the second table and join with table1_data
                folder_path2 = dbtable2
                for file_name in os.listdir(folder_path2):
                    if file_name.endswith(".csv"):
                        file_path = os.path.join(folder_path2, file_name)
                        with open(file_path, 'r', newline='') as csvfile:
                            csvreader = csv.reader(csvfile)
                            header2 = next(csvreader, None)
                            try:
                                col_index2 = header2.index(column2)
                            except ValueError:
                                print(f"Column '{column2}' not found in table '{dbtable2}'.")
                                return
                            total_size = 0
                            for row in csvreader:
                                key = row[col_index2]
                                if key in table1_data:
                                    for table1_row in table1_data[key]:
                                        row_without_column2 = row[:col_index2] + row[col_index2 + 1:]
                                        joined_row = table1_row + row_without_column2
                                        row_size = sum(len(str(cell)) for cell in joined_row)
                                        joined_data.append(joined_row)
                                        # Process the joined_row (e.g., store, print, etc.)
                                        if total_size + row_size > chunk_size and chunk_rows:
                                            chunk_path = os.path.join(f"chunk_{chunk_number}.csv")
                                            self.process_chunk(chunk_path, chunk_rows, header)
                                            chunk_rows = [row]
                                            total_size = row_size
                                            chunk_number += 1
                                        else:
                                            chunk_rows.append(row)
                                            total_size += row_size
                            #print(joined_data)
                if output_table:
                    header = self.get_combined_header(dbtable1, dbtable2,column1,column2)
                    self.write_grouped_data_in_chunks(joined_data, header, output_table, chunk_size=500)
                    print(f"Joined data saved in chunks in '{output_table}'")