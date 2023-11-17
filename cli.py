import cmd
from functions import MyFunctions

class MyCLI(cmd.Cmd):
    intro = "Welcome to the CLI!"
    prompt = 'evbase> '

    def __init__(self):
        super().__init__()
        self.functions = MyFunctions(self)

    def cmdloop_with_prompt(self):
        while True:
            try:
                self.cmdloop()
                break
            except KeyboardInterrupt:
                print("\nType 'exit' to exit the CLI.")

    def do_exit(self, line):
        """Exit the CLI"""
        return True

    '''
    def do_db(self, line):
        """Create or set the active database"""
        self.functions.create_or_set_database(line.strip())

    def do_table(self, line):
        """Create or set the active table in the current active database"""
        self.functions.create_or_set_table(line.strip())
    def do_import_csv(self, line):
        """Import a CSV file into a specified table"""
        parts = line.split()
        if len(parts) == 2:
            path = parts[0]
            dbtable = parts[1]
            chunk_size = 5000  # Set your desired chunk size here

            self.functions.import_csv_into_table(path, dbtable, chunk_size)
        else:
            print("Invalid syntax. Use 'import_csv path.csv dbtable'.")

    def do_show_active(self, line):
        """Show the active database and table"""
        self.functions.show_active()
    def do_plus_records(self, line):
        """Append records to the last chunk in the folder"""
        parts = line.split(maxsplit=1)
        if parts:
            records_str = parts[0]
            dbtable = parts[1] if len(parts) > 1 else self.functions.active_table

            records = records_str.split(',')
            self.functions.add_records_to_table(records, dbtable)
        else:
            print("Invalid syntax. Use '+ record1,record2,... dbtable'.")
    '''
    def default(self, line):
        """Handle custom syntax ! db dbname"""
        parts = line.split()
        if len(parts) == 3 and parts[0] == '!' and parts[1] == 'db':
            dbname = parts[2]
            self.functions.create_or_set_database(dbname)
        elif len(parts) == 3 and parts[0] == '!' and parts[1] == 'table':
            dbtable = parts[2]
            self.functions.create_or_set_table(dbtable)
        elif line.startswith("() "):
            # Split the line to extract path and dbtable
            _, path, dbtable = line.split()
            chunk_size = 500  # Set your desired chunk size here

            self.functions.import_csv_into_table(path, dbtable, chunk_size)
        elif len(parts) > 1 and parts[0] == '+':
            # Handle + record commands
            if self.functions.active_table:
                records = parts[1:]
                dbtable = self.functions.active_table
                chunk_size = 500
                self.functions.add_records_to_table(records,dbtable)
            else:
                print("No active table. Use '! table dbtable' to set the active table.")
        elif len(parts) >= 3 and parts[0] == '=':
            search_column = parts[1]
            search_value = parts[2]

            # Determine if a specific table is specified and where the display columns start
            dbtable = self.functions.active_table
            display_columns_start = 3
            if len(parts) > 3 and parts[3].lower() != 'show':
                dbtable = parts[3]
                display_columns_start = 4

            # Check if specific columns to display are specified
            display_columns = None
            if len(parts) > display_columns_start and parts[display_columns_start].lower() == 'show':
                display_columns = parts[display_columns_start + 1:]

            self.functions.search_in_folder(search_column, search_value, dbtable, display_columns)
        elif len(parts) == 2 and parts[0] == 'show' and parts[1] == 'active':
            self.functions.show_active()
        elif len(parts) == 2 and parts[0] == '=':
            dbtable = parts[1]
            self.functions.show_entire_table(dbtable)
        elif len(parts) >= 3 and parts[0] == '-' and parts[1] == '=':
            column = parts[2]
            value = parts[3]
            dbtable = self.functions.active_table if len(parts) == 4 else parts[4]
            self.functions.delete_records(column, value, dbtable)
        elif len(parts) == 2 and parts[0] == '#':
            dbtable = parts[1] if len(parts) > 1 else self.functions.active_table
            count = self.functions.count_records(dbtable)
            print(f"Total records in '{dbtable}': {count}")
        elif len(parts) >= 2 and parts[0] == '*':
            column = parts[1]
            dbtable = parts[2] if len(parts) > 2 else self.functions.active_table
            total_sum = self.functions.sum_numeric_column(dbtable, column)
            if total_sum is not None:
                print(f"Sum of numeric values in column '{column}' of '{dbtable}': {total_sum}")
            else:
                print(f"Column '{column}' contains non-numeric values or is empty.")
        elif len(parts) == 2 and parts[0] == '*/#':
            column = parts[1]
            dbtable = self.functions.active_table
            average = self.functions.average_numeric_column(dbtable, column)
            if average is not None:
                print(f"Average of numeric values in column '{column}' of '{dbtable}': {average}")
        elif len(parts) >= 2 and parts[0] == '^':
            column = parts[1]
            dbtable = parts[2] if len(parts) > 2 else self.functions.active_table
            max_value = self.functions.find_max_in_column(dbtable, column)
            if max_value is not None:
                print(f"Maximum value in column '{column}' of '{dbtable}': {max_value}")
            else:
                print(f"Could not determine the maximum value in column '{column}' of '{dbtable}'.")
        elif len(parts) >= 2 and parts[0] == '_':
            column = parts[1]
            dbtable = parts[2] if len(parts) > 2 else self.functions.active_table
            min_value = self.functions.find_min_in_column(dbtable, column)
            if min_value is not None:
                print(f"Minimum value in column '{column}' of '{dbtable}': {min_value}")
            else:
                print(f"Could not determine the minimum value in column '{column}' of '{dbtable}'.")
        elif line.startswith("[] "):
            _, column = line.split(maxsplit=1)
            if self.functions.active_table:
                # Group by column
                self.functions.group_by_column(self.functions.active_table, column)

                # Print and delete folder
                self.functions.print_and_delete_folder(f"{self.functions.active_table}_groupby_{column}")

            else:
                print("No active table. Use '! table dbtable' to set the active table.")

        elif line.startswith("@ "):
            _, column = line.split(maxsplit=1)
            if self.functions.active_table:
                # Order by column
                self.functions.order_by_column(self.functions.active_table, column)

                # Print and delete folder
                self.functions.print_and_delete_folder(f"{self.functions.active_table}_orderby_{column}")

            else:
                print("No active table. Use '! table dbtable' to set the active table.")
        else:
            print(f"Unknown syntax: {line}")
if __name__ == "__main__":
    cli = MyCLI()
    cli.cmdloop_with_prompt()
