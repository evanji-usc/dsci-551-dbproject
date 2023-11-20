import cmd
from functions import MyFunctions
from handle import handle_tilde_command, handle_search_with_condition
class MyCLI(cmd.Cmd):
    intro = "Welcome to the CLI!"
    prompt = 'evbase> '

    def do_help(self, arg):
        print("Custom CLI Commands:")
        print("  ! db [dbname] - Create or set the active database.\n")
        print("  ! table [tablename] - Create or set the active table.\n")
        print("  () 'path.csv' <tablename> - load a csv file into specified table name")
        print("  + [record1,record2,...] [dbtable(optional)] - Append records to the last chunk.\n")
        print("  - val1 val2 val3 ... - removes matching records from the table.\n ")
        print("  [>], [<], [>=], [<=] [column] [value] [show] [columns(optional)] - Conditional search.\n")
        print("  = [column] [value] [show] [columns(optional)] - Exact match search.\n")
        print("  [] [column] - Group by column.\n")
        print("  @ [column] - Order by column.\n")
        print("  && [dbtable1] [dbtable2] [column1] [column2] - Inner join two tables.\n")
        print("  # [dbtable] - Count records in a table.\n")
        print("  */# [column] - Average of a numeric column.\n")
        print("  ^ [column] - Find max value in a column.\n")
        print("  _ [column] - Find min value in a column.\n")
        print("  ~ [val1] [val2] ... with [newval1] [newval2] ... - Update records.\n")
        print("  delete table [dbtable] - Delete a table.\n")
        print("  show tables - Show all tables in the active database.\n")
        print("  show databases - SHow all available databases,\n")
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

        elif len(parts) == 2 and parts[0] == 'show' and parts[1] == 'active':
            self.functions.show_active()
        elif len(parts) == 2 and parts[0] == '=':
            dbtable = parts[1]
            self.functions.show_entire_table(dbtable)
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
            parts = line.split()
            column = parts[1]
            save_results = 'save' in parts

            if self.functions.active_table:
                self.functions.group_by_column(self.functions.active_table, column, chunk_size=500)
                if not save_results:
                    #print(f"{self.functions.active_table}_orderby_{column}")
                    self.functions.print_and_delete_folder(f"{self.functions.active_table}")
                    print('Table deleted, remember to set a new active table')

        # Handling for '@' command
        elif line.startswith("@ "):
            parts = line.split()
            column = parts[1]
            save_results = 'save' in parts

            if self.functions.active_table:
                self.functions.order_by_column(self.functions.active_table, column,chunk_size=500)
                if not save_results:
                    #print(f"{self.functions.active_table}_orderby_{column}")
                    self.functions.print_and_delete_folder(f"{self.functions.active_table}")
                    print('Table deleted, remember to set a new active table')
                    self.functions.active_table=None

        elif line.startswith(('=', '>', '<', '>=', '<=')):
            handle_search_with_condition(line, self.functions)
        elif line.strip() == 'show tables':
            self.functions.show_tables()
        elif line.startswith('delete table '):
            dbtable = line[len('delete table '):].strip()
            self.functions.delete_table(dbtable)
        elif line.startswith('~ '):
            handle_tilde_command(line[2:],self.functions)
        elif line.startswith("&&"):
            parts = line.split()
            dbtable1, dbtable2, column1, column2 = parts[1:5]
            save_results = 'save' in parts
            if save_results:
                output_table = (f"{dbtable1}_joined_table_{dbtable2}")  # or any other naming convention
                self.functions.create_or_set_table(output_table)
            self.functions.join_tables(dbtable1, dbtable2, column1, column2, output_table)
        elif line == 'show databases':
            self.functions.show_databases()
        elif line.startswith('delete database '):
            dbname = line[len('delete database '):].strip()
            self.functions.delete_database(dbname)
        elif line.startswith('-'):
            # Split the line to extract the values
            parts = line.split(maxsplit=1)
            if len(parts) == 2:
                values_str = parts[1]
                old_values = values_str.split()
                if self.functions.active_table:
                    # Call the remove_records method with the provided values
                    self.functions.remove_records(old_values)
                else:
                    print("No active table. Use '! table dbtable' to set the active table.")
            else:
                print("Invalid syntax. Use '- val1 val2 ...' to remove records.")
        else:
            print(f"Unknown syntax: {line}")
if __name__ == "__main__":
    cli = MyCLI()
    cli.cmdloop_with_prompt()
