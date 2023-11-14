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
                self.functions.add_records_to_table(records,dbtable)
            else:
                print("No active table. Use '! table dbtable' to set the active table.")
        elif len(parts) >= 3 and parts[0] == '=':
            column = parts[1]
            value = parts[2]
            dbtable = parts[3] if len(parts) >= 4 else self.functions.active_table
            self.functions.search_in_folder(column, value, dbtable)
        elif len(parts) == 2 and parts[0] == 'show' and parts[1] == 'active':
            self.functions.show_active()
        else:
            print(f"Unknown syntax: {line}")
if __name__ == "__main__":
    cli = MyCLI()
    cli.cmdloop_with_prompt()
