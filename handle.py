def handle_tilde_command(line, functions):
    parts = line.split(' with ')
    if len(parts) == 2:
        old_values = parts[0].split()
        new_values = parts[1].split()
        functions.update_records(old_values, new_values)
    else:
        print("Invalid syntax. Use '~ val1 val2 ... with newval1 newval2 ...'")

def handle_search_with_condition(line, functions):
    parts = line.split()
    if len(parts) >= 3:
        operator = parts[0]
        search_column = parts[1]
        search_value = parts[2]

        dbtable = functions.active_table
        display_columns = None
        save_results = False

        for i in range(3, len(parts)):
            if parts[i].lower() == 'show':
                display_columns = parts[i + 1:]
                break
            elif parts[i] == 'save':
                save_results = True
            else:
                if not save_results:
                    dbtable = parts[i]

        functions.search_with_condition(search_column, search_value, operator, dbtable, display_columns, save_results)
    else:
        print("Invalid syntax. Use '<operator> <column> <value> [table] [show <cols>] [save]'")