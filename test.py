import os
from cli import MyCLI

def test_create_set_database():
    cli = MyCLI()
    cli.onecmd("! db testdb")
    assert cli.functions.active_database == "testdb.csv", "Active database not set correctly"

def test_create_set_table():
    cli = MyCLI()
    cli.onecmd("! db testdb")
    cli.onecmd("! table testtable")
    assert cli.functions.active_table == "testtable", "Active table not set correctly"

def test_delete_table():
    cli = MyCLI()
    cli.onecmd("! db testdb")
    cli.onecmd("! table testtable")
    cli.onecmd("delete_table testtable")
    assert not os.path.exists("testtable"), "Table not deleted correctly"

def test_show_tables():
    cli = MyCLI()
    cli.onecmd("! db testdb")
    cli.onecmd("show tables")
    # This part is tricky without knowing how your CLI outputs information
    # You might need to capture stdout or check the file directly
def test_append_records():
    cli = MyCLI()
    cli.onecmd("! db testdb")
    cli.onecmd("! table testtable")
    cli.onecmd("+ 5.1,3.5,1.4,0.2,Iris-setosa")
    # Verify the record is added (mock or read the last chunk file of testtable)

def test_conditional_search():
    cli = MyCLI()
    cli.onecmd("! db testdb")
    cli.onecmd("! table testtable")
    cli.onecmd("> petal.length 1.5")
    # Verify the search results (mock or read the output)

def test_exact_match_search():
    cli = MyCLI()
    cli.onecmd("! db testdb")
    cli.onecmd("! table testtable")
    cli.onecmd("= variety Iris-setosa")
    # Verify the search results

def test_group_by_column():
    cli = MyCLI()
    cli.onecmd("! db testdb")
    cli.onecmd("! table testtable")
    cli.onecmd("[] variety")
    # Verify the grouping (check created group files)

def test_order_by_column():
    cli = MyCLI()
    cli.onecmd("! db testdb")
    cli.onecmd("! table testtable")
    cli.onecmd("@ petal.length")
    # Verify the ordering (check the order in the output files)

def test_inner_join():
    cli = MyCLI()
    cli.onecmd("! db testdb")
    cli.onecmd("&& testtable1 testtable2 petal.length petal.length")
    # Verify the join results (check the joined table's files)

def test_count_records():
    cli = MyCLI()
    cli.onecmd("! db testdb")
    cli.onecmd("! table testtable")
    cli.onecmd("#")
    # Verify the count (mock or read the output)

def test_average_numeric_column():
    cli = MyCLI()
    cli.onecmd("! db testdb")
    cli.onecmd("! table testtable")
    cli.onecmd("*/# petal.length")
    # Verify the average calculation

def test_find_max_in_column():
    cli = MyCLI()
    cli.onecmd("! db testdb")
    cli.onecmd("! table testtable")
    cli.onecmd("^ petal.length")
    # Verify the max value

def test_find_min_in_column():
    cli = MyCLI()
    cli.onecmd("! db testdb")
    cli.onecmd("! table testtable")
    cli.onecmd("_ petal.length")
    # Verify the min value

def test_update_records():
    cli = MyCLI()
    cli.onecmd("! db testdb")
    cli.onecmd("! table testtable")
    cli.onecmd("~ 5.1,3.5 with 5.2,3.6")
    # Verify the update (check the table's files)

if __name__ == "__main__":
    #test_create_set_database()
    #test_create_set_table()
    #test_append_records()
    #test_conditional_search()
    #test_exact_match_search()
    #test_group_by_column()
    #test_order_by_column()
    #test_inner_join()
    #test_count_records()
    #test_average_numeric_column()
    #test_find_max_in_column()
    #test_find_min_in_column()
    #test_update_records()
    #test_delete_table()
    #test_show_tables()
    print("All tests completed.")
