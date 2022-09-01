"""

main class.
Executes the script with using _Models and _Database_Operations.

AUTHOR : Sencer YÜCEL
Written in 2022 JUNE

"""

from playhouse.postgres_ext import *
import _Database_Operations

does_exist = lambda t1, t2, t3, t4, t5: t1.table_exists() and \
                                        t2.table_exists() and \
                                        t3.table_exists() and \
                                        t4.table_exists() and \
                                        t5.table_exists()

# Connection to the Postgresql database.
try:
    db = PostgresqlDatabase('Interpol', host='localhost', port=5432, user='postgres', password='databasePassword')
except Exception as Error:
    db = None
    print("Database connection was not successful: ", Error)

if __name__ == "__main__":
    db.connect()

    has_all_tables = does_exist(_Database_Operations.Criminals, _Database_Operations.Arrest_Warrants,
                                _Database_Operations.Nationalities, _Database_Operations.Languages,
                                _Database_Operations.Photos)

    if has_all_tables:
        _Database_Operations.table_updater()
    else:
        _Database_Operations.table_creator()

    db.close()
