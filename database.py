"""
Database Module
~~~~~~~~~~~~~~~
Wrapper for database connection and querying
functions and useful util functions.
"""

__title__ = 'database'
__author__ = 'Chidiebere Okpoechi (Chidi)'
__version__ = '1.0.0'

import os
import sqlite3
import unittest
from typing import Collection, Dict, List, Literal, Union


class Util:
    @classmethod
    def escape_value(cls, value: Union[str, int, Literal[None]]) -> str:
        '''
        Return the SQL-escaped string equivalent of a value
        for use in queries

        Parameters
        ----------
        value : str | int | Literal[None]
            Literal to be converted and appropriately escaped for use
            in SQL queries

        Returns
        -------
        escaped : str
            SQL-query usable string value
        '''

        if (value == None):
            return 'null'

        if (type(value) == str):
            return '\'%s\'' % value

        return str(value)

    @classmethod
    def escape_list(cls,
                    values: Collection[Union[str, int, Literal[None]]]
                    ) -> List[str]:
        '''
        Return the SQL-escaped string equivalents of
        a list of values for use in queries

        Parameters
        ----------
        values : Collection[str | int | Literal[None]]
            Collection of literals to be converted and appropriately escaped
            for use in SQL queries

        Returns
        -------
        escaped : List[str]
            List of SQL-query usable string values
        '''

        return [cls.escape_value(value) for value in values]


class UtilTest(unittest.TestCase):
    def test_escape_value(self) -> None:
        '''
        Test Util `escape_value` method
        '''

        value, expected = None, 'null'
        self.assertEqual(Util.escape_value(value), expected)

        value, expected = 'bob', '\'bob\''
        self.assertEqual(Util.escape_value(value), expected)

        value, expected = 12, '12'
        self.assertEqual(Util.escape_value(value), expected)

    def test_escape_list(self) -> None:
        '''
        Test Util `escape_list` method
        '''

        values = [None, 'bob', 12]
        expected = ['null', '\'bob\'', '12']
        escaped = Util.escape_list(values)

        # Each value in the escaped list should match the expected
        self.assertListEqual(escaped, expected)


class Table:
    def __init__(self, connection, name: str,
                 schema: Dict[str, str]) -> None:

        self.connection = connection
        self.name = name
        self.schema = schema

        if (connection == None):
            raise Exception('Database connection was not specified')

    def initialize(self) -> None:
        '''
        Create the table in the database if it is not
        already present
        '''

        columns: List[str] = []

        for x in self.schema:
            columns.append('%s %s' % (x, self.schema[x]))

        stmt = 'CREATE TABLE IF NOT EXISTS `%s` (%s)' % (
            self.name, ', '.join(columns))

        self.execute_sql(stmt)

    def check_exists(self) -> bool:
        '''
        Checks if the table exists in the database

        Returns
        -------
        exists : bool
            Flag showing whether table exists in the database or not
        '''

        stmt = 'SELECT name FROM sqlite_master \
            WHERE type=\'table\' AND name=\'%s\'' % (self.name)

        results = self.execute_sql(stmt)

        return len(list(results)) == 1

    def insert(self, columns: tuple, values: List[tuple]) -> sqlite3.Cursor:
        '''
        Inserts one or more records into the table

        Parameters
        ----------
        columns : Tuple[string]
            Column names to use in insert statement

        values : List[tuple]
            List of values (as tuples) to insert for the specified
            columns

        Returns
        -------
        cursor : sqlite3.Cursor
            SQLite3 database cursor
        '''

        if (values == None or len(values) < 1):
            raise Exception('Invalid insert statement')

        head = 'INSERT INTO `%s` (%s) VALUES ' % (
            self.name, ', '.join(columns))

        records: List[str] = []

        for record in values:
            records.append('(%s)' % (', '.join(Util.escape_list(record))))

        stmt = head + ', '.join(records)

        return self.execute_sql(stmt)

    def list_all(self) -> list:
        '''
        Returns all records in the table

        Returns
        -------
        records : List[Tuple[str | int]]
            List of tuples containing all records in the table
        '''

        stmt = 'SELECT * FROM `%s`' % self.name
        results = self.execute_sql(stmt)

        return list(results)

    def select_where(self, pairs: Dict[str, Union[str, int]]) -> list:
        '''
        Executes a WHERE SQL query using key-value pairs
        provided in the parameters

        Parameters
        ----------
        pairs : Dict[str, str | int]
            Key-value pair of conditional matchers for
            select statement

        Returns
        -------
        records : List[Tuple[str | int]]
            List of tuples containing the selected records
        '''

        conditions = []

        for column in pairs:
            conditions.append(
                '%s = %s' %
                (column, Util.escape_value(pairs[column]))
            )

        stmt = 'SELECT * FROM `%s` WHERE ' % (self.name) + \
            ' AND '.join(conditions)

        results = self.execute_sql(stmt)

        return list(results)

    def custom_command(self, stmt: str) -> sqlite3.Cursor:
        '''
        Exectutes a custom SQL query for use-cases
        that require more granular control of the 
        database.

        Parameters
        ----------
        stmt : str
            Custom SQL statement to execute

        Returns
        -------
        cursor : sqlite3.Cursor
            SQLite3 database cursor
        '''

        return self.execute_sql(stmt)

    def execute_sql(self, stmt: str) -> sqlite3.Cursor:
        '''
        Executes an SQL query on the database.
        (This is a wrapper for the execute command
        on the connection object).

        Parameters
        ----------
        stmt : str
            Custom SQL statement to execute

        Returns
        -------
        cursor : sqlite3.Cursor
            SQLite3 database cursor
        '''

        return self.connection.execute_sql(stmt)


class TableTest(unittest.TestCase):
    def setUp(self) -> None:
        '''
        Put testing requirements in place
        '''

        self.database = Database('test.db', drop=True)
        self.table = Table(self.database, 'person', {
            'ID': 'INTEGER PRIMARY KEY',
            'NAME': 'VARCHAR(45)'
        })

        self.table.initialize()

    def tearDown(self) -> None:
        '''
        Clean up after tests
        '''

        self.database.drop()

    def test_initialize_and_check_exists(self) -> None:
        '''
        Test Table `initialize` and `check_exists` methods
        '''

        database = Database('test_2.db', drop=True)
        table = Table(database, 'test', {
            'ID': 'INTEGER PRIMARY KEY',
        })

        # Table should not exist in the database initally
        self.assertFalse(table.check_exists())

        table.initialize()
        self.assertTrue(table.check_exists())

        database.drop()

    def test_insert(self) -> None:
        '''
        Test Table `insert` method
        '''

        self.assertEqual(len(self.table.list_all()), 0)

        ids = [1, 2, 3, 4]

        self.table.insert(('ID',), [(i,) for i in ids])
        self.assertEqual(len(self.table.list_all()), len(ids))

        self.table.insert(('ID',), [(5,)])
        self.assertEqual(len(self.table.list_all()), len(ids) + 1)

        with self.assertRaises(Exception):
            # Fail for incorrect column names
            self.table.insert(('X',), [(i,) for i in ids])

    def test_list_all(self) -> None:
        '''
        Test Table `list_all` method
        '''

        self.assertEqual(len(self.table.list_all()), 0)

        people = [(1, None), (2, None), (3, 'Sam')]
        self.table.insert(('ID', 'name'), people)

        records = self.table.list_all()
        self.assertEqual(len(people), len(records))

        for i, (_id, name) in enumerate(people):
            self.assertEqual(_id, records[i][0])
            self.assertEqual(name, records[i][1])

    def test_select_where(self) -> None:
        '''
        Test Table `select_where` method
        '''

        people = [(1, None), (2, 'Jennifer')]
        self.table.insert(('ID', 'name'), people)

        records = self.table.select_where({
            'name': people[1][1]
        })

        self.assertEqual(len(records), 1)
        self.assertEqual(people[1][0], records[0][0])
        self.assertEqual(people[1][1], records[0][1])

    def test_custom_command(self) -> None:
        '''
        Test Table `custom_command` method
        '''

        table_name = 'X'
        self.table.custom_command(
            'CREATE TABLE %s (NAME VARCHAR PRIMARY KEY)' % table_name
        )

        names = ['Queen', 'Slim']
        self.table.custom_command(
            'INSERT INTO %s (NAME) VALUES (%s), (%s)' %
            (table_name, Util.escape_value(
                names[0]), Util.escape_value(names[1]))
        )

        results = list(self.table.custom_command(
            'SELECT COUNT(NAME) FROM %s' % table_name
        ))

        count = results[0][0]
        self.assertEqual(len(names), count)


class Database:
    def __init__(self, name: str = 'Library.db', drop: bool = False) -> None:
        if (drop):
            try:
                os.remove(name)
            except Exception:
                pass

        self.name = name
        self.connection = sqlite3.connect(name)

        # Create table for books (unique by ISBN)
        self.books = Table(self, 'book', {
            'ISBN': 'CHAR(13) PRIMARY KEY',
            'title': 'VARCHAR(75)',
            'author': 'VARCHAR(45)'
        })

        # Create table for book copies (unique by serial book id)
        self.book_copies = Table(self, 'book_copy', {
            'book_id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
            'ISBN': 'CHAR(13)'
        })

        # Create table for transactions (unique by serial transaction id)
        self.transactions = Table(self, 'transaction', {
            'transaction_id': 'INTEGER PRIMARY KEY AUTOINCREMENT',
            'book_id': 'INTEGER',
            'checkout_date': 'DATETIME',
            'return_date': 'DATETIME',
            'member_id': 'INTEGER'
        })

    def initialize(self) -> None:
        '''
        Initializes all component tables.
        '''

        self.books.initialize()
        self.book_copies.initialize()
        self.transactions.initialize()

    def execute_sql(self, stmt: str) -> sqlite3.Cursor:
        '''
        Executes an SQL query on the database.
        (This is a wrapper for the execute command
        on the connection object).
        '''

        results = self.connection.execute(stmt)
        self.connection.commit()

        return results

    def drop(self) -> bool:
        '''
        Closes the database connection
        and deletes the database file.
        '''

        self.connection.close()

        if (not os.path.isfile(self.name)):
            return False

        os.remove(self.name)
        return True


class DatabaseTest(unittest.TestCase):
    def test_initialize(self) -> None:
        '''
        Test Database `initialize` method
        '''

        database = Database('test.db', drop=True)
        self.assertIsInstance(database.books, Table)
        self.assertIsInstance(database.book_copies, Table)
        self.assertIsInstance(database.transactions, Table)

        self.assertFalse(database.books.check_exists())
        self.assertFalse(database.book_copies.check_exists())
        self.assertFalse(database.transactions.check_exists())

        database.initialize()
        self.assertTrue(database.books.check_exists())
        self.assertTrue(database.book_copies.check_exists())
        self.assertTrue(database.transactions.check_exists())

        database.drop()

    def test_execute_sql(self) -> None:
        '''
        Test Database `execute_sql` method
        '''

        database = Database('test.db', drop=True)
        database.initialize()

        book_agot = (
            '9780553381689',
            'A Game of Thrones (A Song of Ice and Fire, Book 1)',
            'Martin, George R. R.'
        )

        book_tlt = (
            '9780786838653',
            'The Lightning Thief (Percy Jackson and the Olympians, Book 1)',
            'Riordan, Rick'
        )

        database.books.insert(
            ('ISBN', 'title', 'author'),
            [book_agot, book_tlt]
        )

        database.book_copies.insert(
            ('ISBN',),
            [(book_agot[0],), (book_agot[0],)]
        )

        books = list(
            database.execute_sql('\
            SELECT * FROM `book` WHERE EXISTS (\
                SELECT * FROM `book_copy` WHERE ISBN=book.ISBN\
            )')
        )

        self.assertEqual(len(books), 1)
        self.assertEqual(books[0][0], book_agot[0])
        self.assertEqual(books[0][1], book_agot[1])
        self.assertEqual(books[0][1], book_agot[2])

    def test_drop(self) -> None:
        '''
        Test Database `drop` method
        '''

        database = Database('random', drop=True)

        self.assertTrue(database.drop())
        self.assertFalse(database.drop())


if (__name__ == '__main__'):
    print('[~] Testing database module')
    unittest.main()

else:
    database = Database()
    database.initialize()
