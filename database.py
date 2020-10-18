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
from sqlite3 import Cursor
from typing import Collection, Dict, List, Union


class Util:
    @classmethod
    def escape_value(cls, value: Union[str, int, None]) -> str:
        '''
        Return the SQL-escaped string equivalent of a value
        for use in queries
        '''

        if (value == None):
            return 'null'

        if (type(value) == str):
            return '\'%s\'' % value

        return str(value)

    @classmethod
    def escape_list(cls, values: Collection[Union[str, int, None]]) -> List[str]:
        '''
        Return the SQL-escaped string equivalents of
        a list of values for use in queries
        '''

        return [cls.escape_value(value) for value in values]

        # return list(map(cls.escape_value, values))


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
        '''

        stmt = 'SELECT name FROM sqlite_master \
            WHERE type=\'table\' AND name=\'%s\'' % (self.name)
        results = self.execute_sql(stmt)

        return len(list(results)) == 1

    def insert(self, columns: tuple, values: List[tuple]):
        '''
        Inserts one or more records into the table
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
        '''

        stmt = 'SELECT * FROM `%s`' % self.name
        results = self.execute_sql(stmt)

        return list(results)

    def select_where(self, pairs: Dict[str, Union[str, int]]) -> list:
        '''
        Executes a WHERE SQL query using key-value pairs
        provided in the parameters
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

    def custom_command(self, stmt: str):
        '''
        Exectutes a custom SQL query for use-cases
        that require more granular control of the 
        database.
        '''

        return self.execute_sql(stmt)

    def execute_sql(self, stmt: str) -> Cursor:
        '''
        Executes an SQL query on the database.
        (This is a wrapper for the execute command
        on the connection object).
        '''

        return self.connection.execute_sql(stmt)


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

    def initialize(self):
        '''
        Initializes all component tables.
        '''

        self.books.initialize()
        self.book_copies.initialize()
        self.transactions.initialize()

    def execute_sql(self, stmt: str) -> Cursor:
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

        try:
            self.connection.close()
            os.remove(self.name)
            return True
        except Exception as e:
            print('There was an error dropping the database')
            print(e)
            return False


def test():
    '''
    Tests the functions in the database module.
     - `Util` class
     - `Table` class
     - `Database` class
    '''

    ##############################
    # Test suites for Util class
    ##############################
    # Util - Escape value
    # Util - Escape list of values
    ##############################

    value, expected = None, 'null'
    assert Util.escape_value(value) == expected

    value, expected = 'bob', '\'bob\''
    assert Util.escape_value(value) == expected

    value, expected = 12, '12'
    assert Util.escape_value(value) == expected

    values = [None, 'bob', 12]
    expected = ['null', '\'bob\'', '12']
    escaped = Util.escape_list(values)

    # Each value in the escaped list should match the expected
    assert escaped[0] == expected[0]
    assert escaped[1] == expected[1]
    assert escaped[2] == expected[2]

    ###########################################
    # Test suite for Table and Database classes
    ###########################################
    # Database - Connection
    # Table - Check if exists
    # Table - Insert records
    # Table - List all records
    # Table - Select where
    # Table - Custom commands
    # Database - Drop database
    ###########################################

    database = Database('test.db', drop=True)

    # Test that database connection is defined
    # and is a connection instance
    assert database.connection is not None
    assert database.connection.execute is not None

    database.initialize()

    # Test that database object is correctly linked with the tables
    assert database.books.connection == database

    # Test that check exists method works properly
    assert database.books.check_exists() == True

    test_table = Table(database, 'test', {
        'ID': 'INTEGER PRIMARY KEY'
    })

    assert test_table.check_exists() == False

    # Table should exist in database after initialization
    test_table.initialize()
    assert test_table.check_exists() == True

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

    # Insert both books into the database
    database.books.insert(
        ('ISBN', 'title', 'author'),
        [book_agot, book_tlt]
    )

    books = database.books.list_all()

    # Two books were inserted. Two should be retrieved
    assert len(books) == 2

    # First book retrieved should match the first book inserted
    assert books[0][0] == book_agot[0]
    assert books[0][1] == book_agot[1]
    assert books[0][2] == book_agot[2]

    # Second book retrieved should match the second book inserted
    assert books[1][0] == book_tlt[0]
    assert books[1][1] == book_tlt[1]
    assert books[1][2] == book_tlt[2]

    books = database.books.select_where({
        'ISBN': book_agot[0]
    })

    assert len(books) == 1

    assert books[0][0] == book_agot[0]
    assert books[0][1] == book_agot[1]
    assert books[0][2] == book_agot[2]

    database.book_copies.insert(
        ('ISBN',),
        [(book_agot[0],), (book_agot[0],)]
    )

    books = list(database.books.custom_command('\
        SELECT * FROM `book` WHERE EXISTS (\
            SELECT * FROM `book_copy` WHERE ISBN=book.ISBN\
        )'))

    assert len(books) == 1

    assert books[0][0] == book_agot[0]
    assert books[0][1] == book_agot[1]
    assert books[0][1] == book_agot[2]

    database.drop()


if (__name__ == '__main__'):
    print('Testing database module')

    try:
        test()
        print('[~] Database module passed')
    except AssertionError as e:
        print('[X] Database module failed')
        print(e)

else:
    database = Database()
    database.initialize()
