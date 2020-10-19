"""
Book Search Module
~~~~~~~~~~~~~~~~~~
Module providing function for searching for books
in the library database
"""

__title__ = 'booksearch'
__author__ = 'Chidiebere Okpoechi (Chidi)'
__version__ = '1.0.0'

import unittest
from typing import List

from booklist import Book
from database import Database, database


def search_book(search_string: str,
                database: Database = database) -> List[Book]:
    '''
    Performs a search on the available books in the database
    based on any of the following properties of the book:

    - title
    - author

    Parameters
    ----------
    search_string : str
        String value to search the books for

    database : Database
        Database object to access

    Returns
    -------
    books : List[Book]
        List of book objects
    '''

    like_matcher = r'%{}%'.format(search_string)

    records = database.books.select_where(pairs={
        'title': like_matcher,
        'author': like_matcher
    },  op="LIKE", sep="OR")

    return Book.from_record_list(records)


class BookSearchTest(unittest.TestCase):
    def test_search_book(self):
        '''
        Test `search_book` function
        '''

        test_database = Database('test_library.db', drop=True)
        test_database.initialize()

        book_tuples = [
            ('1000000000001', 'Book 1', 'Alpha'),
            ('1000000000002', 'Book 2', 'Bravo'),
            ('1000000000003', 'Book 3', 'Charlie'),
        ]

        test_database.books.insert(
            ('ISBN', 'title', 'author'),
            book_tuples,
        )

        books = search_book('Book', test_database)
        self.assertEqual(len(books), len(book_tuples))

        books = search_book('l', test_database)
        self.assertEqual(len(books), 2)

        books = search_book('Some name', test_database)
        self.assertEqual(len(books), 0)

        test_database.drop()


if (__name__ == "__main__"):
    print('[~] Testing database module')
    unittest.main()
