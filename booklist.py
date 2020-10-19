"""
Book List Module
~~~~~~~~~~~~~~~~
A Python module which contains functions used to list the book
titles (not book copies) in the popularity order and appropriately
visualise the list by using the Python module Matplotlib
"""

__title__ = 'booklist'
__author__ = 'Chidiebere Okpoechi (Chidi)'
__version__ = '1.0.0'

from typing import List, Tuple

from database import database


class Book:
    def __init__(self, ISBN: str, title: str, author: str) -> None:
        self.ISBN = ISBN
        self.title = title
        self.author = author

    def __str__(self) -> str:
        return '''Book {
    ISBN: %s,
    title: %s,
    author: %s
}''' % (self.ISBN, self.title, self.author)

    @classmethod
    def from_record(cls, record: Tuple[str, str, str]):
        '''
        Creates a book object from a book tuple
        of format ( ISBN, title, author )

        Parameters
        ----------
        record : Tuple[str, str, str]
            Book record

        Returns
        -------
        book : Book
            Book object
        '''

        ISBN, title, author = record
        return cls(ISBN, title, author)

    @classmethod
    def from_record_list(cls, record_list:
                         List[Tuple[str, str, str]]):
        '''
        Creates a list of book objects from a list of book tuples
        of format ( ISBN, title, author )

        Parameters
        ----------
        record_list : List[Tuple[str, str, str]]
            List of book records

        Returns
        -------
        books : List[Book]
            Book objects
        '''

        return list(map(cls.from_record, record_list))


class BookCopy:
    def __init__(self, _id: int, book: Book) -> None:
        self.id = _id
        self.book = book

    def __str__(self) -> str:
        return '''BookCopy {
    id: %d,
    ISBN: %s,
    title: %s,
    author: %s
}''' % (self.id, self.book.ISBN, self.book.title, self.book.author)

    @classmethod
    def from_record(cls, record: Tuple[int], book: Book):
        '''
        Creates a book copy object from a book tuple
        of format ( id, ... ) and a book object

        Parameters
        ----------
        record : Tuple[int]
            Book record

        book : Book
            Book object

        Returns
        -------
        book_copy : BookCopy
            Book copy object
        '''

        _id = record[0]

        return cls(_id, book)

    @classmethod
    def from_record_list(cls, record_list:
                         List[Tuple[int]], book: Book):
        '''
        Creates a book copy object from a list of book tuples
        of format ( id, ... ) and a book object

        Parameters
        ----------
        record_list : List[Tuple[int]]
            Book record

        book : Book
            Book object

        Returns
        -------
        book_copies : List[BookCopy]
            List of book copy objects
        '''

        return list(
            map(lambda record: cls.from_record(record, book), record_list)
        )


def list_books() -> List[Book]:
    '''
    Retrieves the list of books in the database

    Returns
    -------
    books : List[Book]
        List of books
    '''

    records = database.books.list_all()

    return Book.from_record_list(records)


def list_copies_of(book: Book) -> List[BookCopy]:
    '''
    Retrieves the copies of a book in the database

    Returns
    -------
    book_copies : List[BookCopy]
        List of copies for book
    '''

    records = database.book_copies.select_where({
        'ISBN': book.ISBN,
    })

    return BookCopy.from_record_list(records, book)
