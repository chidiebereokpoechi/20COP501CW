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
        (ISBN, title, author) = record
        return cls(ISBN, title, author)

    @classmethod
    def from_record_list(cls, record_list:
                         List[Tuple[str, str, str]]):
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
        _id = record[0]
        return cls(_id, book)

    @classmethod
    def from_record_list(cls, record_list:
                         List[Tuple[int]], book: Book):
        return list(
            map(lambda record: cls.from_record(record, book), record_list)
        )


def list_books():
    records = database.books.list_all()
    return Book.from_record_list(records)


def list_copies_of(book: Book):
    records = database.book_copies.select_where({
        'ISBN': book.ISBN,
    })

    return BookCopy.from_record_list(records, book)


def test():
    books = list_books()
    book = books[0]

    print(book)

    copies = list_copies_of(book)
    copy = copies[0]

    print(copy)


if (__name__ == '__main__'):
    test()
