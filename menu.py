from booklist import list_books


def main():
    books = [str(book) for book in list_books()]
    print(',\n'.join(books))


main()
