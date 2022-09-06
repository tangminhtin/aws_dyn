import logging
from pprint import pprint
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class Books:
    def __init__(self, dyn_resource):
        self.dyn_resource = dyn_resource
        self.table = None

    def exists(self, table_name):
        """
        Determines whether a table exists. As a side effect, stores the table in
        a member variable.

        :param table_name: The name of the table to check.
        :return: True when the table exists; otherwise, False.
        """
        try:
            table = self.dyn_resource.Table(table_name)
            table.load()
            exists = True
        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                exists = False
            else:
                logger.error(
                    "Couldn't check for existence of %s. Here's why: %s: %s",
                    table_name,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        else:
            self.table = table
        return exists

    def create_table(self, table_name):
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'isbn', 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': 'title', 'KeyType': 'RANGE'}  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'isbn', 'AttributeType': 'S'},
                    {'AttributeName': 'title', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10})
            self.table.wait_until_exists()
        except ClientError as err:
            logger.error(
                "Couldn't create table %s. Here's why: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return self.table

    def list_tables(self):
        try:
            tables = []
            for table in self.dyn_resource.tables.all():
                print(table.name)
                tables.append(table)
        except ClientError as err:
            logger.error(
                "Couldn't list tables. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return tables

    def write_batch(self, books):
        try:
            with self.table.batch_writer() as writer:
                for book in books:
                    writer.put_item(Item=book)
        except ClientError as err:
            logger.error(
                "Couldn't load data into table %s. Here's why: %s: %s", self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def add_book(self, isbn, title, author, pages):
        try:
            self.table.put_item(
                Item={
                    'isbn': isbn,
                    'title': title,
                    'author': author,
                    'pages': pages
                })
        except ClientError as err:
            logger.error(
                "Couldn't add book %s to table %s. Here's why: %s: %s",
                title, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def get_book(self, isbn, title):
        try:
            response = self.table.get_item(Key={'isbn': isbn, 'title': title})
        except ClientError as err:
            logger.error(
                "Couldn't get book %s from table %s. Here's why: %s: %s",
                title, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Item']

    def update_book(self, isbn, title, author, pages):
        try:
            response = self.table.update_item(
                Key={'isbn': isbn, 'title': title},
                UpdateExpression="set author=:a, pages=:p",
                ExpressionAttributeValues={
                    ':a': author, ':p': pages},
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.error(
                "Couldn't update book %s in table %s. Here's why: %s: %s",
                title, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Attributes']

    def query_book(self, title):
        try:
            response = self.table.query(
                KeyConditionExpression=Key('title').eq(title))
        except ClientError as err:
            logger.error(
                "Couldn't query for books released in %s. Here's why: %s: %s", title,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Items']

    def delete_book(self, isbn, title):
        try:
            self.table.delete_item(Key={'isbn': isbn, 'title': title})
        except ClientError as err:
            logger.error(
                "Couldn't delete book %s. Here's why: %s: %s", title,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def delete_table(self):
        try:
            self.table.delete()
            self.table = None
        except ClientError as err:
            logger.error(
                "Couldn't delete table. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise


def run_scenario(table_name, dyn_resource):
    logging.basicConfig(level=logging.INFO,
                        format='%(levelname)s: %(message)s')
    print('-'*88)
    print("Welcome to the Amazon DynamoDB getting started demo.")
    print('-'*88)

    books = Books(dyn_resource)
    books_exists = books.exists(table_name)
    if not books_exists:
        print(f"\nCreating table {table_name}...")
        books.create_table(table_name)
        print(f"\nCreated table {books.table.name}.")

    my_book = {
        "isbn": "9781593279509",
        "title": "Eloquent JavaScript, Third Edition",
        "author": "Marijn Haverbeke",
        "pages": 472,
    }
    books.add_book(**my_book)
    print(f"\nAdded '{my_book['title']}' to '{books.table.name}'.")
    print('-'*88)

    book_update = {
        "isbn": "8888893299999",
        "title": "Django JavaScript, Third Edition",
        "author": "John",
        "pages": 1000,
    }
    my_book.update(book_update)
    updated = books.update_book(**my_book)
    print(f"\nUpdated '{my_book['title']}' with new attributes:")
    pprint(updated)
    print('-'*88)

    books.write_batch([
        {
            "isbn": "9781593279509",
            "title": "Eloquent JavaScript, Third Edition",
            "subtitle": "A Modern Introduction to Programming",
            "author": "Marijn Haverbeke",
            "published": "2018-12-04T00:00:00.000Z",
            "publisher": "No Starch Press",
            "pages": 472,
            "description": "JavaScript lies at the heart of almost every modern web application, from social apps like Twitter to browser-based game frameworks like Phaser and Babylon. Though simple for beginners to pick up and play with, JavaScript is a flexible, complex language that you can use to build full-scale applications.",
            "website": "http://eloquentjavascript.net/"
        },
        {
            "isbn": "9781491943533",
            "title": "Practical Modern JavaScript",
            "subtitle": "Dive into ES6 and the Future of JavaScript",
            "author": "Nicolás Bevacqua",
            "published": "2017-07-16T00:00:00.000Z",
            "publisher": "O'Reilly Media",
            "pages": 334,
            "description": "To get the most out of modern JavaScript, you need learn the latest features of its parent specification, ECMAScript 6 (ES6). This book provides a highly practical look at ES6, without getting lost in the specification or its implementation details.",
            "website": "https://github.com/mjavascript/practical-modern-javascript"
        },
        {
            "isbn": "9781593277574",
            "title": "Understanding ECMAScript 6",
            "subtitle": "The Definitive Guide for JavaScript Developers",
            "author": "Nicholas C. Zakas",
            "published": "2016-09-03T00:00:00.000Z",
            "publisher": "No Starch Press",
            "pages": 352,
            "description": "ECMAScript 6 represents the biggest update to the core of JavaScript in the history of the language. In Understanding ECMAScript 6, expert developer Nicholas C. Zakas provides a complete guide to the object types, syntax, and other exciting changes that ECMAScript 6 brings to JavaScript.",
            "website": "https://leanpub.com/understandinges6/read"
        }])

    book = books.get_book("9781593277574", "Understanding ECMAScript 6")
    print("\nHere's what I found:")
    pprint(book)

    # title = "Practical Modern JavaScript"
    # releases = books.query_book(title)
    # if releases:
    #     print(
    #         f"There were {len(releases)} books released in {title}:")
    #     for release in releases:
    #         print(f"\t{release['title']}")
    # else:
    #     print(f"I don't know about any books released in {title}!")

    books.delete_book("9781593277574", "Understanding ECMAScript 6")

    books.delete_table()


if __name__ == '__main__':
    try:
        run_scenario(
            'table-books', boto3.resource('dynamodb', endpoint_url="http://localhost:8000"))
    except Exception as e:
        print(f"Something went wrong with the demo! Here's what: {e}")
