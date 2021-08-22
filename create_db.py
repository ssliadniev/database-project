from argparse import ArgumentParser
from typing import List, Optional, Tuple, Union

import psycopg2
from pandas import DataFrame
from psycopg2 import DatabaseError, sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def get_parser() -> ArgumentParser:
    parser = ArgumentParser()
    parser.add_argument("--db_name", type=str, required=True, help="Database name.")
    parser.add_argument(
        "--db_user",
        type=str,
        default="postgres",
        help="Username to connect to the database.",
    )
    parser.add_argument(
        "--db_password",
        type=str,
        required=True,
        help="Password to connect to the database.",
    )
    parser.add_argument(
        "--db_host",
        type=str,
        default="localhost",
        help="The name of the server or IP address.",
    )
    parser.add_argument(
        "--db_port",
        type=str,
        default="5432",
        help="Port for connecting to the database.",
    )
    parser.add_argument(
        "--insert_data",
        type=str,
        choices=[True, False],
        default=False,
        help="Populate tables with generated data or not.",
    )
    return parser


def create_connection_to_POSTGRES(
    db_user: str, db_password: str, db_host: str, db_port: str, db_name: str = None
) -> Union[None, psycopg2.extensions.connection]:
    """A function to create a connection to a PostgreSQL server/database.

    Parameters:
    ----------
        - db_user: type[`str`]
        Username to connect to the server.
        - db_password: type[`str`]
        Password to connect to the server.
        - db_host: type[`str`]
        The name of the server or IP address that the database is running on.
        - db_port: type[`str`]
        Port for connecting to the server.
        -db_name: type[`str`]
        Database name.

    Returns:
    -------
        type[`psycopg2.extensions.connection`]
        A connection object to a PostgreSQL.
    """

    if db_name is None:
        print("Connection to PostgreSQL...")
    else:
        print(f"Connecting to Postgres database |{db_name}|...")
    try:
        connection = psycopg2.connect(
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            connect_timeout=10,
            database=db_name,
        )
        if db_name is None:
            print("Connection to PostgreSQL successful.")
        else:
            print("Connection to Postgres database successful.")
    except (Exception, DatabaseError) as error:
        print(error)
        raise SystemExit
    else:
        return connection


def check_if_exists_database(connection, db_name: str) -> Union[None, bool]:
    """A function to check for the existence of a specified database.

    Parameters:
    ----------
        - connection: type[`psycopg2.extensions.connection`]
        A connection object to PostgreSQL.
        - db_name: type[`str`]
        A database name.

    Returns:
    -------
        type[`Union[None, bool]`]:
        - None if an exception was thrown
        - bool: True if database is exist, False if not
    """

    check_query_database: str
    list_databases: List

    check_query_database = "SELECT datname FROM pg_database"

    try:
        cursor = connection.cursor()
        cursor.execute(check_query_database)
        list_databases = cursor.fetchall()
    except (Exception, DatabaseError) as error:
        print(error)
        raise SystemExit
    else:
        return (db_name,) in list_databases


def create_database(
    db_name: str, connection: psycopg2.extensions.connection
) -> Union[bool, None]:
    """Database creation in PostgreSQL.
    Parameters:
    ----------
        - db_name: type[`str`]
        A database name.
        - connection: type[`psycopg2.extensions.connection`]
        A connection object to a PostgreSQL database instance.

    Returns:
    -------
        type[`bool`]: whether the database was created or not.
    """

    cursor: psycopg2.extensions.cursor

    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()

    try:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        print("The database has been created successfully.")
    except (Exception, DatabaseError) as error:
        print(error)
        raise SystemExit
    else:
        connection.commit()
        return True


def check_if_exists_tables(connection, db_tables: Tuple[str]) -> Union[None, bool]:
    check_query_tables: str
    list_tables: List

    check_query_tables = """SELECT table_name FROM information_schema.tables
                            WHERE table_schema='public' ORDER BY table_name"""

    try:
        cursor = connection.cursor()
        cursor.execute(check_query_tables)
        list_tables = cursor.fetchall()
    except (Exception, DatabaseError) as error:
        print(error)
        raise SystemExit

    return sorted(db_tables) == sorted(list_tables)


def create_tables(connection, db_tables: str) -> None:
    """A function to create database tables.
    The function creates seven tables: `users`, `carts`, `categories`,
    `products`, `cart_product`, `orders`.

    Parameters:
    ----------
        - connection: `psycopg2.extensions.connection`.
        A connection object to a PostgreSQL database instance.
        - db_tables: `List[str]`.
        A tuple of database tables.
    """

    cursor: psycopg2.extensions.cursor
    commands: Tuple[str, ...]

    commands = (
        """ 
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER NOT NULL PRIMARY KEY
                GENERATED ALWAYS AS IDENTITY,
            email VARCHAR(255) NOT NULL,
            password VARCHAR(255) NOT NULL,
            first_name VARCHAR(255) NOT NULL,
            last_name VARCHAR(255) NOT NULL,
            gender VARCHAR(45),
            is_staff SMALLINT,
            country VARCHAR(255),
            city VARCHAR(255),
            address TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS categories (
            category_id INTEGER  NOT NULL PRIMARY KEY
                GENERATED ALWAYS AS IDENTITY,
            category_title VARCHAR(255) NOT NULL,
            category_description TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS carts (
            cart_id INTEGER NOT NULL PRIMARY KEY
                GENERATED ALWAYS AS IDENTITY,
            Users_user_id INTEGER NOT NULL,
            subtotal DECIMAL NOT NULL,
            total DECIMAL NOT NULL,
            created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            FOREIGN KEY (Users_user_id) REFERENCES users (user_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER NOT NULL PRIMARY KEY,
            Users_user_id INTEGER NOT NULL,
            Carts_cart_id INTEGER NOT NULL,
            status_name VARCHAR(255) NOT NULL,
            shipping_total DECIMAL NOT NULL,
            total DECIMAL NOT NULL,
            created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            FOREIGN KEY (Users_user_id) REFERENCES
                users (user_id),
            FOREIGN KEY (Carts_cart_id) REFERENCES carts (cart_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER NOT NULL PRIMARY KEY
                GENERATED ALWAYS AS IDENTITY,
            product_title VARCHAR(255) NOT NULL,
            product_description TEXT,
            in_stock INTEGER NOT NULL,
            slug VARCHAR(45) NOT NULL,
            price REAL NOT NULL,
            Category_category_id INTEGER NOT NULL,
            Orders_order_id INTEGER NOT NULL,
            FOREIGN KEY (Category_category_id) REFERENCES categories (category_id),
            FOREIGN KEY (Orders_order_id) REFERENCES orders (order_id)
        )
        """,
        """
            CREATE TABLE IF NOT EXISTS cart_product (
                Carts_cart_id INTEGER NOT NULL,
                Products_product_id INTEGER NOT NULL,
                FOREIGN KEY (Carts_cart_id) REFERENCES carts (cart_id),
                FOREIGN KEY (Products_product_id) REFERENCES products (product_id)
            )
        """,
    )

    try:
        cursor = connection.cursor()
        for command, table in zip(commands, db_tables):
            print(f"Creating a table |{table}|...")
            cursor.execute(command)
            print("The table was created successfully.")
        connection.commit()
    except (Exception, DatabaseError) as error:
        print(error)


def insert_data_to_db(connection: psycopg2.extensions.connection, table: str) -> None:
    """A function for loading data from csv files into database tables.
    Parameters:
    ----------
        - connection: `psycopg2.extensions.connection`.
        A connection object to a PostgreSQL database instance.
        - table: `str`.
        Database table name.
    """

    path = f"sql_input_files/{table}.csv"

    print("Loading data into database tables...")
    with connection.cursor() as cursor:
        with open(path, "r") as f:
            try:
                cursor.copy_from(f, table, sep=",")
                print(f"Data from |{table}| loaded successfully.")
            except (Exception, DatabaseError) as error:
                print(error)

    connection.commit()


def first_db_request(connection) -> DataFrame:
    first_select_query: str

    first_select_query = """
        SELECT first_name,
               last_name,
               (SELECT COUNT(products_product_id) FROM cart_product
               WHERE cart_product.carts_cart_id = users.user_id)
        FROM users
    """

    try:
        cursor = connection.cursor()
        cursor.execute(first_select_query)
        users_data = cursor.fetchall()
    except (Exception, DatabaseError) as error:
        print(error)
        raise SystemExit

    users_df = DataFrame(
        users_data,
        columns=["First name", "Last name", "Number of products in the cart"],
    )
    return users_df


def main():
    args = get_parser().parse_args()

    db_name: str = args.db_name
    db_user: str = args.db_user
    db_password: str = args.db_password
    db_host: str = args.db_host
    db_port: str = args.db_port

    insert_data: bool = args.insert_data

    db_tables: Tuple = (
        "users",
        "categories",
        "carts",
        "orders",
        "products",
        "cart_product",
    )

    connection = create_connection_to_POSTGRES(db_user, db_password, db_host, db_port)

    print("--" * 25)
    print(f"Creating a database |{db_name}|...")
    if not check_if_exists_database(connection, db_name):
        create_database(db_name, connection)
    else:
        print(f"Database |{db_name}| already exists.")

    print("--" * 25)
    connection = create_connection_to_POSTGRES(
        db_user, db_password, db_host, db_port, db_name
    )

    print("--" * 25)
    print("Checking for tables in the database...")
    if check_if_exists_tables(connection, db_tables) is not None:
        print("All tables are exists.")
    else:
        print("--" * 25)
        print("Creating tables...")
        create_tables(connection, db_tables)

    print("--" * 25)
    if insert_data:
        for table in db_tables:
            insert_data_to_db(connection, table)

    print(first_db_request(connection).head(20))


if __name__ == "__main__":
    main()
