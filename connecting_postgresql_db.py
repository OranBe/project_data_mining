from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

def create_db_session():
    """
    This function creates a database session using SQLAlchemy
    after loading connection details from environment variables.
    
    Returns:
        session (Session): The SQLAlchemy session object connected to PostgreSQL
    """
    # Load environment variables from .env file
    load_dotenv()

    # Get connection details from environment variables
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME')

    # Create the database connection string
    db_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    # Create the engine to connect SQLAlchemy to PostgreSQL
    engine = create_engine(db_string)

    # Create a session to perform actions on the database
    Session = sessionmaker(bind=engine)
    session = Session()
    
    return session

def execute_pg_query(query):
    """
    Executes a provided SQL query on the PostgreSQL database and returns the raw results.

    Args:
        query (str): The SQL query to execute.

    Returns:
        result: The raw result object from executing the query.
    """
    # Create a database session
    session = create_db_session()

    try:
        # Execute the query and fetch all results
        result = session.execute(text(query))
        return result

    finally:
        # Close the session
        session.close()


def get_data_from_pg_db(query):
    """
    Retrieves data from PostgreSQL database using a provided query and returns it in dictionary format.

    Args:
        query (str): The SQL query to execute.

    Returns:
        list: A list of dictionaries containing the results of the query.
    """
    # Get the raw result from the database
    result = execute_pg_query(query)

    # Convert the result to a list of dictionaries
    columns = result.keys()
    data = result.fetchall()

    return [dict(zip(columns, row)) for row in data]
