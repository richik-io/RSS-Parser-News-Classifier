import pandas as pd
from sqlalchemy import create_engine, Column, String, DateTime, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError

# Define the base class for the ORM
Base = declarative_base()

# Define the table structure as a class
class NewsArticle(Base):
    __tablename__ = 'news_articles'

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False)
    content = Column(String)
    date = Column(String)
    url = Column(String, nullable=False, unique=True)
    category = Column(String)

# MySQL connection URL (replace with your credentials)
db_url = 'mysql+pymysql://root:admin@localhost:3306/NEWS'

def add_article(title, url, content, date, category, session):
    """Insert an article into the database, ignoring duplicates."""
    new_article = NewsArticle(title=title, url=url, content=content, date=date, category=category)

    try:
        # Attempt to add and commit the new article
        session.add(new_article)
        session.commit()
        print(f"Article '{title}' inserted successfully.")
    except IntegrityError:
        # Rollback the session in case of duplicate entry
        session.rollback()
        print(f"Duplicate entry found for URL: {url}. Article not inserted.")
    except Exception as e:
        # Rollback the session in case of other errors
        session.rollback()
        print(f"Error inserting article '{title}': {e}")

# Function to store DataFrame into a database
def store_data_in_db(data, index=0):
    """Store a pandas DataFrame in the database, starting from the given index."""
    # Create a database connection using SQLAlchemy
    engine = create_engine(db_url)
    
    # Create the table if it doesn't exist
    Base.metadata.create_all(engine)

    # Create a session for database operations
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        for i in range(index, len(data['title'])):
            try:
                add_article(
                    title=data['title'][i],
                    url=data['url'][i],
                    content=data['content'][i],
                    date=data['date'][i],
                    category=data['pred'][i],  # Assuming 'pred' is the category
                    session=session
                )
            except Exception as e:
                # Catch any error for this row, rollback and continue to next row
                session.rollback()
                print(f"Skipping row {i} due to error: {e}")
    finally:
        # Close the session after committing all changes
        session.close()
        print("Data processing complete. Session closed.")
        index = len(data['title'])

