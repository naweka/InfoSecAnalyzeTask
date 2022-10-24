import os
from dotenv import load_dotenv
load_dotenv()


DATABASE_ADDRESS = os.getenv('DATABASE_ADDRESS')
DATABASE_USER = os.getenv('DATABASE_USER')
DATABASE_PASSWORD = os.getenv('DATABASE_PASSWORD')