import os
from dotenv import load_dotenv

load_dotenv()

ES_HOST = os.getenv('ES_HOST')
ES_INDEX = os.getenv('ES_INDEX')
AWS_ACCESS_ID = os.getenv('AWS_ACCESS_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION')
