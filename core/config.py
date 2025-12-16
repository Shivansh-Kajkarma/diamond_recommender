import os
from dotenv import load_dotenv
load_dotenv()

class Settings:
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    DB_NAME = os.getenv("DB_NAME", "diamond_inventory")
    COLLECTION_NAME = os.getenv("COLLECTION_NAME", "diamonds_test")
    
    WEIGHT_PRICE = float(os.getenv("WEIGHT_PRICE", "1.5"))
    WEIGHT_CARAT = float(os.getenv("WEIGHT_CARAT", "2.0"))
    WEIGHT_COLOR = float(os.getenv("WEIGHT_COLOR", "1.0"))

    # Project Paths
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DATA_FILE = os.path.join(BASE_DIR, "response.json")

settings = Settings()