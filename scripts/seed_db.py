import sys
import os
from pymongo import MongoClient


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.config import settings

#This file is used to migrate data from a client's live database to local database for testing and ML purposes.
CLIENT_URI = ""
CLIENT_DB_NAME = ""
CLIENT_COLLECTION = ""

LOCAL_URI = settings.MONGO_URI
LOCAL_DB_NAME = settings.DB_NAME

DEST_ACTIVE = ""          
DEST_BACKUP = ""   # For Safety

def migrate_data():
    print("🚀 Starting Data Migration...")
    
    print(f"🔌 Connecting to Client DB ({CLIENT_DB_NAME})...")
    try:
        client_src = MongoClient(CLIENT_URI)
        db_src = client_src[CLIENT_DB_NAME]
        col_src = db_src[CLIENT_COLLECTION]
        
        count = col_src.count_documents({})
        print(f"✅ Found {count} diamonds in client's inventory.")
        
        if count == 0:
            print("❌ Client collection is empty. Aborting.")
            return

        print("📥 Downloading data...")
        data = list(col_src.find({}))
        print("✅ Download complete.")
        
    except Exception as e:
        print(f"❌ Failed to connect to Client DB: {e}")
        return

    print(f"\n🔌 Connecting to Local DB ({LOCAL_DB_NAME})...")
    try:
        client_dest = MongoClient(LOCAL_URI)
        db_dest = client_dest[LOCAL_DB_NAME]
        
        print(f"💾 Saving to Active Collection: '{DEST_ACTIVE}'...")
        col_active = db_dest[DEST_ACTIVE]
        col_active.drop() # Clear old test data
        col_active.insert_many(data)
        print(f"✅ Inserted {len(data)} records into '{DEST_ACTIVE}'.")

        print(f"🔒 Saving to Backup Collection: '{DEST_BACKUP}'...")
        col_backup = db_dest[DEST_BACKUP]
        col_backup.drop() # Clear old backup
        col_backup.insert_many(data)
        print(f"✅ Inserted {len(data)} records into '{DEST_BACKUP}'.")

    except Exception as e:
        print(f"❌ Failed to write to Local DB: {e}")
    finally:
        client_src.close()
        client_dest.close()
        print("\n✨ Migration Finished Successfully!")

if __name__ == "__main__":
    migrate_data()