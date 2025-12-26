import sys
import os
import pandas as pd
from pymongo import MongoClient, UpdateOne

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.config import settings
from core.ml_logic import DiamondRecommender

def run_batch_update():
    print("⏳ Starting Batch Update Process...")
    
    # 2. Connect to DB
    try:
        client = MongoClient(settings.MONGO_URI)
        db = client[settings.DB_NAME]
        collection = db[settings.COLLECTION_NAME]
        
        # 3. Fetch Data (Only fields needed for ML + ID)
        print("📥 Fetching inventory from MongoDB...")
        
        # We need to map your config weights to the keys present in DB
        # Assuming DB keys are: priceListUSD, weight, depthPerc, tablePerc, etc.
        projection = {
            "_id": 1,
            "priceListUSD": 1, 
            "weight": 1,
            "depthPerc": 1,
            "tablePerc": 1,
            "color": 1,
            "clarity": 1,
            "cutGrade": 1,
            "polish": 1, 
            "symmetry": 1
        }
        
        cursor = collection.find({}, projection)
        df = pd.DataFrame(list(cursor))
        
        if df.empty:
            print("⚠️ Database is empty. Nothing to do.")
            return

        print(f"🧠 Training model on {len(df)} diamonds...")

        # 4. Initialize & Fit ML Model
        recommender = DiamondRecommender()
        
        # This fits the model AND returns the indices of neighbors
        # We access the internal logic manually for efficiency
        ids, indices = recommender.find_similar(df)

        print("⚡ Preparing Bulk Update Operations...")

        # 5. Prepare Bulk Writes
        operations = []
        for i, neighbor_indices in enumerate(indices):
            # The current diamond's ID
            current_id = ids[i]
            
            # The neighbors (Slice [1:] to skip the diamond itself)
            # We convert ObjectId -> str for the Frontend to use easily
            similar_ids = [str(ids[n]) for n in neighbor_indices[1:]]
            
            # Create Update Operation
            op = UpdateOne(
                {"_id": current_id},
                {"$set": {"similar_diamonds": similar_ids}}
            )
            operations.append(op)

        # 6. Execute Bulk Write
        if operations:
            print(f"🚀 Writing {len(operations)} updates to MongoDB...")
            result = collection.bulk_write(operations)
            print(f"✅ DONE! Matched: {result.matched_count} | Modified: {result.modified_count}")
        else:
            print("⚠️ No operations generated.")

    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    # Test run
    run_batch_update()