import sys
import os
import pandas as pd
from pymongo import MongoClient, UpdateOne

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.config import settings
from core.ml_logic import DiamondRecommender


def run_batch_update():
    print("Starting Batch Update Process...")

    # 2. Connect to DB
    try:
        client = MongoClient(settings.MONGO_URI)
        db = client[settings.DB_NAME]
        collection = db[settings.COLLECTION_NAME]

        # 3. Fetch Data (Only fields needed for ML + ID)
        print("Fetching inventory from MongoDB...")

        projection = {
            "_id": 1,
            "stockRef": 1,  # Add stockRef to projection
            "shape": 1,  # Add shape for shape-based filtering
            "priceListUSD": 1,
            "weight": 1,
            "depthPerc": 1,
            "tablePerc": 1,
            "color": 1,
            "clarity": 1,
            "cutGrade": 1,
            "polish": 1,
            "symmetry": 1,
        }

        cursor = collection.find({}, projection)
        df = pd.DataFrame(list(cursor))

        if df.empty:
            print("Database is empty. Nothing to do.")
            return

        print(f"Processing {len(df)} diamonds...")

        # 4. Group by shape and process each group separately
        # This ensures recommendations are always within the same shape
        shapes = df["shape"].unique()
        print(f"Found {len(shapes)} unique shapes: {list(shapes)}")

        operations = []
        recommender = DiamondRecommender()

        for shape in shapes:
            # Filter diamonds by current shape
            shape_df = df[df["shape"] == shape].reset_index(drop=True)

            if len(shape_df) < 2:
                print(f"⚠️ Skipping shape '{shape}' - only {len(shape_df)} diamond(s)")
                continue

            print(
                f"Training model for shape '{shape}' with {len(shape_df)} diamonds..."
            )

            # Find similar diamonds within this shape group
            ids, indices = recommender.find_similar(shape_df)
            stock_refs = shape_df["stockRef"].tolist()

            for i, neighbor_indices in enumerate(indices):
                current_stock_ref = stock_refs[i]

                # Filter out self-recommendations explicitly
                similar_stock_refs = [
                    stock_refs[n]
                    for n in neighbor_indices
                    if stock_refs[n] != current_stock_ref
                ][:10]  # Limit to 10 recommendations

                op = UpdateOne(
                    {"stockRef": current_stock_ref},
                    {"$set": {"similar_diamonds": similar_stock_refs}},
                )
                operations.append(op)

        print("Preparing Bulk Update Operations...")

        # 6. Execute Bulk Write
        if operations:
            print(f"🚀 Writing {len(operations)} updates to MongoDB...")
            result = collection.bulk_write(operations)
            print(
                f"✅ DONE! Matched: {result.matched_count} | Modified: {result.modified_count}"
            )
        else:
            print("⚠️ No operations generated.")

    except Exception as e:
        print(f"❌ CRITICAL ERROR: {e}")
    finally:
        if "client" in locals():
            client.close()


if __name__ == "__main__":
    # Test run
    run_batch_update()
