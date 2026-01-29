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
        # source_collection = db[settings.COLLECTION_NAME]  # where data is fetched from
        # target_collection = db["diamonds"]  # where results go

        # 3. Fetch Data (Only fields needed for ML + ID)
        print("Fetching inventory from MongoDB...")

        projection = {
            "_id": 1,
            "stockRef": 1,  # Add stockRef to projection
            "shape": 1,  
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
        # cursor = source_collection.find({}, projection)
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

        # Track single-diamond shapes for fallback processing
        single_diamond_shapes = []

        for shape in shapes:
            # Filter diamonds by current shape
            shape_df = df[df["shape"] == shape].reset_index(drop=True)

            if len(shape_df) < 2:
                # Store for later - will use cross-shape recommendations
                single_diamond_shapes.append(shape)
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

        # Handle single-diamond shapes with cross-shape fallback
        if single_diamond_shapes:
            print(
                f"\n📌 Processing {len(single_diamond_shapes)} single-diamond shapes with cross-shape fallback..."
            )

            # Get all single diamonds
            single_diamonds_df = df[
                df["shape"].isin(single_diamond_shapes)
            ].reset_index(drop=True)

            # Use the full dataset (excluding themselves) for recommendations
            for idx, row in single_diamonds_df.iterrows():
                current_stock_ref = row["stockRef"]
                current_shape = row["shape"]

                # Find similar from ALL other diamonds (cross-shape)
                other_diamonds = df[df["stockRef"] != current_stock_ref].reset_index(
                    drop=True
                )

                if len(other_diamonds) < 1:
                    print(f"⚠️ No other diamonds to recommend for '{current_stock_ref}'")
                    continue

                # Fit model on all other diamonds and find nearest neighbors
                ids, indices = recommender.find_similar(other_diamonds)

                # Get the features for the current diamond to find its neighbors
                # We need to find the nearest neighbors for this single diamond
                from sklearn.preprocessing import StandardScaler

                # Reuse the recommender to get neighbors for single diamond
                # First fit on other diamonds, then query for this diamond
                other_stock_refs = other_diamonds["stockRef"].tolist()

                # Get top 10 similar (already excludes self since we removed it from dataset)
                similar_stock_refs = [other_stock_refs[n] for n in indices[0][:10]]

                op = UpdateOne(
                    {"stockRef": current_stock_ref},
                    {"$set": {"similar_diamonds": similar_stock_refs}},
                )
                operations.append(op)
                print(
                    f"   ✅ '{current_shape}' diamond ({current_stock_ref}) -> {len(similar_stock_refs)} cross-shape recs"
                )

        print("\nPreparing Bulk Update Operations...")

        # 6. Execute Bulk Write
        if operations:
            print(f"🚀 Writing {len(operations)} updates to MongoDB...")
            result = collection.bulk_write(operations)
            # result = target_collection.bulk_write(operations)
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
