import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
from core.config import settings


class DiamondRecommender:
    def __init__(self):
        self.scaler = StandardScaler()
        # Metric='euclidean' is standard for distance.
        # n_neighbors=11 because we want 10 results, but the 1st one is always the diamond itself.
        self.model = NearestNeighbors(
            metric="euclidean", algorithm="ball_tree", n_neighbors=11
        )
        self.feature_columns = []

    def preprocess(self, df: pd.DataFrame):
        """
        Cleans data and applies weights.
        Returns: (Scaled Feature Matrix, List of IDs)
        """
        df_processed = df.copy()

        # 1. Map Categorical Data to Numbers (Ordinal Encoding)
        # Lower number = "Better" or "Standard Order"
        color_map = {
            "D": 1,
            "E": 2,
            "F": 3,
            "G": 4,
            "H": 5,
            "I": 6,
            "J": 7,
            "K": 8,
            "L": 9,
            "M": 10,
        }
        clarity_map = {
            "FL": 1,
            "IF": 2,
            "VVS1": 3,
            "VVS2": 4,
            "VS1": 5,
            "VS2": 6,
            "SI1": 7,
            "SI2": 8,
            "I1": 9,
            "I2": 10,
        }
        cut_map = {"EX": 1, "VG": 2, "G": 3, "F": 4, "P": 5}

        # Apply maps. Fill unknowns with a high number (penalty).
        df_processed["color_score"] = df_processed["color"].map(color_map).fillna(11)
        df_processed["clarity_score"] = (
            df_processed["clarity"].map(clarity_map).fillna(11)
        )

        # Handle Cut/Polish/Sym. Some datasets use 'cutGrade' instead of 'cut'.
        # We check which one exists.
        cut_col = "cutGrade" if "cutGrade" in df_processed.columns else "cut"
        df_processed["cut_score"] = df_processed.get(cut_col).map(cut_map).fillna(5)
        df_processed["polish_score"] = df_processed["polish"].map(cut_map).fillna(5)
        df_processed["sym_score"] = df_processed["symmetry"].map(cut_map).fillna(5)

        # 2. Select Numerical Features for Similarity
        # We assume these columns exist in your JSON data
        self.feature_columns = [
            "priceListUSD",  # Price
            "weight",  # Carat
            "depthPerc",  # Depth %
            "tablePerc",  # Table %
            "color_score",  # Color
            "clarity_score",  # Clarity
            "cut_score",  # Cut
        ]

        # Fill missing numerical values with 0 (or mean) to prevent crashes
        X = df_processed[self.feature_columns].fillna(0).values

        # 3. Apply WEIGHTS (The "Business Logic")
        # manipulate the raw values BEFORE scaling/fitting to make them more "important"
        # Since i use StandardScaler, multiplying the input column increases its variance,
        # which effectively increases its weight in the distance calculation.

        # Get column indices
        price_idx = self.feature_columns.index("priceListUSD")
        carat_idx = self.feature_columns.index("weight")
        color_idx = self.feature_columns.index("color_score")

        # Apply multipliers from Config
        X[:, price_idx] *= settings.WEIGHT_PRICE
        X[:, carat_idx] *= settings.WEIGHT_CARAT
        X[:, color_idx] *= settings.WEIGHT_COLOR

        # 4. Scale Data (Z-Score Normalization)
        X_scaled = self.scaler.fit_transform(X)

        return X_scaled, df_processed["_id"].tolist()

    def fit(self, df: pd.DataFrame):
        """
        Trains the KNN model in memory.
        """
        if df.empty:
            print("Warning: DataFrame is empty. Cannot fit model.")
            return None, []

        X_scaled, ids = self.preprocess(df)
        self.model.fit(X_scaled)
        return X_scaled, ids

    def find_similar(self, df: pd.DataFrame):
        """
        Full Pipeline: Fits model on provided DF and returns neighbors for all rows.
        Returns: List of (id, [similar_ids...])
        """
        n_samples = len(df)
        # We want 11 (1 self + 10 recs), but we can't exceed the dataset size.
        k = min(n_samples, 11)

        # Update model's n_neighbors dynamically based on dataset size
        self.model.set_params(n_neighbors=k)

        X_scaled, ids = self.fit(df)

        # Get neighbors for the entire matrix (uses the updated k value)
        distances, indices = self.model.kneighbors(X_scaled)

        return ids, indices
