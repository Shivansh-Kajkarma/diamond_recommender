# Diamond Recommender

A machine learning-based diamond recommendation system.

## Requirements

- Python 3.10.19

## Project Structure

```
diamond_recommendar/
├── app/
│   ├── __init__.py
│   └── main.py              # Main API file (call after database updates)
├── core/
│   ├── __init__.py
│   ├── config.py            # Configuration settings
│   └── ml_logic.py          # Machine learning logic
├── scripts/
│   ├── __init__.py
│   ├── batch_updater.py     # Main batch processing file
│   └── seed_db.py           # Useless
├── requirements.txt
└── README.md
```

## Main Files

| File | Description |
|------|-------------|
| `scripts/batch_updater.py` | **Main file** - Handles batch processing and updates |
| `core/ml_logic.py` | Contains all the machine learning logic |
| `core/config.py` | Configuration settings for the application |
| `app/main.py` | Main API file - **Must be called after any database update** |

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Else this is the main file which hits After database updates, trigger the API:
   ```bash
   python app/main.py
   ``` 

3. And if you want to run this script on standalone dataset to get recommendations then Run the batch updater only:
   ```bash
   python scripts/batch_updater.py
   ```
