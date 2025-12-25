from fastapi import FastAPI, BackgroundTasks, HTTPException
import sys
import os

# Fix path to ensure we can import 'scripts'
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.batch_updater import run_batch_update

app = FastAPI(title="Diamond Recommendation Service")

@app.get("/")
def health_check():
    """Simple health check to see if server is running."""
    return {"status": "running", "service": "Diamond Recommender"}

@app.post("/trigger-update")
async def trigger_update(background_tasks: BackgroundTasks):
    """
    Endpoint for Node.js backend to trigger.
    It returns '200 OK' immediately and runs the heavy script in the background.
    """
    print("🔔 Trigger received! Starting update task...")
    
    # This runs the function in the background
    background_tasks.add_task(run_batch_update)
    
    return {
        "success": True, 
        "message": "Update process started in background."
    }
