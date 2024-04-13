from crash_handler import check_server_health
from fastapi import FastAPI, Request, HTTPException
from threading import Thread
from globals import app
# app = FastAPI()

@app.post("/sync_app")
async def sync_app(request: Request):
    try:
        req = await request.json()
        # app.hash_dict = req["hash_dict"]
        # app.server_list = req["server_list"]

        app.hash_dict = req["hash_dict"]
        app.server_list = req["server_list"]
        app.locks = req["locks"]

        return {
            "message": "Successfully updated",
            "status": "success"
        }
        
    except Exception as e:
        print("error in update the app datastructure: ", e)


if __name__ == "__main__":
    import uvicorn
    print("starting shard manager ....")

    t1 = Thread(target=lambda: uvicorn.run(app, host="0.0.0.0", port=8000))
    t2 = Thread(target=check_server_health)
    
    t2.start()
    t1.start()
    
    t1.join()
    t2.join()