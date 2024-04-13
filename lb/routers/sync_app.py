
from fastapi import APIRouter, Request, HTTPException,Body
from fastapi.responses import JSONResponse
from globals import *

router = APIRouter()
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
