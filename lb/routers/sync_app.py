
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

        app.server_list = req["server_list"]
        to_remove = req["to_remove"]
        to_add = req["to_add"]
        for x in to_remove:
            app.hash_dict[x[0]].remove_server(x[1])
        for x in to_add:
            app.hash_dict[x[0]].add_server(x[1], x[2], x[3])
        return {
            "message": "Successfully updated",
            "status": "success"
        }
        
    except Exception as e:
        print("error in update the app datastructure: ", e)
