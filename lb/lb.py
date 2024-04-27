from time import sleep


from globals import app
from routers import init, status, add, rm, update, write, read, delete,sync_app,read_server




        

# Add routers to the FastAPI app

app.include_router(init.router)
app.include_router(status.router)
app.include_router(add.router)
app.include_router(rm.router)
app.include_router(update.router)
app.include_router(write.router)
app.include_router(read.router)
app.include_router(delete.router)
app.include_router(sync_app.router)
app.include_router(read_server.router)



# def checker_thread():
#     while 1:
#         print("Checking server health ....")
#         sleep(5)
    


# Run the FastAPI app
if __name__ == "__main__":
    
    print("Starting Load Balancer......")
    
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)