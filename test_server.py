from fastapi import FastAPI
import pickle

app = FastAPI()

@app.post("/receive-pickle")
async def receive_pickle(data: bytes):
    # Deserialize pickled data
    received_data = pickle.loads(data)
    print("Received set:", received_data)
    return {"message": "Set received successfully"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
