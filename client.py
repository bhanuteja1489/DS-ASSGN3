import requests
import pickle

# Data to send
data_to_send = {1, 2, 3, 4, 5}

# Serialize data using pickle
serialized_data = pickle.dumps(data_to_send)

# Send POST request to the server
response = requests.post("http://localhost:8000/receive-pickle", json=serialized_data)

# Print response
print(response.text)
