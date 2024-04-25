import requests

url = "http://localhost:3000/get-message"

payload = {}
headers = {}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.json()["pdfData"])
