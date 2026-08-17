from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE'])
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def catch_all(path):
    print(f"\n=== [MOCK LIS] Incoming {request.method} Request to: /{path} ===", flush=True)
    print(f"Headers: {dict(request.headers)}", flush=True)
    
    if request.is_json:
        print(f"JSON Payload: {request.get_json()}", flush=True)
    else:
        print(f"Raw Data: {request.get_data(as_text=True)}", flush=True)
        
    print("===============================================================\n", flush=True)
    return jsonify({"status": "SUCCESS", "received_path": path}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5005)