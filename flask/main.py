from flask import Flask, request, jsonify
import json
import datetime

app = Flask(__name__)

PORT = 5000
# HOST = "127.0.0.1"
HOST = "0.0.0.0"

@app.before_request
def log_request_info():
    if request.method == "POST" and request.path == "/events":
        print("\n" + "═" * 80)
        print("📨 RICEVUTA RICHIESTA HTTP POST su /events")
        print("🕐 Timestamp:", datetime.datetime.utcnow().isoformat() + "Z")
        print("═" * 80)

@app.route("/events", methods=["POST"])
def receive_events():
    try:
        raw_body = request.get_data(as_text=True)
        if not raw_body:
            return jsonify({"error": "Empty body"}), 400
        
        payload = json.loads(raw_body)
    except json.JSONDecodeError as e:
        print("❌ ERRORE PARSING JSON:", e.msg)
        print("Body ricevuto (primi 500 caratteri):")
        print(raw_body[:500])
        print("═" * 80 + "\n")
        return jsonify({"error": "Invalid JSON"}), 400
    
    # Estrai i campi Debezium (compatibile con il formato che manda Flink/Debezium)
    inner_payload = payload.get("payload", {})
    op = inner_payload.get("op", "N/A")
    after = inner_payload.get("after")
    before = inner_payload.get("before")
    ts_ms = inner_payload.get("ts_ms", "N/A")
    source = inner_payload.get("source", {})
    
    print("✅ JSON parsato correttamente")
    print("🔄 Tipo operazione:", op)
    if after is not None:
        print("📊 Dati (after):", json.dumps(after, indent=2, ensure_ascii=False))
    if op == "d" and before is not None:
        print("🗑️  Dati (before):", json.dumps(before, indent=2, ensure_ascii=False))
    print("🕖 Timestamp evento:", ts_ms)
    db_name = source.get("db") or source.get("schema") or "N/A"
    print("🏭 Sorgente:", db_name)
    print("═" * 80 + "\n")
    
    return jsonify({
        "status": "ok",
        "received": True,
        "processed_at": datetime.datetime.utcnow().isoformat() + "Z",
        "operation": op
    })

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "error": "Not Found",
        "message": f"Endpoint {request.method} {request.path} non esistente"
    }), 404

if __name__ == "__main__":
    print("\n🚀 Server Flask avviato con successo!")
    print(f"📍 In ascolto su: http://localhost:{PORT}")
    print(f"📌 Endpoint principale: POST http://localhost:{PORT}/events")
    print("⏳ In attesa di eventi Debezium...\n")
    
    # debug=True abilita il reload automatico quando modifichi il codice
    app.run(host=HOST, port=PORT, debug=True)