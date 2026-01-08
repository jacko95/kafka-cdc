# from flask import Flask, request, jsonify
# import json
# import datetime
#
# app = Flask(__name__)
#
# PORT = 5000
# # HOST = "127.0.0.1"
# HOST = "0.0.0.0"
#
# @app.before_request
# def log_request_info():
#     if request.method == "POST" and request.path == "/events":
#         print("\n" + "═" * 80)
#         print("📨 RICEVUTA RICHIESTA HTTP POST su /events")
#         # print("🕐 Timestamp:", datetime.datetime.utcnow().isoformat() + "Z")
#         print("═" * 80)
#
# @app.route("/events", methods=["POST"])
# def receive_events():
#     try:
#         raw_body = request.get_data(as_text=True)
#         if not raw_body:
#             return jsonify({"error": "Empty body"}), 400
#
#         payload = json.loads(raw_body)
#     except json.JSONDecodeError as e:
#         print("❌ ERRORE PARSING JSON:", e.msg)
#         print("Body ricevuto (primi 500 caratteri):")
#         print(raw_body[:500])
#         print("═" * 80 + "\n")
#         return jsonify({"error": "Invalid JSON"}), 400
#
#     # Estrai i campi Debezium (compatibile con il formato che manda Flink/Debezium)
#     inner_payload = payload.get("payload", {})
#     op = inner_payload.get("op", "N/A")
#     after = inner_payload.get("after")
#     before = inner_payload.get("before")
#     ts_ms = inner_payload.get("ts_ms", "N/A")
#     source = inner_payload.get("source", {})
#
#     print("✅ JSON parsato correttamente")
#     print("🔄 Tipo operazione:", op)
#     if after is not None:
#         print("📊 Dati (after):", json.dumps(after, indent=2, ensure_ascii=False))
#     if op == "d" and before is not None:
#         print("🗑️  Dati (before):", json.dumps(before, indent=2, ensure_ascii=False))
#     print("🕖 Timestamp evento:", ts_ms)
#     db_name = source.get("db") or source.get("schema") or "N/A"
#     print("🏭 Sorgente:", db_name)
#     print("═" * 80 + "\n")
#
#     return jsonify({
#         "status": "ok",
#         "received": True,
#         # "processed_at": datetime.datetime.utcnow().isoformat() + "Z",
#         "operation": op
#     })
#
# @app.errorhandler(404)
# def not_found(error):
#     return jsonify({
#         "error": "Not Found",
#         "message": f"Endpoint {request.method} {request.path} non esistente"
#     }), 404
#
# if __name__ == "__main__":
#     print("\n🚀 Server Flask avviato con successo!")
#     print(f"📍 In ascolto su: http://localhost:{PORT}")
#     print(f"📌 Endpoint principale: POST http://localhost:{PORT}/events")
#     print("⏳ In attesa di eventi Debezium...\n")
#
#     # debug=True abilita il reload automatico quando modifichi il codice
#     app.run(host=HOST, port=PORT, debug=True)
#
######################################################################################################################

from flask import Flask, request, jsonify
import json
import mysql.connector
from mysql.connector import Error
import datetime
import os
import time

app = Flask(__name__)

print("🔍 AVVIO FLASK - DEBUG INFO")
print(f"   MYSQL_HOST: {os.getenv('MYSQL_HOST', 'mysql')}")
print(f"   MYSQL_DATABASE: {os.getenv('MYSQL_DATABASE', 'db_cdc')}")
print(f"   MYSQL_USER: {os.getenv('MYSQL_USER', 'cdc_user')}")

MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'mysql'),
    'port': 3306,
    'database': os.getenv('MYSQL_DATABASE', 'db_cdc'),
    'user': os.getenv('MYSQL_USER', 'cdc_user'),
    'password': os.getenv('MYSQL_PASSWORD', 'cdc_password')
}

def test_mysql_connection():
    """Test esplicito della connessione MySQL"""
    print("🔌 Test connessione MySQL...")
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        print(f"✅ MySQL connesso a {MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}")
        
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"📊 Tabelle trovate: {[t[0] for t in tables]}")
        
        cursor.close()
        conn.close()
        return True
    except Error as e:
        print(f"❌ Errore MySQL: {e}")
        print(f"   Config usata: host={MYSQL_CONFIG['host']}, user={MYSQL_CONFIG['user']}, db={MYSQL_CONFIG['database']}")
        return False

def get_mysql_connection():
    """Connessione semplice"""
    try:
        return mysql.connector.connect(**MYSQL_CONFIG)
    except Error as e:
        print(f"❌ Errore connessione: {e}")
        return None

@app.route("/events", methods=["POST"])
def receive_events():
    """Endpoint principale"""
    try:
        data = request.get_json()
        print(f"📨 Ricevuto: {json.dumps(data)[:200]}...")
        
        # Risposta fissa per test
        return jsonify({
            "status": "ok",
            "received": True,
            "timestamp": datetime.datetime.now().isoformat()
        })
    
    except Exception as e:
        print(f"💥 Errore: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check"""
    conn = get_mysql_connection()
    mysql_status = "connected" if conn else "disconnected"
    if conn:
        conn.close()
    
    return jsonify({
        "status": "ok",
        "flask": "running",
        "mysql": mysql_status,
        "time": datetime.datetime.now().isoformat()
    })

@app.route('/test-mysql', methods=['GET'])
def test_mysql():
    """Test esplicito MySQL"""
    conn = get_mysql_connection()
    if not conn:
        return jsonify({"error": "MySQL non connesso"}), 500
    
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        cursor.execute("SELECT COUNT(*) as count FROM utenti")
        count = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            "mysql_connected": True,
            "tables": [list(t.values())[0] for t in tables],
            "utenti_count": count['count'] if count else 0
        })
    except Error as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # Test immediato della connessione
    if not test_mysql_connection():
        print("⚠️  Attenzione: MySQL non raggiungibile all'avvio")
    
    print(f"\n🚀 Flask in ascolto su http://0.0.0.0:5000")
    print("📌 Endpoints:")
    print("   GET  /health     - Health check")
    print("   GET  /test-mysql - Test MySQL")
    print("   POST /events     - Ricevi eventi")
    print("\n")
    
    app.run(host="0.0.0.0", port=5000, debug=True)