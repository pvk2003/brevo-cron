import os
from flask import Flask, request, jsonify

# Import file job (chứa code bạn)
import brevo_job

app = Flask(__name__)

@app.get("/run")
def run():
    # Bắt buộc có token để tránh người lạ gọi spam
    token = request.args.get("token", "")
    if token != os.getenv("CRON_TOKEN", ""):
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    # Chạy job
    brevo_job.main()
    return jsonify({"ok": True})
