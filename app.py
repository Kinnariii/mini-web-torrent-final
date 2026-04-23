import os
import json
import subprocess
import threading
import random
import time
from flask import Flask, render_template, request, jsonify

import mini_dht

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Start the DHT Bootstrap node
bootstrap_dht = None
try:
    bootstrap_dht = mini_dht.MiniDHTNode("0.0.0.0", 8468)
except OSError:
    print("\n[WARNING] Port 8468 is already in use!")
    print("If you just restarted the app, the old background process might still be running.")
    print("The app will continue, but the DHT bootstrap node won't run in this process.\n")

download_progress = {}

def run_seeder(metainfo_path):
    port = random.randint(6000, 7000)
    log_file = open("seeder.log", "a", encoding="utf-8")
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    subprocess.Popen(
        ["python", "peer.py", metainfo_path, "--port", str(port), "--seeder"],
        stdout=log_file, stderr=subprocess.STDOUT, env=env
    )

def run_leecher(metainfo_path, port):
    with open(metainfo_path) as f:
        meta = json.load(f)

    file_name = meta["name"]
    total_pieces = len(meta["piece_hashes"])
    download_progress[file_name] = {
        "percent": 0, "pieces_done": 0,
        "total_pieces": total_pieces, "status": "downloading"
    }

    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    process = subprocess.Popen(
        ["python", "peer.py", metainfo_path, "--port", str(port)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        encoding="utf-8", env=env
    )

    pieces_done = 0
    for line in process.stdout:
        clean = line.encode('ascii', errors='ignore').decode()
        print("[DEBUG]", clean, flush=True)
        if "verified" in clean and "saved" in clean:
            pieces_done += 1
            pct = round((pieces_done / total_pieces) * 100, 1)
            download_progress[file_name] = {
                "percent": pct, "pieces_done": pieces_done,
                "total_pieces": total_pieces, "status": "downloading"
            }

    download_progress[file_name]["status"] = "completed"
    download_progress[file_name]["percent"] = 100

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    files = request.files.getlist('file')
    if not files or not files[0].filename:
        return jsonify({"error": "No files selected"}), 400

    first_path = files[0].filename.replace('\\', '/')
    if '/' in first_path:
        target_name = first_path.split('/')[0]
    else:
        target_name = first_path
        
    for file in files:
        filename = file.filename.replace('\\', '/')
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
        # Fix Windows MAX_PATH limit (260 chars) by converting to absolute path with \\?\ prefix
        abs_path = os.path.abspath(file_path)
        if os.name == 'nt' and not abs_path.startswith('\\\\?\\'):
            abs_path = '\\\\?\\' + abs_path
            
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        file.save(abs_path)

    target_path_for_meta = os.path.join(app.config['UPLOAD_FOLDER'], target_name)
    subprocess.run(["python", "metainfo_generator.py", target_path_for_meta], check=True)

    import shutil
    meta_src = f"{target_name}.json"
    meta_dest = os.path.join(app.config['UPLOAD_FOLDER'], f"{target_name}.json")
    if os.path.exists(meta_src):
        shutil.move(meta_src, meta_dest)

    run_seeder(meta_dest)

    with open(meta_dest) as f:
        meta = json.load(f)

    return jsonify({
        "message": "Files uploaded and metainfo created!",
        "file_name": target_name,
        "metainfo_path": meta_dest,
        "size": meta["total_size"],
        "pieces": len(meta["piece_hashes"])
    })

@app.route('/download', methods=['POST'])
def start_download():
    data = request.get_json()
    metainfo_path = data.get("metainfo_path")
    port = data.get("port", 6882)

    if not metainfo_path or not os.path.exists(metainfo_path):
        return jsonify({"error": "Metainfo file not found"}), 400

    t = threading.Thread(target=run_leecher, args=(metainfo_path, port), daemon=True)
    t.start()

    with open(metainfo_path) as f:
        file_name = json.load(f)["name"]

    with open(metainfo_path) as f:
        meta_data = json.load(f)

    return jsonify({
        "message": "Download started",
        "file_name": file_name,
        "num_pieces": len(meta_data["piece_hashes"])
    })

@app.route('/progress')
def progress():
    return jsonify(download_progress)

@app.route('/delete', methods=['POST'])
def delete_torrent():
    data = request.get_json()
    metainfo_path = data.get("metainfo_path")

    if not metainfo_path or not os.path.exists(metainfo_path):
        return jsonify({"error": "Metainfo file not found"}), 400

    try:
        with open(metainfo_path) as f:
            meta = json.load(f)

        file_name = meta["name"]
        is_dir = meta.get("is_dir", False)

        # Remove the metainfo JSON file
        os.remove(metainfo_path)

        # Remove the uploaded file or folder
        import shutil
        target_path = os.path.join(app.config['UPLOAD_FOLDER'], file_name)
        if os.path.isdir(target_path):
            shutil.rmtree(target_path)
        elif os.path.isfile(target_path):
            os.remove(target_path)

        # Remove from download_progress tracking
        if file_name in download_progress:
            del download_progress[file_name]

        return jsonify({"message": f"Deleted {file_name} successfully", "file_name": file_name})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/stats')
def stats():
    if not bootstrap_dht:
        return jsonify({"error": "DHT Node is not running on this process"})
        
    result = {}
    now = time.time()
    total_active_peers = 0
    total_healthy_peers = 0

    for info_hash, peers in bootstrap_dht.store.items():
        active = {}
        healthy = 0
        for p, data in peers.items():
            age = now - data["ts"]
            if age <= 90:
                active[p] = round(age, 1)
                total_active_peers += 1
                if age <= 30:   # "healthy" = announced within last 30 seconds
                    healthy += 1
                    total_healthy_peers += 1

        result[info_hash] = {
            "active_peers": len(active),
            "healthy_peers": healthy,
            "peers": active
        }

    return jsonify({
        "summary": {
            "total_active_peers": total_active_peers,
            "total_healthy_peers": total_healthy_peers,
            "total_torrents": len(result)
        },
        "torrents": result
    })

if __name__ == '__main__':
    # Disabled debug mode to prevent the Flask reloader from spawning a second process
    # which causes WinError 10048 when binding the DHT UDP socket.
    app.run(debug=False, port=8000)
