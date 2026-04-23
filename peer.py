import socket
import json
import hashlib
import threading
import time
import os
import argparse
import ssl
import subprocess
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.layout import Layout
from rich import box
import datetime

from mini_dht import MiniDHTNode

console = Console()

# ─────────────────────────────────────────────
#  Shared state
# ─────────────────────────────────────────────
pieces_lock = threading.Lock()
stats = {
    "downloaded": 0,
    "uploaded":   0,
    "peers_seen": set(),
    "start_time": time.time(),
}

def get_long_path(p):
    abs_p = os.path.abspath(p)
    if os.name == 'nt' and not abs_p.startswith('\\\\?\\'):
        return '\\\\?\\' + abs_p
    return abs_p

# ─────────────────────────────────────────────
#  Logical File Stream (Multi-file support)
# ─────────────────────────────────────────────
class LogicalFileStream:
    def __init__(self, files_meta, is_seeder, torrent_name, is_dir, base_dir="uploads"):
        self.files = []
        current_offset = 0
        for f in files_meta:
            if not is_dir:
                file_path = os.path.join(base_dir, f["path"])
            else:
                file_path = os.path.join(base_dir, torrent_name, f["path"])
                
            long_path = get_long_path(file_path)
            self.files.append({
                "path": file_path,
                "long_path": long_path,
                "length": f["length"],
                "start": current_offset,
                "end": current_offset + f["length"]
            })
            current_offset += f["length"]
            
            if not is_seeder:
                os.makedirs(os.path.dirname(long_path) or ".", exist_ok=True)
                if not os.path.exists(long_path):
                    with open(long_path, "wb") as fd:
                        fd.truncate(f["length"])

    def read_piece(self, offset, size):
        data = b""
        bytes_left = size
        curr_offset = offset
        for f in self.files:
            if bytes_left == 0: break
            if f["start"] <= curr_offset < f["end"]:
                file_offset = curr_offset - f["start"]
                read_size = min(bytes_left, f["end"] - curr_offset)
                with open(f["long_path"], "rb") as fd:
                    fd.seek(file_offset)
                    data += fd.read(read_size)
                bytes_left -= read_size
                curr_offset += read_size
        return data

    def write_piece(self, offset, data):
        bytes_left = len(data)
        curr_offset = offset
        data_idx = 0
        for f in self.files:
            if bytes_left == 0: break
            if f["start"] <= curr_offset < f["end"]:
                file_offset = curr_offset - f["start"]
                write_size = min(bytes_left, f["end"] - curr_offset)
                with open(f["long_path"], "r+b") as fd:
                    fd.seek(file_offset)
                    fd.write(data[data_idx:data_idx+write_size])
                bytes_left -= write_size
                curr_offset += write_size
                data_idx += write_size

# ─────────────────────────────────────────────
#  Uploader  (serves pieces to other peers)
# ─────────────────────────────────────────────
def handle_peer_request(client_socket, addr, lfs, piece_size, total_size):
    try:
        message = client_socket.recv(1024).decode().strip()
        if message.startswith("GET_PIECE:"):
            piece_index = int(message.split(":")[1])
            offset = piece_index * piece_size
            actual_piece_size = min(piece_size, total_size - offset)
            
            piece_data = lfs.read_piece(offset, actual_piece_size)
            client_socket.sendall(piece_data)

            with pieces_lock:
                stats["uploaded"] += 1
                stats["peers_seen"].add(addr[0])

            console.log(f"[green][Uploader][/green] Served piece {piece_index} to {addr[0]}")
    except Exception as e:
        console.log(f"[red][Uploader] Error handling {addr}: {e}[/red]")
    finally:
        client_socket.close()

def run_uploader(port, lfs, piece_size, total_size):
    if not os.path.exists("cert.pem") or not os.path.exists("key.pem"):
        console.log("[yellow]Generating self-signed TLS certificates...[/yellow]")
        subprocess.run([
            "openssl", "req", "-x509", "-newkey", "rsa:2048",
            "-keyout", "key.pem", "-out", "cert.pem",
            "-days", "365", "-nodes", "-subj", "/CN=mini-webtorrent"
        ], capture_output=True)

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile="cert.pem", keyfile="key.pem")

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("", port))
    server_socket.listen(10)
    
    secure_socket = context.wrap_socket(server_socket, server_side=True)
    console.log(f"[cyan][Uploader][/cyan] Listening on port {port} (TLS Enabled)")

    with ThreadPoolExecutor(max_workers=10) as executor:
        while True:
            try:
                client_socket, addr = secure_socket.accept()
                executor.submit(handle_peer_request, client_socket, addr, lfs, piece_size, total_size)
            except Exception as e:
                console.log(f"[red][Uploader] Accept error: {e}[/red]")

# ─────────────────────────────────────────────
#  Download a single piece from a single peer
# ─────────────────────────────────────────────
def download_piece(peer_addr, piece_index, piece_size, total_size, expected_hash):
    peer_ip, peer_port = peer_addr.split(":")
    offset = piece_index * piece_size
    actual_piece_size = min(piece_size, total_size - offset)

    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(10)
    secure_s = context.wrap_socket(s, server_hostname=peer_ip)
    try:
        secure_s.connect((peer_ip, int(peer_port)))
        secure_s.sendall(f"GET_PIECE:{piece_index}".encode())

        piece_data = b""
        while len(piece_data) < actual_piece_size:
            chunk = secure_s.recv(actual_piece_size - len(piece_data))
            if not chunk: break
            piece_data += chunk
    finally:
        secure_s.close()

    if hashlib.sha1(piece_data).hexdigest() != expected_hash:
        raise ValueError(f"Hash mismatch for piece {piece_index} from {peer_addr}")

    return piece_index, piece_data

# ─────────────────────────────────────────────
#  Downloader
# ─────────────────────────────────────────────
def run_downloader(metainfo, my_port, is_seeder):
    target_name = metainfo["name"]
    total_size  = metainfo["total_size"]
    piece_size  = metainfo["piece_size"]
    piece_hashes= metainfo["piece_hashes"]
    files_meta  = metainfo["files"]
    info_hash   = metainfo["info_hash"]
    num_pieces  = len(piece_hashes)
    is_dir      = metainfo.get("is_dir", False)

    lfs = LogicalFileStream(
        files_meta=files_meta,
        is_seeder=is_seeder,
        torrent_name=target_name,
        is_dir=is_dir,
        base_dir="uploads"
    )
    my_pieces = [is_seeder] * num_pieces

    # Start DHT Node
    dht_port = my_port + 1000
    dht = MiniDHTNode("0.0.0.0", dht_port, bootstrap_node=("127.0.0.1", 8468))
    console.log(f"[cyan][DHT][/cyan] Node running on port {dht_port}")

    # Uploader thread
    uploader_thread = threading.Thread(
        target=run_uploader,
        args=(my_port, lfs, piece_size, total_size),
        daemon=True,
    )
    uploader_thread.start()

    with Progress(
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(bar_width=40),
        TextColumn("[progress.percentage]{task.percentage:>5.1f}%"),
        TextColumn("({task.completed}/{task.total} pieces)"),
        console=console,
    ) as progress:
        dl_task = progress.add_task("Downloading", total=num_pieces, completed=sum(my_pieces))

        def print_stats(peers):
            elapsed = int(time.time() - stats["start_time"])
            table = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
            table.add_row("[bold]Target[/bold]",      target_name)
            table.add_row("[bold]Size[/bold]",        f"{total_size / 1024:.1f} KB")
            table.add_row("[bold]Pieces[/bold]",      f"{num_pieces} × {piece_size // 1024} KB")
            table.add_row("[bold]DHT Peers[/bold]",   str(len(peers)))
            table.add_row("[bold]Downloaded[/bold]",  str(stats["downloaded"]))
            table.add_row("[bold]Uploaded[/bold]",    str(stats["uploaded"]))
            table.add_row("[bold]Elapsed[/bold]",     f"{elapsed}s")
            console.print(Panel(table, title="[bold magenta]mini-webtorrent[/bold magenta]", border_style="magenta"))

        while True:
            with pieces_lock:
                done = sum(my_pieces)

            progress.update(dl_task, completed=done)

            if done == num_pieces:
                console.print("\n[bold green]✔ All pieces downloaded! Now seeding.[/bold green]")
                dht.announce(info_hash, my_port, list(range(num_pieces)))
                time.sleep(30)
                continue

            # Announce to DHT
            with pieces_lock:
                my_pieces_list = [i for i in range(num_pieces) if my_pieces[i]]
            dht.announce(info_hash, my_port, my_pieces_list)
            
            # Find peers via DHT
            peers_data = dht.find_peers(info_hash)
            # Filter out ourselves
            my_addr = f"127.0.0.1:{my_port}"
            peers_data = [p for p in peers_data if p["addr"] != my_addr]
            
            print_stats([p["addr"] for p in peers_data])

            if not peers_data:
                console.log("[yellow]No peers yet. Waiting...[/yellow]")
                time.sleep(10)
                continue

            # Rarest-First logic
            with pieces_lock:
                missing = [i for i in range(num_pieces) if not my_pieces[i]]

            piece_freq = {i: 0 for i in missing}
            peer_has_piece = {i: [] for i in missing}
            for p in peers_data:
                addr = p["addr"]
                for pi in p.get("pieces", []):
                    if pi in piece_freq:
                        piece_freq[pi] += 1
                        peer_has_piece[pi].append(addr)

            available_missing = [pi for pi in missing if piece_freq[pi] > 0]
            available_missing.sort(key=lambda x: piece_freq[x])

            work_items = []
            for piece_index in available_missing:
                peer_addr = random.choice(peer_has_piece[piece_index])
                work_items.append((piece_index, peer_addr))

            with ThreadPoolExecutor(max_workers=min(8, len(work_items))) as executor:
                futures = {
                    executor.submit(download_piece, peer_addr, pi, piece_size, total_size, piece_hashes[pi]): pi
                    for pi, peer_addr in work_items
                }

                for future in as_completed(futures):
                    pi = futures[future]
                    try:
                        _, piece_data = future.result()
                        lfs.write_piece(pi * piece_size, piece_data)
                        with pieces_lock:
                            my_pieces[pi] = True
                            stats["downloaded"] += 1
                        progress.update(dl_task, advance=1)
                        console.log(f"[green]✔ Piece {pi} verified & saved[/green]")
                    except Exception as e:
                        console.log(f"[red]✘ Piece {pi} failed: {e}[/red]")

            time.sleep(5)

def main():
    parser = argparse.ArgumentParser(description="mini-webtorrent peer")
    parser.add_argument("metainfo_file", help="Path to .json metainfo file")
    parser.add_argument("--port", type=int, required=True, help="Port to listen on")
    parser.add_argument("--seeder", action="store_true", help="Start as initial seeder")
    args = parser.parse_args()

    with open(args.metainfo_file) as f:
        metainfo = json.load(f)

    console.print(Panel.fit(
        f"[bold cyan]mini-webtorrent[/bold cyan]  |  port [yellow]{args.port}[/yellow]  |  "
        f"mode [magenta]{'SEEDER' if args.seeder else 'LEECHER'}[/magenta]",
        border_style="cyan",
    ))

    run_downloader(metainfo, args.port, args.seeder)

if __name__ == "__main__":
    main()
