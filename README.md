# mini-webtorrent 🧲

A BitTorrent-inspired peer-to-peer file sharing system built from scratch in Python.  
Implements core BitTorrent mechanics: tracker-based peer discovery, SHA1 piece integrity verification, chunked file transfer over raw TCP sockets, and **parallel multi-peer downloading**.

---

## Architecture

```
┌─────────────┐        announce / get peers        ┌─────────────────┐
│   Peer A    │ ──────────────────────────────────► │                 │
│  (Seeder)   │ ◄──────────────────────────────── │    Tracker      │
└──────┬──────┘         peer list                  │  (Flask / HTTP) │
       │                                            └─────────────────┘
       │  GET_PIECE:N (raw TCP)                             ▲
       ▼                                                    │
┌─────────────┐        announce / get peers                 │
│   Peer B    │ ────────────────────────────────────────────┘
│  (Leecher)  │
└─────────────┘
```

### Components

| File | Role |
|---|---|
| `tracker.py` | Central HTTP tracker — maintains peer lists with TTL eviction |
| `metainfo_generator.py` | Generates `.json` metainfo file (like a `.torrent` file) |
| `peer.py` | Full peer — uploads pieces to others AND downloads in parallel |

### Key concepts demonstrated
- **Chunked file transfer** — files split into fixed-size pieces
- **SHA1 integrity verification** — every piece verified before writing to disk
- **Parallel downloads** — pieces fetched simultaneously from multiple peers via `ThreadPoolExecutor`
- **Tracker announce protocol** — HTTP GET with TTL-based peer eviction (90s)
- **Seeder / Leecher roles** — seeder has all pieces; leechers download and immediately become partial seeders
- **Custom wire protocol** — `GET_PIECE:<index>` over raw TCP sockets

---

## Setup

```bash
pip install -r requirements.txt
```

---

## Demo (3 terminals)

### Terminal 1 — Start the tracker
```bash
python tracker.py
```

### Terminal 2 — Generate metainfo & start seeder
```bash
# Create a test file
echo "Hello from mini-webtorrent! " > testfile.txt
python -c "open('testfile.txt','wb').write(b'A'*500_000)" # ~500 KB test file

# Generate metainfo
python metainfo_generator.py testfile.txt

# Start seeder (has the file, serves pieces)
python peer.py testfile.txt.json --port 6881 --seeder
```

### Terminal 3 — Start a leecher (downloads the file)
```bash
python peer.py testfile.txt.json --port 6882
```

### Check tracker stats (any browser or curl)
```bash
curl http://127.0.0.1:5000/stats
```

---

## How it works (step by step)

1. **Seeder** runs `metainfo_generator.py` → produces `file.json` with SHA1 hashes of all pieces
2. **Tracker** starts and waits for peer announcements
3. **Seeder** starts `peer.py --seeder` → begins listening for piece requests on its TCP port
4. **Leecher** starts `peer.py` → announces to tracker, receives peer list
5. Leecher spawns a **thread pool** and downloads multiple pieces **in parallel** from available peers
6. Each piece is **SHA1 verified** before being written to disk
7. Once leecher has a piece, it also **serves it** to other peers (becomes a partial seeder)
8. Tracker **evicts dead peers** every 30s if they haven't announced in 90s

---

## Improvements over v1

- ✅ **Last-piece size bug fixed** — final piece is correctly sized, no file corruption
- ✅ **Parallel downloads** — `ThreadPoolExecutor` fetches multiple pieces simultaneously
- ✅ **Socket timeout** — dead peers don't cause indefinite hangs (10s timeout)
- ✅ **Thread-safe piece tracking** — `threading.Lock` prevents race conditions
- ✅ **Tracker TTL eviction** — dead peers removed after 90s inactivity
- ✅ **Peer self-exclusion** — tracker never returns a peer to itself
- ✅ **Rich terminal UI** — live progress bar, piece count, peer stats
- ✅ **Tracker `/stats` endpoint** — live view of all active torrents and peers
