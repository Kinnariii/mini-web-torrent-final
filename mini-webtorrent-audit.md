# mini-webtorrent — Full Reliability Audit Report

---

## Phase 1 — Issue Inventory

---

### 1. PEER DISCOVERY

---

#### ISSUE PD-1 — DHT `find_peers` is fire-and-forget with a fixed 1-second sleep
**File:** `mini_dht.py`  
**Function:** `find_peers`  
**Severity:** 🔴 Critical

```python
def find_peers(self, info_hash):
    msg = {"type": "FIND_VALUE", "info_hash": info_hash}
    for node in list(self.known_nodes):
        self._send(msg, node)

    # Give network time to respond
    time.sleep(1)          # <-- fixed 1-second blind wait

    now = time.time()
    peers = []
    if info_hash in self.store:
        peers = [...]
    return peers
```

**Why it fails:** UDP responses from DHT nodes are received asynchronously in `_listen`. The 1-second sleep is a blind wait with no acknowledgement that any response has arrived. On a loaded machine, in a VM, or over any latency, responses may not arrive within 1 second. Because `find_peers` is called every loop iteration and returns immediately after the sleep, a leecher that started before seeders have fully announced will get an empty peer list and wait 10 more seconds before retrying. This compounds with the 5-second sleep at the end of the download loop.

**How to reproduce:** Start a seeder and a leecher simultaneously on a slow machine or add `time.sleep(0.5)` before announcing on the seeder. The leecher will log "No peers yet. Waiting..." repeatedly even though the seeder exists.

---

#### ISSUE PD-2 — `known_nodes` is a plain `set` with no thread-safety
**File:** `mini_dht.py`  
**Function:** `_listen`, `announce`, `find_peers`  
**Severity:** 🟠 High

```python
self.known_nodes = set()
# In _listen (background thread):
self.known_nodes.add(addr)
# In announce (caller thread):
for node in list(self.known_nodes):
    self._send(msg, node)
```

**Why it fails:** `_listen` runs in its own daemon thread and mutates `known_nodes` concurrently with `announce` and `find_peers` which iterate over it. Although Python's GIL prevents torn writes on individual operations, the `list(self.known_nodes)` snapshot in `announce`/`find_peers` can race with `add` in `_listen`. More critically, `self.store` (a nested dict) is mutated by `_listen`, `_cleanup`, `announce`, and `find_peers` with zero locking — nested dict mutation is **not** GIL-safe across multiple operations.

**How to reproduce:** Run 4+ peers simultaneously announcing and querying the bootstrap node. Intermittent `RuntimeError: dictionary changed size during iteration` in `_cleanup`.

---

#### ISSUE PD-3 — `_cleanup` deletes from `self.store` while iterating it
**File:** `mini_dht.py`  
**Function:** `_cleanup`  
**Severity:** 🟠 High

```python
def _cleanup(self):
    while self.running:
        time.sleep(30)
        now = time.time()
        for info_hash in list(self.store.keys()):   # outer copy — OK
            stale = [p for p, data in self.store[info_hash].items()  # inner NOT copied
                     if now - data["ts"] > 90]
            for p in stale:
                del self.store[info_hash][p]         # mutates while _listen may be writing
            if not self.store[info_hash]:
                del self.store[info_hash]
```

**Why it fails:** The inner comprehension `self.store[info_hash].items()` iterates the peer dict without holding a lock. `_listen` can insert a new peer into the same inner dict at the exact same moment, causing `RuntimeError: dictionary changed size during iteration`.

**How to reproduce:** Many active peers announcing rapidly to the bootstrap node with pieces changing frequently; cleanup will eventually crash the cleanup thread silently (daemon thread, exception swallowed by bare `except: pass`).

---

#### ISSUE PD-4 — `announce` hardcodes `127.0.0.1` as the peer address
**File:** `mini_dht.py`  
**Function:** `announce`  
**Severity:** 🟡 Medium (localhost-only deployment), 🔴 Critical for any multi-host use

```python
def announce(self, info_hash, peer_port, pieces):
    peer_addr = f"127.0.0.1:{peer_port}"   # always localhost!
```

**Why it fails:** Even if peers run on different hosts, they always announce themselves as `127.0.0.1:PORT`. Any peer that receives this announcement will try to connect to its own localhost instead of the actual remote host, getting connection refused. This makes the system **fundamentally single-machine only** with no indication of the limitation.

---

#### ISSUE PD-5 — Peer expiry window (90 s) vs. announce interval mismatch
**File:** `mini_dht.py` / `peer.py`  
**Severity:** 🟡 Medium

The DHT expires peers after 90 seconds. In `peer.py`, the main download loop calls `dht.announce(...)` every iteration. With the `time.sleep(5)` at the end of the loop plus `find_peers`'s internal 1-second sleep plus connection time, a heavily loaded peer may miss announcement windows and be silently dropped from the DHT, making active seeders invisible to leechers.

---

### 2. DOWNLOAD PIPELINE

---

#### ISSUE DL-1 — No retry logic for failed piece downloads
**File:** `peer.py`  
**Function:** `run_downloader`  
**Severity:** 🔴 Critical

```python
for future in as_completed(futures):
    pi = futures[future]
    try:
        _, piece_data = future.result()
        lfs.write_piece(pi * piece_size, piece_data)
        with pieces_lock:
            my_pieces[pi] = True
            stats["downloaded"] += 1
        ...
    except Exception as e:
        console.log(f"[red]✘ Piece {pi} failed: {e}[/red]")
        # piece stays False in my_pieces — will retry NEXT LOOP ITERATION
```

**Why it fails:** When a piece fails (timeout, hash mismatch, connection error), it is silently dropped. It will be retried only in the next outer `while True` loop iteration — after a 5-second sleep, a DHT announce, a `find_peers` (1-second sleep), and re-sorting of pieces. If the seeder is overloaded or intermittently unavailable, the retry window may never align and pieces stay permanently stuck. There is no per-piece retry counter, no exponential backoff, and no mechanism to blacklist a consistently-failing peer.

**How to reproduce:** Use a seeder with a rate-limited or unreliable network interface. Observe pieces fail indefinitely even with the seeder available.

---

#### ISSUE DL-2 — Duplicate piece downloads (race condition)
**File:** `peer.py`  
**Function:** `run_downloader`  
**Severity:** 🔴 Critical

```python
with pieces_lock:
    missing = [i for i in range(num_pieces) if not my_pieces[i]]

# ... build work_items from missing ...

with ThreadPoolExecutor(max_workers=min(8, len(work_items))) as executor:
    futures = {
        executor.submit(download_piece, peer_addr, pi, ...): pi
        for pi, peer_addr in work_items
    }
    for future in as_completed(futures):
        ...
        with pieces_lock:
            my_pieces[pi] = True
```

**Why it fails:** The `missing` list is computed under lock. Then ALL missing pieces are submitted to the thread pool at once. If a piece download completes successfully and sets `my_pieces[pi] = True`, but the next outer loop iteration begins before the thread pool drains (impossible here since the pool is used as a context manager), OR if two consecutive loop iterations overlap (they can't with the current code), this isn't triggered here. **However**, the real race is: all missing pieces at iteration N are submitted. While they download, the loop blocks at `as_completed`. On the next iteration, `missing` is recomputed — but since the `ThreadPoolExecutor` context manager has already exited (all futures complete), this is safe in the single-loop-iteration case. **The actual duplicate issue** occurs when a piece that was submitted as a `work_item` succeeds, but the outer loop immediately re-submits it before `my_pieces[pi]` is set — this can't happen with the blocking `as_completed`. What *can* happen: the entire `work_items` list includes ALL missing pieces every iteration, so if a piece fails and the loop restarts, the same piece is re-attempted from the same potentially-bad peer (no peer rotation on failure).

---

#### ISSUE DL-3 — `work_items` submits every missing piece every iteration — thread explosion
**File:** `peer.py`  
**Function:** `run_downloader`  
**Severity:** 🟠 High

```python
with ThreadPoolExecutor(max_workers=min(8, len(work_items))) as executor:
    futures = {
        executor.submit(download_piece, ...): pi
        for pi, peer_addr in work_items   # ALL missing pieces
    }
```

**Why it fails:** For a torrent with 100 missing pieces, this creates 100 futures but only 8 worker threads. The other 92 tasks queue internally. This is not inherently wrong, but combined with a 10-second per-piece socket timeout, the total time to drain all futures is bounded by `ceil(100/8) * 10s = 130 seconds` worst-case per loop iteration. During this time, the DHT announce loop stalls — no fresh peer announcements are sent, peers expire from DHT, and the leecher becomes invisible to new peers joining mid-download.

---

#### ISSUE DL-4 — Peer selection never rotates on failure
**File:** `peer.py`  
**Function:** `run_downloader`  
**Severity:** 🟠 High

```python
peer_addr = random.choice(peer_has_piece[piece_index])
work_items.append((piece_index, peer_addr))
```

**Why it fails:** A peer is selected randomly once per loop iteration. If that peer is down or slow, the piece fails. On the next iteration, the same peer may be randomly selected again (especially with few peers). There is no mechanism to track which peers have failed and prefer alternatives.

---

### 3. UPLOAD PIPELINE

---

#### ISSUE UP-1 — Uploader reads only a single `recv(1024)` with no loop
**File:** `peer.py`  
**Function:** `handle_peer_request`  
**Severity:** 🔴 Critical

```python
def handle_peer_request(client_socket, addr, lfs, piece_size, total_size):
    try:
        message = client_socket.recv(1024).decode().strip()
        if message.startswith("GET_PIECE:"):
            piece_index = int(message.split(":")[1])
```

**Why it fails:** TCP is a stream protocol. `recv(1024)` may return a partial message (e.g., only `"GET_PIE"` instead of `"GET_PIECE:5"`). On a loopback this is extremely unlikely but on any real network it can happen. The message check `startswith("GET_PIECE:")` will then fail silently — the piece is never served, the leecher's `recv` loop hangs until timeout, and the piece download fails. There is no loop to accumulate a full line, and no delimiter framing.

---

#### ISSUE UP-2 — No socket timeout on the uploader's accepted connections
**File:** `peer.py`  
**Function:** `handle_peer_request`, `run_uploader`  
**Severity:** 🟠 High

```python
client_socket, addr = secure_socket.accept()
executor.submit(handle_peer_request, client_socket, addr, lfs, piece_size, total_size)
```

No `client_socket.settimeout(...)` is called. If a leecher connects and then hangs (network drop, crash), the uploader thread for that connection blocks in `client_socket.recv(1024)` forever, consuming a worker thread from the `ThreadPoolExecutor(max_workers=10)`. With 10 stale connections, the uploader is completely deadlocked — no new pieces can be served.

---

#### ISSUE UP-3 — `run_uploader` uses a plain `server_socket` then wraps it, but the TLS context wraps the wrong socket
**File:** `peer.py`  
**Function:** `run_uploader`  
**Severity:** 🟠 High

```python
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind(("", port))
server_socket.listen(10)

secure_socket = context.wrap_socket(server_socket, server_side=True)
```

`context.wrap_socket(server_socket, server_side=True)` on a listening socket creates a TLS-wrapped listening socket. Calling `secure_socket.accept()` returns a **new already-TLS-wrapped client socket**. This part is actually correct for server-side TLS. However, if `wrap_socket` raises (e.g., bad cert/key), `server_socket` is leaked — it's already bound to the port and never closed. On restart, `bind` will fail unless `SO_REUSEADDR` kicks in. Not strictly a data-corruption bug but causes server start failures.

---

#### ISSUE UP-4 — Uploader does not validate `piece_index` range
**File:** `peer.py`  
**Function:** `handle_peer_request`  
**Severity:** 🟡 Medium

```python
piece_index = int(message.split(":")[1])
offset = piece_index * piece_size
actual_piece_size = min(piece_size, total_size - offset)
piece_data = lfs.read_piece(offset, actual_piece_size)
```

If `piece_index` is out of range (negative, or >= num_pieces), `actual_piece_size` becomes negative or zero and `read_piece` will silently return empty bytes. The leecher receives 0 bytes, the SHA1 check fails, and the piece download fails. A malformed or accidentally corrupted request message causes a cascade failure.

---

### 4. NETWORKING

---

#### ISSUE NET-1 — `download_piece` recv loop breaks on empty chunk — partial data treated as complete
**File:** `peer.py`  
**Function:** `download_piece`  
**Severity:** 🔴 Critical

```python
piece_data = b""
while len(piece_data) < actual_piece_size:
    chunk = secure_s.recv(actual_piece_size - len(piece_data))
    if not chunk: break       # <-- silent partial data acceptance path
    piece_data += chunk
```

**Why it fails:** When `not chunk` (EOF / connection closed), the loop breaks and `piece_data` is whatever partial data arrived. The SHA1 check then runs on the incomplete data:

```python
if hashlib.sha1(piece_data).hexdigest() != expected_hash:
    raise ValueError(f"Hash mismatch for piece {piece_index} from {peer_addr}")
```

The hash mismatch will be caught and the piece is retried — so data corruption is prevented. **However**, the error message is "Hash mismatch" not "Partial read", making diagnosis misleading. More critically: if by extremely bad luck partial data matches the hash (impossible for SHA1 practically, but the logic path exists), corrupted data would be written to disk.

Actually the real failure path: the connection is closed mid-transfer because the uploader does `client_socket.close()` in `finally` immediately after `sendall` — this is correct. But if `sendall` itself only partially sends due to a TCP buffer issue (see UP-5), the receiver gets partial data, breaks on empty recv, and fails hash check. This creates a permanent retry loop if the issue is consistent.

---

#### ISSUE NET-2 — No socket timeout in `download_piece` after connection
**File:** `peer.py`  
**Function:** `download_piece`  
**Severity:** 🟠 High

```python
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(10)              # timeout set on raw socket
secure_s = context.wrap_socket(s, server_hostname=peer_ip)
try:
    secure_s.connect((peer_ip, int(peer_port)))
    secure_s.sendall(...)
    ...
    while len(piece_data) < actual_piece_size:
        chunk = secure_s.recv(...)   # timeout still active? 
```

`settimeout(10)` on the raw socket **does** carry over to the wrapped TLS socket in CPython. So this is partially fine. However, the timeout applies per-`recv` call, not to the total download duration. A slow peer sending data in tiny trickles, each arriving within 10 seconds, will hold a worker thread for much longer than intended (e.g., a 1 MB piece served at 1 byte/10 seconds = 10 million seconds).

---

#### ISSUE NET-3 — `sendall` in the uploader may silently fail for large pieces
**File:** `peer.py`  
**Function:** `handle_peer_request`  
**Severity:** 🟠 High

```python
piece_data = lfs.read_piece(offset, actual_piece_size)
client_socket.sendall(piece_data)
```

`sendall` raises `OSError` if the send fails — it does not silently truncate. So the exception path is correct (it will fall to `except Exception as e` and close the socket). However, there is no check that `piece_data` is actually `actual_piece_size` bytes — `read_piece` may return fewer bytes if a file read fails silently (see FS-2). The leecher then receives truncated data, the hash fails, and the piece is retried forever.

---

#### ISSUE NET-4 — TLS certificate verification disabled on leecher
**File:** `peer.py`  
**Function:** `download_piece`  
**Severity:** 🟡 Medium (security, not reliability)

```python
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE
```

While not a download-reliability bug per se, disabling cert verification means a man-in-the-middle can inject arbitrary piece data. Combined with the SHA1 integrity check this is partially mitigated, but it's a design flaw worth noting.

---

### 5. CONCURRENCY

---

#### ISSUE CON-1 — `stats` dict is accessed without lock in `print_stats`
**File:** `peer.py`  
**Function:** `run_downloader` / `print_stats` closure  
**Severity:** 🟡 Medium

```python
stats = {
    "downloaded": 0,
    "uploaded":   0,
    "peers_seen": set(),
    ...
}
# In handle_peer_request (uploader thread):
with pieces_lock:
    stats["uploaded"] += 1
    stats["peers_seen"].add(addr[0])

# In print_stats (downloader thread, no lock):
table.add_row("Downloaded",  str(stats["downloaded"]))
table.add_row("Uploaded",    str(stats["uploaded"]))
```

`stats["downloaded"]` is mutated in the `as_completed` loop without a lock on the stats dict itself (only `pieces_lock` is used). The uploader uses `pieces_lock` for `stats["uploaded"]`. The downloader reads `stats["downloaded"]` and `stats["uploaded"]` in `print_stats` without any lock. Under CPython the GIL protects individual int reads/writes but the read-modify-write `+= 1` is not atomic without a lock.

---

#### ISSUE CON-2 — `lfs.write_piece` is not thread-safe for overlapping writes
**File:** `peer.py`  
**Function:** `LogicalFileStream.write_piece`  
**Severity:** 🔴 Critical

```python
def write_piece(self, offset, data):
    ...
    for f in self.files:
        if bytes_left == 0: break
        if f["start"] <= curr_offset < f["end"]:
            ...
            with open(f["long_path"], "r+b") as fd:
                fd.seek(file_offset)
                fd.write(data[data_idx:data_idx+write_size])
```

Each call opens the file, seeks to a position, and writes. If two threads call `write_piece` concurrently for pieces that map to the same file (which is almost always the case for single-file torrents), the seek+write sequence is not atomic. Thread A seeks to offset 0, Thread B seeks to offset 512, Thread A writes 512 bytes — but Thread B has now set the file position to 512 so Thread A's write may land at the wrong position. **Actually**: each `open()` call creates an independent file descriptor with its own file position, so concurrent `write_piece` calls are safe in that regard. **However**, for multi-file torrents where a single piece spans two files, `write_piece` opens file A, writes, then opens file B, writes — there is a window between these two `open` calls during which the first file write is committed but the second is not. If the process crashes here, piece data is split across the file boundary in a half-written state with no recovery path. No atomicity guarantee.

---

#### ISSUE CON-3 — `my_pieces[pi] = True` is set before verifying successful write
**File:** `peer.py`  
**Function:** `run_downloader`  
**Severity:** 🔴 Critical

```python
_, piece_data = future.result()
lfs.write_piece(pi * piece_size, piece_data)    # may raise!
with pieces_lock:
    my_pieces[pi] = True                         # set even if write_piece raised?
    stats["downloaded"] += 1
```

Wait — actually `lfs.write_piece` is called **before** the lock block, so if it raises, `my_pieces[pi]` is never set to `True` and the piece will be retried. This is correct. **However**, `lfs.write_piece` may **not raise** even if the write is incomplete — `fd.write()` returns the number of bytes written and this return value is ignored. A short write to a nearly-full disk will silently succeed from Python's perspective but the data on disk is truncated.

---

#### ISSUE CON-4 — `stats["downloaded"] += 1` without `pieces_lock`
**File:** `peer.py`  
**Function:** `run_downloader`  
**Severity:** 🟡 Medium

```python
with pieces_lock:
    my_pieces[pi] = True
    stats["downloaded"] += 1
```

Actually `stats["downloaded"]` IS updated inside `pieces_lock` here — this is fine. But `stats["uploaded"]` in the uploader is also updated inside `pieces_lock`. So both stats share the same lock — correct. However, `stats["peers_seen"]` is a `set`, and `set.add()` under the GIL is safe, but iterating it elsewhere without a lock is not.

---

### 6. FILE SYSTEM

---

#### ISSUE FS-1 — `LogicalFileStream.__init__` truncates files to declared size unconditionally
**File:** `peer.py`  
**Function:** `LogicalFileStream.__init__`  
**Severity:** 🟠 High

```python
if not is_seeder:
    os.makedirs(os.path.dirname(long_path) or ".", exist_ok=True)
    if not os.path.exists(long_path):
        with open(long_path, "wb") as fd:
            fd.truncate(f["length"])
```

**Why it fails:** `fd.truncate(f["length"])` on a newly-opened file in `"wb"` mode does not actually pre-allocate `f["length"]` bytes on all filesystems. On Linux with sparse file support it creates a sparse file (correct). But `fd.truncate()` is called on an already-opened `"wb"` file handle — at the time of the call the file position is 0, so `truncate(n)` extends the file to `n` bytes. This is correct. **However**: if the process is killed mid-download and restarted, `os.path.exists(long_path)` returns `True` for the partially-written file — the file is NOT re-truncated, and the leecher resumes from a blank `my_pieces = [False] * num_pieces` state. This means previously-downloaded pieces are re-downloaded and overwritten (wasteful but correct for fully-downloaded pieces). For pieces that were only partially written (write interrupted), the stale data from the previous run remains on disk, and if those pieces somehow pass hash checks (they won't for SHA1), they'd be corrupt. Since they fail hash checks they'll be retried — correct behavior, but no resume support.

---

#### ISSUE FS-2 — `read_piece` silently returns short data if a file is shorter than declared
**File:** `peer.py`  
**Function:** `LogicalFileStream.read_piece`  
**Severity:** 🔴 Critical

```python
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
                data += fd.read(read_size)   # may return FEWER than read_size bytes!
            bytes_left -= read_size          # decrements by intended size, not actual size
            curr_offset += read_size
    return data
```

**Why it fails:** `fd.read(read_size)` may return fewer than `read_size` bytes if the file is shorter than declared in the metainfo. The code decrements `bytes_left` by `read_size` (the intended amount) rather than `len(returned_data)`. So `bytes_left` underflows, the loop exits, and the function returns truncated data **without signaling an error**. The uploader sends this short data to the leecher. The leecher's SHA1 check fails. The piece is retried forever. For the seeder, this silently means bad piece data is served for any file that is shorter than its declared size — which happens if a file was modified or truncated after the metainfo was generated.

---

#### ISSUE FS-3 — `write_piece` return value of `fd.write()` is ignored
**File:** `peer.py`  
**Function:** `LogicalFileStream.write_piece`  
**Severity:** 🟠 High

```python
with open(f["long_path"], "r+b") as fd:
    fd.seek(file_offset)
    fd.write(data[data_idx:data_idx+write_size])  # return value ignored
```

On a full disk, `fd.write()` may raise `OSError: [Errno 28] No space left on device` — this would propagate up and the piece would be retried. However on some networked filesystems or in edge cases, partial writes can succeed silently. The number of bytes actually written should be checked and compared to `write_size`.

---

#### ISSUE FS-4 — Multi-file piece boundary: `read_piece` loop condition misses file boundaries
**File:** `peer.py`  
**Function:** `LogicalFileStream.read_piece`  
**Severity:** 🔴 Critical

```python
for f in self.files:
    if bytes_left == 0: break
    if f["start"] <= curr_offset < f["end"]:
        ...
        bytes_left -= read_size
        curr_offset += read_size
```

**Why it fails:** After consuming bytes from file A, `curr_offset` advances. The loop then checks the next file. But the condition `f["start"] <= curr_offset < f["end"]` will correctly match file B if `curr_offset` now falls in file B's range. **This looks correct at first glance.** However: if `bytes_left` is not 0 after the loop completes all files, the function silently returns short data with no error. This happens when a piece spans a file boundary and `curr_offset` skips a file whose `start` > `curr_offset` after advancing. Consider files [A: 0-100, B: 100-200] and a read at offset 90, size 20: first iteration matches A (90 < 100), reads 10 bytes, advances curr_offset to 100. Second iteration: checks B (100 <= 100 < 200) — matches correctly. This specific case works. But if files are non-contiguous in the metadata (which shouldn't happen but isn't validated), the loop could miss a file.

More critically: `bytes_left -= read_size` uses `read_size` (the *intended* read size) not `len(fd.read(...))` as noted in FS-2. This makes FS-2 the dominant bug here.

---

#### ISSUE FS-5 — Single-file torrent path construction may be wrong
**File:** `peer.py`  
**Function:** `LogicalFileStream.__init__`  
**Severity:** 🟠 High

```python
if not is_dir:
    file_path = os.path.join(base_dir, f["path"])
else:
    file_path = os.path.join(base_dir, torrent_name, f["path"])
```

For a single-file torrent, `f["path"]` is set in `metainfo_generator.py` as `target_name` (the filename). So the path becomes `uploads/<filename>`. For a directory torrent, `f["path"]` is the relative path within the directory (e.g., `subdir/file.txt`), and the full path becomes `uploads/<torrent_name>/subdir/file.txt`. 

**The seeder** uses the same `LogicalFileStream` constructor with `is_seeder=True`, which skips file creation. The seeder's file at `uploads/<filename>` is the original upload. The leecher writes to `uploads/<filename>` too — potentially overwriting the seeder's file if both run on the same machine in the same working directory. This is the case in the web UI (`app.py`) where seeder and leecher share `uploads/`.

---

### 7. INTEGRITY VERIFICATION

---

#### ISSUE IV-1 — SHA1 is cryptographically weak (informational)
**File:** `peer.py`, `metainfo_generator.py`  
**Severity:** 🟢 Low (noted for completeness; not a reliability bug)

SHA1 is used for piece hashes. This matches BitTorrent v1 convention but SHA1 is deprecated for security. Not a reliability bug.

---

#### ISSUE IV-2 — Hash mismatch exception message is swallowed and indistinguishable from network errors
**File:** `peer.py`  
**Function:** `run_downloader`  
**Severity:** 🟡 Medium

```python
except Exception as e:
    console.log(f"[red]✘ Piece {pi} failed: {e}[/red]")
```

All exceptions — hash mismatch, timeout, connection refused, partial read — result in the same log message. There is no differentiation between "bad peer" (should be blacklisted) and "transient network error" (should be retried soon) and "always-corrupt piece" (should flag the seeder as bad). This makes debugging production failures extremely difficult.

---

#### ISSUE IV-3 — No end-to-end file integrity check after all pieces are assembled
**File:** `peer.py`  
**Function:** `run_downloader`  
**Severity:** 🟠 High

```python
if done == num_pieces:
    console.print("\n[bold green]✔ All pieces downloaded! Now seeding.[/bold green]")
    dht.announce(info_hash, my_port, list(range(num_pieces)))
    time.sleep(30)
    continue
```

Once all pieces are marked done, there is no final SHA1 or MD5 check over the complete assembled file. If two pieces were written to overlapping offsets due to a race, or if a disk error corrupted a completed piece after it was marked done, the final file is silently corrupt. A file-level hash in the metainfo would catch this.

---

### 8. RECONSTRUCTION

---

#### ISSUE REC-1 — `app.py` leecher progress tracking parses `stdout` with a fragile string match
**File:** `app.py`  
**Function:** `run_leecher`  
**Severity:** 🟠 High

```python
for line in process.stdout:
    clean = line.encode('ascii', errors='ignore').decode()
    if "verified" in clean and "saved" in clean:
        pieces_done += 1
```

The progress tracking relies on parsing the rich-formatted console output of `peer.py` for the string `"verified"` and `"saved"`. This is fragile:
- Rich may add ANSI escape codes that survive the ASCII stripping
- If the log message in `peer.py` is changed, progress tracking silently breaks
- Pieces that fail then succeed on retry may cause `pieces_done` to count above `total_pieces`

---

#### ISSUE REC-2 — `run_leecher` in `app.py` marks progress as `completed` even on crash
**File:** `app.py`  
**Function:** `run_leecher`  
**Severity:** 🟠 High

```python
for line in process.stdout:
    ...

download_progress[file_name]["status"] = "completed"
download_progress[file_name]["percent"] = 100
```

The `for line in process.stdout` loop exits when the subprocess terminates — whether by successful completion, crash, or kill. After the loop, status is unconditionally set to `"completed"` and percent to `100`. If `peer.py` crashes mid-download, the UI reports 100% success.

---

#### ISSUE REC-3 — `app.py` seeder uses a random port with no collision detection
**File:** `app.py`  
**Function:** `run_seeder`  
**Severity:** 🟡 Medium

```python
port = random.randint(6000, 7000)
```

If two seeders are started (two uploads) and they randomly pick the same port, the second seeder's `bind` fails. The failure is silent — `subprocess.Popen` does not check the seeder process's exit code. The torrent is announced to DHT with a port that has no listener, and all download attempts to that seeder will get connection refused.

---

#### ISSUE REC-4 — `metainfo_generator.py` does not include a total file hash
**File:** `metainfo_generator.py`  
**Severity:** 🟠 High (design gap)

The metainfo only contains per-piece SHA1 hashes. There is no hash of the complete logical byte stream. After reconstruction, there is no way to verify the complete file is correct without re-reading and re-hashing every piece independently.

---

## Phase 2 — Root Cause Rankings

### #1 — Most Likely: ISSUE DL-1 + NET-1 (No Retry + Partial Read → Permanent Piece Failure)
**Confidence: Very High**

The combination of:
1. A piece failing due to a transient network error (timeout, partial TCP read)  
2. No per-piece retry with alternative peer selection  
3. Only retrying in the next outer loop iteration (5s sleep + DHT cycle)  
4. Potentially re-selecting the same broken peer via `random.choice`

...means a download gets permanently stuck on one or more pieces when peers are unreliable. This is the single most common real-world failure mode for torrent clients and this implementation has zero mitigation.

---

### #2 — Second Most Likely: ISSUE FS-2 (Silent Short Reads from Seeder)
**Confidence: High**

`read_piece` uses the *intended* read size to decrement `bytes_left` rather than the *actual* bytes returned. The seeder then sends short data. The leecher gets a hash mismatch. The piece is retried — from the same seeder — which sends the same short data again. This creates a permanent retry loop that appears as the download being "stuck" at N-1 pieces.

This is triggered by any file that is smaller on disk than declared in the metainfo, which can happen if: the file was modified after metainfo generation, the file system ran out of space during upload, or the sparse file pre-allocation in `__init__` behaved unexpectedly.

---

### #3 — Third Most Likely: ISSUE CON-2 / FS-5 (Write Races / Wrong Path on Same-Machine Multi-Instance)
**Confidence: Medium-High**

When running seeder and leecher on the same machine (the default in the web UI), both processes point to `uploads/<filename>`. If the leecher overwrites pieces that the seeder is actively reading (for serving to other leechers), a seeder `read_piece` may return stale/partially-overwritten data. Additionally, `write_piece` opens a file, seeks, and writes — on multi-file torrents with pieces spanning file boundaries, a crash between the two `open()` calls leaves the torrent in a half-written state with no recovery.

---

## Phase 3 — Fix Plan

---

### Fix 1: Per-piece retry with peer rotation and backoff
**Files:** `peer.py`  
**Function:** `run_downloader`  
**Priority:** Critical

**New logic:**
- Add a `piece_retry_count = {}` dict tracking attempts per piece index
- Add a `failed_peers = {}` dict mapping piece_index → set of failed peer addresses
- In the `as_completed` loop, on exception: increment retry count, add peer to failed set, re-submit immediately (up to 3 retries) with a different peer excluding failed ones
- After 3 failures, log the piece as persistently failed and move to next loop iteration
- Add exponential backoff: `time.sleep(2 ** retry_count)` before re-submit
- **Risk:** More complexity in the future-management logic; need to ensure futures dict is updated correctly
- **Test cases:** Single seeder with 50% packet drop; verify all pieces eventually download

---

### Fix 2: Read loop in uploader + recv accumulation fix
**Files:** `peer.py`  
**Functions:** `handle_peer_request`, `download_piece`  
**Priority:** Critical

**New logic in `handle_peer_request`:**
```python
buffer = b""
while b"\n" not in buffer:
    chunk = client_socket.recv(256)
    if not chunk:
        return
    buffer += chunk
message = buffer.decode().strip()
```
Use `\n`-terminated messages. Leecher appends `\n` to request: `f"GET_PIECE:{piece_index}\n"`.

**New logic in `download_piece`:**
- After `break` on empty chunk, check `len(piece_data) < actual_piece_size` and raise `IOError("Partial read: got X bytes, expected Y")` instead of relying on hash check alone.

**Risk:** Protocol change — all peers must be updated simultaneously.  
**Test cases:** Limit TCP send buffer to 1 byte, verify full piece is received correctly.

---

### Fix 3: Socket timeout on accepted connections in uploader
**Files:** `peer.py`  
**Function:** `run_uploader`  
**Priority:** High

**New logic:**
```python
client_socket, addr = secure_socket.accept()
client_socket.settimeout(30)   # 30s per-connection timeout
executor.submit(handle_peer_request, ...)
```
**Risk:** None significant.  
**Test cases:** Connect to uploader without sending a request; verify thread is released after 30s.

---

### Fix 4: Fix `read_piece` short-read accounting
**Files:** `peer.py`  
**Function:** `LogicalFileStream.read_piece`  
**Priority:** Critical

**New logic:**
```python
chunk = fd.read(read_size)
actual_read = len(chunk)
data += chunk
bytes_left -= actual_read      # use actual, not intended
curr_offset += actual_read
if actual_read < read_size:
    raise IOError(f"Short read in {f['path']}: got {actual_read}, expected {read_size}")
```
**Risk:** Will surface previously-silent errors; existing code that called `read_piece` without error handling needs `try/except`.  
**Test cases:** Truncate a seeder file to half its declared size; verify error is raised and logged, not silently served.

---

### Fix 5: DHT thread safety — add a `threading.Lock` to `MiniDHTNode`
**Files:** `mini_dht.py`  
**Functions:** `_listen`, `_cleanup`, `announce`, `find_peers`  
**Priority:** High

**New logic:**
```python
self.store_lock = threading.Lock()
self.nodes_lock = threading.Lock()
```
Wrap all reads and writes to `self.store` and `self.known_nodes` with these locks.  
**Risk:** Slight performance overhead; potential deadlock if nested (verify no nested lock acquisition).  
**Test cases:** 10 peers all announcing to one bootstrap node simultaneously for 60 seconds; verify no RuntimeError.

---

### Fix 6: `find_peers` — event-based response waiting instead of fixed sleep
**Files:** `mini_dht.py`  
**Functions:** `find_peers`, `_listen`  
**Priority:** High

**New logic:**
- Add `self._pending_finds = {}`: `{info_hash: threading.Event}`
- In `find_peers`: create Event, store in pending, send FIND_VALUE, `event.wait(timeout=3)`, then read store
- In `_listen`: on `FIND_VALUE_RESPONSE`, set the corresponding event
- **Risk:** Event cleanup on timeout; don't leak Events.  
**Test cases:** Measure peer discovery latency; verify no 1-second delay when response arrives in 50ms.

---

### Fix 7: Fix leecher/seeder path collision on same machine
**Files:** `peer.py`  
**Function:** `LogicalFileStream.__init__`  
**Priority:** High

**New logic:**
For leechers, write to a separate `downloads/` directory rather than `uploads/`. The seeder reads from `uploads/` and the leecher writes to `downloads/`. After completion, optionally move/copy to `uploads/` for re-seeding.  
**Risk:** UI needs updating to serve files from `downloads/`.  
**Test cases:** Run seeder and leecher on same machine with same metainfo; verify seeder file is not corrupted.

---

### Fix 8: Unconditional `completed` status in `app.py`
**Files:** `app.py`  
**Function:** `run_leecher`  
**Priority:** High

**New logic:**
```python
exit_code = process.wait()
if exit_code == 0 and pieces_done >= total_pieces:
    download_progress[file_name]["status"] = "completed"
else:
    download_progress[file_name]["status"] = "failed"
download_progress[file_name]["percent"] = round((pieces_done / total_pieces) * 100, 1)
```
Have `peer.py` exit with code 0 on success and non-zero on failure.  
**Risk:** Requires `peer.py` to signal success/failure via exit code.  
**Test cases:** Kill `peer.py` mid-download; verify UI shows "failed" not "completed".

---

### Fix 9: Validate `piece_index` in uploader
**Files:** `peer.py`  
**Function:** `handle_peer_request`  
**Priority:** Medium

**New logic:**
```python
num_pieces = (total_size + piece_size - 1) // piece_size
if not (0 <= piece_index < num_pieces):
    client_socket.sendall(b"ERROR:INVALID_PIECE\n")
    return
```
**Risk:** Leecher must handle error response. Leecher `download_piece` should check for `b"ERROR:"` prefix before treating data as piece content.

---

### Fix 10: Add end-to-end file hash to metainfo
**Files:** `metainfo_generator.py`, `peer.py`  
**Priority:** High

**New logic in generator:**
```python
# After all pieces are hashed:
full_hash = hashlib.sha1()
for piece_hash in piece_hashes:
    full_hash.update(bytes.fromhex(piece_hash))
metainfo["root_hash"] = full_hash.hexdigest()
```
**New logic in `run_downloader` after all pieces done:**
```python
computed = hashlib.sha1()
for i in range(num_pieces):
    computed.update(bytes.fromhex(piece_hashes[i]))
if computed.hexdigest() != metainfo.get("root_hash"):
    console.log("[red]FINAL INTEGRITY CHECK FAILED[/red]")
```
**Risk:** Backward-incompatible metainfo format change; old `.json` files won't have `root_hash`.

---

### Summary Table

| Issue ID | Severity | Fix # | Component |
|----------|----------|-------|-----------|
| DL-1 | 🔴 Critical | Fix 1 | Retry logic |
| NET-1 | 🔴 Critical | Fix 2 | Recv accumulation |
| FS-2 | 🔴 Critical | Fix 4 | Short-read accounting |
| UP-1 | 🔴 Critical | Fix 2 | Request framing |
| CON-3 | 🔴 Critical | Fix 4 | Write verification |
| PD-2 | 🟠 High | Fix 5 | DHT thread safety |
| PD-3 | 🟠 High | Fix 5 | DHT cleanup lock |
| UP-2 | 🟠 High | Fix 3 | Uploader timeout |
| DL-3 | 🟠 High | Fix 1 | Thread pool sizing |
| DL-4 | 🟠 High | Fix 1 | Peer rotation |
| FS-1 | 🟠 High | Fix 7 | Path isolation |
| FS-5 | 🟠 High | Fix 7 | Path isolation |
| IV-3 | 🟠 High | Fix 10 | End-to-end hash |
| REC-1 | 🟠 High | Fix 8 | Progress tracking |
| REC-2 | 🟠 High | Fix 8 | Crash status |
| PD-1 | 🔴 Critical | Fix 6 | DHT find timing |
| REC-3 | 🟡 Medium | — | Port collision |
| PD-4 | 🟡 Medium | — | Multi-host support |
| UP-4 | 🟡 Medium | Fix 9 | Index validation |
