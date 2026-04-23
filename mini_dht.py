import socket
import json
import threading
import time
import hashlib

# Kademlia-inspired Mini DHT
# Uses UDP for communication. Keys are SHA1 hashes.

class MiniDHTNode:
    def __init__(self, host, port, bootstrap_node=None):
        self.host = host
        self.port = port
        self.node_id = hashlib.sha1(f"{host}:{port}".encode()).hexdigest()
        self.known_nodes = set()
        
        # store[info_hash][peer_addr] = {"ts": timestamp, "pieces": [...]}
        self.store = {}
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.host, self.port))
        
        if bootstrap_node:
            self.known_nodes.add(bootstrap_node)
            
        self.running = True
        self.listener_thread = threading.Thread(target=self._listen, daemon=True)
        self.listener_thread.start()
        
        self.cleanup_thread = threading.Thread(target=self._cleanup, daemon=True)
        self.cleanup_thread.start()

        # Ping bootstrap node to join network
        if bootstrap_node:
            self.ping(bootstrap_node)

    def _send(self, msg, addr):
        try:
            self.sock.sendto(json.dumps(msg).encode(), addr)
        except Exception:
            pass

    def _listen(self):
        while self.running:
            try:
                data, addr = self.sock.recvfrom(4096)
                msg = json.loads(data.decode())
                
                # Automatically add any node that contacts us to our routing table
                if addr != (self.host, self.port):
                    self.known_nodes.add(addr)

                msg_type = msg.get("type")
                
                if msg_type == "PING":
                    self._send({"type": "PONG", "node_id": self.node_id}, addr)
                    
                elif msg_type == "PONG":
                    # Already added to known_nodes
                    pass
                    
                elif msg_type == "STORE":
                    info_hash = msg.get("info_hash")
                    peer_addr = msg.get("peer_addr")
                    pieces = msg.get("pieces", [])
                    
                    if info_hash not in self.store:
                        self.store[info_hash] = {}
                        
                    self.store[info_hash][peer_addr] = {"ts": time.time(), "pieces": pieces}
                    
                elif msg_type == "FIND_VALUE":
                    info_hash = msg.get("info_hash")
                    if info_hash in self.store:
                        now = time.time()
                        peers = [
                            {"addr": p, "pieces": data["pieces"]}
                            for p, data in self.store[info_hash].items()
                            if now - data["ts"] <= 90
                        ]
                        self._send({"type": "FIND_VALUE_RESPONSE", "info_hash": info_hash, "peers": peers}, addr)
                    else:
                        # Return known nodes so the requester can ask them
                        nodes_list = [f"{ip}:{port}" for ip, port in self.known_nodes][:10]
                        self._send({"type": "FIND_VALUE_RESPONSE", "info_hash": info_hash, "nodes": nodes_list}, addr)
                        
                elif msg_type == "FIND_VALUE_RESPONSE":
                    info_hash = msg.get("info_hash")
                    if info_hash and "peers" in msg:
                        if info_hash not in self.store:
                            self.store[info_hash] = {}
                        for p in msg["peers"]:
                            self.store[info_hash][p["addr"]] = {"ts": time.time(), "pieces": p["pieces"]}
                    if "nodes" in msg:
                        for n in msg["nodes"]:
                            try:
                                ip, port = n.split(':')
                                self.known_nodes.add((ip, int(port)))
                            except Exception: pass

            except Exception:
                pass

    def _cleanup(self):
        while self.running:
            time.sleep(30)
            now = time.time()
            for info_hash in list(self.store.keys()):
                stale = [p for p, data in self.store[info_hash].items() if now - data["ts"] > 90]
                for p in stale:
                    del self.store[info_hash][p]
                if not self.store[info_hash]:
                    del self.store[info_hash]

    def ping(self, addr):
        self._send({"type": "PING", "node_id": self.node_id}, addr)

    def announce(self, info_hash, peer_port, pieces):
        # Broadcast our STORE message to all known nodes
        # (In a real DHT, we'd only send to the K closest nodes)
        peer_addr = f"127.0.0.1:{peer_port}"
        msg = {
            "type": "STORE",
            "info_hash": info_hash,
            "peer_addr": peer_addr,
            "pieces": pieces
        }
        
        # Store locally too
        if info_hash not in self.store:
            self.store[info_hash] = {}
        self.store[info_hash][peer_addr] = {"ts": time.time(), "pieces": pieces}

        for node in list(self.known_nodes):
            self._send(msg, node)

    def find_peers(self, info_hash):
        # Broadcast FIND_VALUE to all known nodes
        msg = {"type": "FIND_VALUE", "info_hash": info_hash}
        for node in list(self.known_nodes):
            self._send(msg, node)
            
        # Give network time to respond
        time.sleep(1)
        
        now = time.time()
        peers = []
        if info_hash in self.store:
            peers = [
                {"addr": p, "pieces": data["pieces"]}
                for p, data in self.store[info_hash].items()
                if now - data["ts"] <= 90
            ]
        return peers

    def stop(self):
        self.running = False
        self.sock.close()

if __name__ == "__main__":
    # Test harness
    node1 = MiniDHTNode("127.0.0.1", 8468)
    node2 = MiniDHTNode("127.0.0.1", 8469, bootstrap_node=("127.0.0.1", 8468))
    
    time.sleep(1)
    node2.announce("hash123", 6882, [0, 1, 2])
    time.sleep(1)
    print(node1.find_peers("hash123"))
    
    node1.stop()
    node2.stop()
