# Advanced P2P Features Implemented

The mini-webtorrent project has been upgraded with two critical advanced features:

## 1. Rarest-First Algorithm

The downloader now acts much smarter instead of blindly guessing which piece to download next.

**How it works:**
1. **Bitfield Announcement**: Whenever `peer.py` contacts the tracker, it sends a comma-separated list of the pieces it successfully holds.
2. **Tracker Syncing**: `tracker.py` now stores this "bitfield" along with the peer's IP and port. When a peer asks for the peer list, the tracker returns a complex dictionary showing exactly which peer has which piece.
3. **Rarest-First Priority**: The downloader loops through all the tracker data and counts how many peers have each missing piece. It sorts the missing pieces so that the rarest pieces (the ones fewest peers have) are downloaded first. If no peers have a piece, it safely skips it and tries again on the next announce cycle.

> [!TIP]
> This drastically improves download times in swarms by ensuring rare pieces don't go extinct!

## 2. P2P TLS Encryption

Peer-to-peer data transfers are now fully encrypted via TLS, preventing Man-in-the-Middle eavesdropping.

**How it works:**
1. **On-the-fly Certificates**: When `peer.py` starts up, it automatically uses your system's OpenSSL to silently generate a generic, self-signed SSL certificate (`cert.pem`) and private key (`key.pem`) valid for 365 days.
2. **Encrypted Uploader**: The uploader's TCP server socket is wrapped in an `ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)`.
3. **Encrypted Downloader**: The downloader wraps its outgoing TCP client socket using `ssl.create_default_context()`. Because this is a decentralized P2P network, it deliberately bypasses strict CA hostname validation (`ssl.CERT_NONE`), ensuring seamless, encrypted connections between unknown peers.

> [!IMPORTANT]
> Because the tracker protocol and peer socket connections have changed significantly, **you must completely restart both `tracker.py` and `app.py`** in your terminals for the changes to take effect. Old active seeders from before this update will not be able to talk to new downloaders.
