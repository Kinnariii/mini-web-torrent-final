"""
metainfo_generator.py  —  mini-webtorrent
Generates a .json metainfo file (analogous to a .torrent file).

Usage:
    python metainfo_generator.py <file> [--tracker URL] [--piece-size KB]

Example:
    python metainfo_generator.py video.mp4 --tracker http://127.0.0.1:5000/tracker --piece-size 512
"""

import hashlib
import json
import os
import argparse

def get_long_path(p):
    abs_p = os.path.abspath(p)
    if os.name == 'nt' and not abs_p.startswith('\\\\?\\'):
        return '\\\\?\\' + abs_p
    return abs_p

def create_metainfo(target_path: str, piece_size_kb: int):
    piece_size = piece_size_kb * 1024
    piece_hashes = []

    long_target = get_long_path(target_path)
    if not os.path.exists(long_target):
        print(f"[Error] Path '{target_path}' not found.")
        return

    is_dir = os.path.isdir(long_target)
    target_name = os.path.basename(os.path.abspath(target_path))
    
    files = []
    total_size = 0
    
    if is_dir:
        for root, _, filenames in os.walk(long_target):
            for fn in filenames:
                full_path_long = os.path.join(root, fn)
                # Ensure relative path is calculated from the original target path
                rel_path = os.path.relpath(full_path_long, long_target)
                rel_path = rel_path.replace("\\", "/")
                size = os.path.getsize(full_path_long)
                files.append({"path": rel_path, "length": size, "full_path": full_path_long})
                total_size += size
    else:
        size = os.path.getsize(long_target)
        files.append({"path": target_name, "length": size, "full_path": long_target})
        total_size += size

    print(f"Target    : {target_path}")
    print(f"Total Size: {total_size / 1024:.1f} KB  ({total_size} bytes)")
    print(f"Piece size: {piece_size_kb} KB")

    # Generate piece hashes over the logical byte stream
    current_piece = b""
    for f in files:
        with open(f["full_path"], "rb") as fd:
            while True:
                chunk = fd.read(piece_size - len(current_piece))
                if not chunk:
                    break
                current_piece += chunk
                if len(current_piece) == piece_size:
                    piece_hashes.append(hashlib.sha1(current_piece).hexdigest())
                    current_piece = b""
                    
    if current_piece:
        piece_hashes.append(hashlib.sha1(current_piece).hexdigest())

    # Remove "full_path" before saving
    for f in files:
        del f["full_path"]

    metainfo = {
        "name":          target_name,
        "is_dir":        is_dir,
        "total_size":    total_size,
        "piece_size":    piece_size,
        "piece_hashes":  piece_hashes,
        "files":         files
    }
    
    # Generate an info_hash to identify this torrent uniquely in the DHT
    info_str = json.dumps({"name": target_name, "files": files, "piece_size": piece_size}, sort_keys=True)
    metainfo["info_hash"] = hashlib.sha1(info_str.encode()).hexdigest()

    out_path = f"{target_name}.json"
    with open(out_path, "w") as f:
        json.dump(metainfo, f, indent=4)

    print(f"\nMetainfo  : {out_path}")
    print(f"Pieces    : {len(piece_hashes)}")
    print(f"Info Hash : {metainfo['info_hash']}")
    print("Done! Share the .json file with all peers.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate mini-webtorrent metainfo file for files or directories")
    parser.add_argument("target", help="Path to the file or directory to share")
    parser.add_argument(
        "--piece-size",
        type=int,
        default=256,
        help="Piece size in KB (default: 256)",
    )
    args = parser.parse_args()
    create_metainfo(args.target, args.piece_size)
