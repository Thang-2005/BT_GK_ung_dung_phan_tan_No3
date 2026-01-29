"""
CLI Client Với Chức Năng Chọn Node
Cho phép test requests đến các nodes cụ thể trong cluster
"""

import json
from client import KVStoreClient

# Các nodes mặc định trong cluster
NODES = [
    ("127.0.0.1", 5001),
    ("127.0.0.1", 5002),
    ("127.0.0.1", 5003)
]


def main():
    """CLI client chính với chức năng chọn node"""
    chi_so_node_hien_tai = 0
    client = KVStoreClient([NODES[chi_so_node_hien_tai]])
    
    print("=" * 60)
    print("CLI Hệ Thống KV Phân Tán - Chế Độ Chọn Node")
    print("=" * 60)
    print("\nCác Nodes Trong Cluster:")
    for i, (host, port) in enumerate(NODES):
        print(f"  {i+1}. {host}:{port}")
    
    print(f"\nNode Hiện Tại: Node {chi_so_node_hien_tai + 1}")
    print("\nCác Lệnh:")
    print("  NODE <1|2|3>         - Chuyển sang node cụ thể")
    print("  PUT <key> <value>    - Lưu trữ cặp key-value")
    print("  GET <key>            - Lấy value")
    print("  DELETE <key>         - Xóa key")
    print("  STATUS               - Hiển thị trạng thái cluster")
    print("  HELP                 - Hiển thị trợ giúp này")
    print("  EXIT / QUIT          - Thoát")
    print()
    
    while True:
        try:
            command = input(f"[Node {chi_so_node_hien_tai + 1}] >> ").strip()
            
            if not command:
                continue
            
            parts = command.split(maxsplit=2)
            cmd = parts[0].upper()
            
            if cmd == "NODE":
                if len(parts) < 2:
                    print("⚠ Cách dùng: NODE <1|2|3>")
                    continue
                
                try:
                    idx = int(parts[1]) - 1
                    if 0 <= idx < len(NODES):
                        chi_so_node_hien_tai = idx
                        client = KVStoreClient([NODES[chi_so_node_hien_tai]])
                        print(f"✓ Đã chuyển sang Node {idx + 1} ({NODES[idx][0]}:{NODES[idx][1]})")
                    else:
                        print(f"⚠ Chỉ số node không hợp lệ. Sử dụng 1-{len(NODES)}")
                except ValueError:
                    print("⚠ Số node không hợp lệ")
            
            elif cmd == "PUT":
                if len(parts) < 3:
                    print("⚠ Cách dùng: PUT <key> <value>")
                    continue
                
                key, value = parts[1], parts[2]
                client.put(key, value)
            
            elif cmd == "GET":
                if len(parts) < 2:
                    print("⚠ Cách dùng: GET <key>")
                    continue
                
                key = parts[1]
                client.get(key)
            
            elif cmd == "DELETE":
                if len(parts) < 2:
                    print("⚠ Cách dùng: DELETE <key>")
                    continue
                
                key = parts[1]
                client.delete(key)
            
            elif cmd == "STATUS":
                client_day_du = KVStoreClient(NODES)
                client_day_du.hien_thi_trang_thai_cluster()
            
            elif cmd == "HELP":
                print("\nCác Lệnh:")
                print("  NODE <1|2|3>         - Chuyển sang node cụ thể")
                print("  PUT <key> <value>    - Lưu trữ cặp key-value")
                print("  GET <key>            - Lấy value")
                print("  DELETE <key>         - Xóa key")
                print("  STATUS               - Hiển thị trạng thái cluster")
                print("  HELP                 - Hiển thị trợ giúp này")
                print("  EXIT / QUIT          - Thoát")
            
            elif cmd in ("QUIT", "EXIT"):
                print("Tạm biệt!")
                break
            
            else:
                print(f"⚠ Lệnh không xác định: {cmd}")
                print("Gõ HELP để xem các lệnh có sẵn")
        
        except KeyboardInterrupt:
            print("\nTạm biệt!")
            break
        except Exception as e:
            print(f"⚠ Lỗi: {e}")


if __name__ == "__main__":
    main()