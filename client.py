"""
Client cho Hệ Thống Lưu Trữ Phân Tán Key-Value
Cung cấp giao diện để tương tác với cluster
"""

import socket
import json
from typing import Optional, List, Tuple
import time


class KVStoreClient:
    """
    Client cho Hệ Thống Lưu Trữ Phân Tán Key-Value
    
    Tính năng:
    - Tự động failover sang nodes khỏe mạnh
    - Retry logic có thể cấu hình
    - Theo dõi thống kê
    """
    
    def __init__(self, cac_node: List[Tuple[str, int]], timeout: float = 5.0):
        """
        Khởi tạo client với danh sách các cluster nodes
        
        Tham số:
            cac_node: Danh sách các tuples (host, port) cho cluster nodes
            timeout: Socket timeout tính bằng giây
        """
        self.cac_node = cac_node
        self.chi_so_node_hien_tai = 0
        self.timeout = timeout
        
        # Thống kê
        self.thong_ke = {
            'so_request': 0,
            'thanh_cong': 0,
            'that_bai': 0,
            'so_lan_thu_lai': 0
        }
    
    def _gui_request(self, request: dict, thu_lai: bool = True) -> dict:
        """
        Gửi request đến một cluster node
        
        Thực hiện retry logic:
        1. Thử node hiện tại
        2. Nếu thất bại và retry được bật, thử các node khác
        3. Trả về response hoặc error
        """
        self.thong_ke['so_request'] += 1
        
        # Thử node hiện tại trước, sau đó thử các node khác nếu retry được bật
        so_lan_thu_toi_da = len(self.cac_node) if thu_lai else 1
        
        for lan_thu in range(so_lan_thu_toi_da):
            chi_so_node = (self.chi_so_node_hien_tai + lan_thu) % len(self.cac_node)
            host, port = self.cac_node[chi_so_node]
            
            try:
                # Tạo socket với timeout
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.timeout)
                sock.connect((host, port))
                
                # Gửi request
                sock.sendall(json.dumps(request).encode() + b"\n")
                
                # Nhận response
                response_data = b""
                while True:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    response_data += chunk
                    if b"\n" in response_data:
                        break
                
                sock.close()
                
                # Cập nhật node hiện tại khi thành công
                self.chi_so_node_hien_tai = chi_so_node
                self.thong_ke['thanh_cong'] += 1
                
                return json.loads(response_data.decode())
                
            except socket.timeout:
                if lan_thu > 0:
                    self.thong_ke['so_lan_thu_lai'] += 1
                print(f"⚠ Timeout kết nối tới {host}:{port}")
                continue
                
            except ConnectionRefusedError:
                if lan_thu > 0:
                    self.thong_ke['so_lan_thu_lai'] += 1
                print(f"⚠ Kết nối bị từ chối bởi {host}:{port}")
                continue
                
            except Exception as e:
                if lan_thu > 0:
                    self.thong_ke['so_lan_thu_lai'] += 1
                print(f"⚠ Lỗi giao tiếp với {host}:{port}: {e}")
                continue
        
        # Tất cả các lần thử đều thất bại
        self.thong_ke['that_bai'] += 1
        return {"status": "error", "message": "Tất cả nodes không khả dụng"}
    
    def put(self, key: str, value: str, hien_thi: bool = True) -> bool:
        """
        Lưu trữ một cặp key-value
        
        Tham số:
            key: Key cần lưu
            value: Value cần lưu
            hien_thi: Có hiển thị kết quả không
            
        Trả về:
            True nếu thành công, False nếu thất bại
        """
        request = {
            "command": "PUT",
            "key": key,
            "value": value
        }
        
        response = self._gui_request(request)
        
        if response.get("status") == "success":
            if hien_thi:
                print(f"✓ PUT {key} = {value}")
            return True
        else:
            if hien_thi:
                print(f"✗ PUT thất bại: {response.get('message', 'Lỗi không xác định')}")
            return False
    
    def get(self, key: str, hien_thi: bool = True) -> Optional[str]:
        """
        Lấy value cho một key
        
        Tham số:
            key: Key cần lấy
            hien_thi: Có hiển thị kết quả không
            
        Trả về:
            Value nếu tìm thấy, None nếu không tìm thấy
        """
        request = {
            "command": "GET",
            "key": key
        }
        
        response = self._gui_request(request)
        
        if response.get("status") == "success":
            value = response.get("value")
            if hien_thi:
                print(f"✓ GET {key} = {value}")
            return value
        else:
            if hien_thi:
                print(f"✗ GET thất bại: {response.get('message', 'Lỗi không xác định')}")
            return None
    
    def delete(self, key: str, hien_thi: bool = True) -> bool:
        """
        Xóa một key
        
        Tham số:
            key: Key cần xóa
            hien_thi: Có hiển thị kết quả không
            
        Trả về:
            True nếu thành công, False nếu thất bại
        """
        request = {
            "command": "DELETE",
            "key": key
        }
        
        response = self._gui_request(request)
        
        if response.get("status") == "success":
            if hien_thi:
                print(f"✓ DELETE {key}")
            return True
        else:
            if hien_thi:
                print(f"✗ DELETE thất bại: {response.get('message', 'Lỗi không xác định')}")
            return False
    
    def lay_thong_ke_node(self, chi_so_node: int = None) -> Optional[dict]:
        """
        Lấy thống kê từ một node cụ thể
        
        Tham số:
            chi_so_node: Chỉ số của node (None = node hiện tại)
            
        Trả về:
            Dictionary thống kê hoặc None
        """
        if chi_so_node is not None:
            chi_so_cu = self.chi_so_node_hien_tai
            self.chi_so_node_hien_tai = chi_so_node
        
        request = {"command": "GET_STATS"}
        response = self._gui_request(request, thu_lai=False)
        
        if chi_so_node is not None:
            self.chi_so_node_hien_tai = chi_so_cu
        
        if response.get("status") == "success":
            return response.get("stats")
        return None
    
    def lay_thong_ke_client(self) -> dict:
        """
        Lấy thống kê phía client
        
        Trả về:
            Dictionary với thống kê client
        """
        return dict(self.thong_ke)
    
    def hien_thi_trang_thai_cluster(self):
        """
        Hiển thị trạng thái của tất cả nodes trong cluster
        """
        print("\n" + "=" * 60)
        print("TRẠNG THÁI CLUSTER")
        print("=" * 60)
        
        for i, (host, port) in enumerate(self.cac_node):
            thong_ke = self.lay_thong_ke_node(i)
            
            if thong_ke:
                print(f"\n[Node {i+1}] {host}:{port} ✓ ONLINE")
                print(f"  Thời gian hoạt động: {thong_ke.get('thoi_gian_hoat_dong', 0):.1f}s")
                print(f"  Dữ liệu: {thong_ke.get('so_key', 0)} keys")
                print(f"  Peers: {thong_ke.get('so_peer', 0)}")
                print(f"  Thao tác: PUT={thong_ke.get('so_lan_put', 0)}, "
                      f"GET={thong_ke.get('so_lan_get', 0)}, "
                      f"DEL={thong_ke.get('so_lan_delete', 0)}")
                print(f"  Nhân bản: {thong_ke.get('so_lan_nhan_ban', 0)}")
            else:
                print(f"\n[Node {i+1}] {host}:{port} ✗ OFFLINE")
        
        print("\n" + "=" * 60)


def interactive_client():
    """
    Client dòng lệnh tương tác với tính năng nâng cao
    """
    print("=" * 60)
    print("HỆ THỐNG LƯU TRỮ PHÂN TÁN KEY-VALUE - CLIENT v2.0")
    print("=" * 60)
    
    # Lấy cấu hình cluster
    print("\nCấu hình Cluster:")
    print("Nhập địa chỉ node (định dạng: host:port)")
    print("Nhấn Enter trên dòng trống để sử dụng mặc định")
    print("Mặc định: localhost:5001, localhost:5002, localhost:5003")
    print()
    
    cac_node = []
    while True:
        line = input(f"Node {len(cac_node) + 1}: ").strip()
        if not line:
            break
        
        try:
            host, port = line.split(":")
            cac_node.append((host.strip(), int(port.strip())))
        except ValueError:
            print("⚠ Định dạng không hợp lệ. Sử dụng host:port (ví dụ: localhost:5001)")
    
    if not cac_node:
        cac_node = [
            ("localhost", 5001),
            ("localhost", 5002),
            ("localhost", 5003)
        ]
    
    client = KVStoreClient(cac_node)
    
    print(f"\n✓ Đã kết nối tới cluster với {len(cac_node)} node(s)")
    print("\nCác lệnh có sẵn:")
    print("  PUT <key> <value>    - Lưu trữ cặp key-value")
    print("  GET <key>            - Lấy value cho key")
    print("  DELETE <key>         - Xóa một key")
    print("  STATUS               - Hiển thị trạng thái cluster")
    print("  STATS                - Hiển thị thống kê client")
    print("  HELP                 - Hiển thị trợ giúp này")
    print("  QUIT / EXIT          - Thoát client")
    print()
    
    while True:
        try:
            command = input("> ").strip()
            
            if not command:
                continue
            
            parts = command.split(maxsplit=2)
            cmd = parts[0].upper()
            
            if cmd in ("QUIT", "EXIT"):
                print("Tạm biệt!")
                break
            
            elif cmd == "HELP":
                print("\nCác lệnh có sẵn:")
                print("  PUT <key> <value>    - Lưu trữ cặp key-value")
                print("  GET <key>            - Lấy value cho key")
                print("  DELETE <key>         - Xóa một key")
                print("  STATUS               - Hiển thị trạng thái cluster")
                print("  STATS                - Hiển thị thống kê client")
                print("  HELP                 - Hiển thị trợ giúp này")
                print("  QUIT / EXIT          - Thoát client")
            
            elif cmd == "STATUS":
                client.hien_thi_trang_thai_cluster()
            
            elif cmd == "STATS":
                thong_ke = client.lay_thong_ke_client()
                print("\nThống kê Client:")
                print(f"  Tổng số requests: {thong_ke['so_request']}")
                print(f"  Thành công: {thong_ke['thanh_cong']}")
                print(f"  Thất bại: {thong_ke['that_bai']}")
                print(f"  Số lần thử lại: {thong_ke['so_lan_thu_lai']}")
                if thong_ke['so_request'] > 0:
                    ty_le_thanh_cong = (thong_ke['thanh_cong'] / thong_ke['so_request']) * 100
                    print(f"  Tỷ lệ thành công: {ty_le_thanh_cong:.1f}%")
            
            elif cmd == "PUT":
                if len(parts) < 3:
                    print("⚠ Cách dùng: PUT <key> <value>")
                    continue
                
                key = parts[1]
                value = parts[2]
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
            
            else:
                print(f"⚠ Lệnh không xác định: {cmd}")
                print("Gõ HELP để xem các lệnh có sẵn")
        
        except KeyboardInterrupt:
            print("\nTạm biệt!")
            break
        except Exception as e:
            print(f"⚠ Lỗi: {e}")


if __name__ == "__main__":
    interactive_client()