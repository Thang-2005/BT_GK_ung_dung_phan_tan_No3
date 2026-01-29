"""
Hệ Thống Lưu Trữ Phân Tán Key-Value - Node
"""

import socket
import json
import threading
import time
import hashlib
from typing import Dict, Tuple, List, Optional
import logging
from datetime import datetime

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('node.log'),
        logging.StreamHandler()
    ]
)


class Node:
    """
    Node của Hệ Thống Lưu Trữ Phân Tán Key-Value
    
    Tính năng:
    - Consistent hashing để phân phối dữ liệu
    - Nhân bản (replication) để chịu lỗi
    - Cơ chế heartbeat để phát hiện lỗi
    - Tự động khôi phục và đồng bộ dữ liệu
    - Thread-safe operations
    """
    
    def __init__(self, node_id: str, host: str, port: int, he_so_nhan_ban: int = 2):
        """
        Khởi tạo node mới
        
        Tham số:
            node_id: ID duy nhất của node này
            host: Địa chỉ host để bind
            port: Cổng để lắng nghe
            he_so_nhan_ban: Số lượng bản sao cho mỗi key (mặc định = 2)
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.he_so_nhan_ban = he_so_nhan_ban
        
        # Lưu trữ dữ liệu với thread-safe
        self.du_lieu: Dict[str, str] = {}
        self.khoa_du_lieu = threading.Lock()
        
        # Thông tin về các node khác (peers)
        self.cac_node_khac: Dict[str, Tuple[str, int]] = {}
        self.khoa_node_khac = threading.Lock()
        
        # Theo dõi heartbeat
        self.heartbeat_cuoi: Dict[str, float] = {}
        self.khoa_heartbeat = threading.Lock()
        self.thoi_gian_timeout_heartbeat = 10  # giây
        self.khoang_thoi_gian_heartbeat = 3  # giây
        
        # Trạng thái node
        self.dang_chay = False
        self.server_socket: Optional[socket.socket] = None
        self.dang_phuc_hoi = False
        
        # Thống kê
        self.thong_ke = {
            'so_lan_put': 0,
            'so_lan_get': 0,
            'so_lan_delete': 0,
            'so_lan_nhan_ban': 0,
            'so_lan_chuyen_tiep': 0,
            'thoi_gian_bat_dau': time.time()
        }
        self.khoa_thong_ke = threading.Lock()
        
        # Logger
        self.logger = logging.getLogger(f"Node-{node_id}")
        self.logger.info(f"✓ Node đã khởi tạo: {node_id} tại {host}:{port}")
    
    def hash_key(self, key: str) -> int:
        """
        Hash một key để xác định vị trí trên vòng hash
        
        Giải thích: Sử dụng MD5 để hash key thành số nguyên
        Số này sẽ xác định vị trí của key trên vòng tròn hash
        """
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def hash_node(self, node_id: str) -> int:
        """
        Hash một node ID để xác định vị trí trên vòng hash
        
        Giải thích: Tương tự hash_key, nhưng dùng cho node ID
        """
        return int(hashlib.md5(node_id.encode()).hexdigest(), 16)
    
    # def lay_cac_node_chiu_trach_nhiem(self, key: str) -> List[str]:
    #     """
    #     Sử dụng consistent hashing để tìm các node chịu trách nhiệm cho một key
        
    #     FIX QUAN TRỌNG: Bây giờ xét đúng tất cả các node trong cluster
        
    #     Thuật toán:
    #     1. Hash key để lấy vị trí của nó
    #     2. Tìm N node gần nhất theo chiều kim đồng hồ trên vòng
    #     3. Trả về tối đa he_so_nhan_ban nodes
        
    #     Ví dụ: Nếu có 3 nodes và replication_factor=2
    #     - Key "name" hash ra 12345 -> sẽ tìm 2 nodes gần nhất >= 12345
    #     """
        # with self.khoa_node_khac:
        #     # BAO GỒM CHÍNH NODE NÀY trong danh sách tất cả nodes
        #     # tat_ca_cac_node = [self.node_id] + list(self.cac_node_khac.keys())
        #     tat_ca_cac_node = sorted([self.node_id] + list(self.cac_node_khac.keys()))
            
        #     if len(tat_ca_cac_node) == 1:
        #         return [self.node_id]
            
        #     key_hash = self.hash_key(key)
            
        #     # Tạo danh sách (hash, node_id) và sắp xếp
        #     cac_node_hash = [(self.hash_node(nid), nid) for nid in tat_ca_cac_node]
        #     cac_node_hash.sort()
            
        #     # Tìm nodes theo chiều kim đồng hồ từ vị trí key
        #     cac_node_chiu_trach_nhiem = []
            
        #     # Lượt 1: Tìm nodes có hash >= key_hash
        #     for node_hash, nid in cac_node_hash:
        #         if node_hash >= key_hash:
        #             cac_node_chiu_trach_nhiem.append(nid)
        #             if len(cac_node_chiu_trach_nhiem) >= self.he_so_nhan_ban:
        #                 break
            
        #     # Lượt 2: Nếu chưa đủ, quay vòng lại từ đầu
        #     if len(cac_node_chiu_trach_nhiem) < self.he_so_nhan_ban:
        #         for node_hash, nid in cac_node_hash:
        #             if nid not in cac_node_chiu_trach_nhiem:
        #                 cac_node_chiu_trach_nhiem.append(nid)
        #                 if len(cac_node_chiu_trach_nhiem) >= self.he_so_nhan_ban:
        #                     break
            
        #     ket_qua = cac_node_chiu_trach_nhiem[:self.he_so_nhan_ban]
        #     self.logger.debug(f"Key '{key}' -> Các node chịu trách nhiệm: {ket_qua}")
        #     return ket_qua

    def lay_cac_node_chiu_trach_nhiem(self, key: str) -> List[str]:
            with self.khoa_node_khac:
                # FIX: Sắp xếp ID để đảm bảo mọi Node có cùng một Vòng Băm (Hash Ring)
                tat_ca_cac_node = sorted([self.node_id] + list(self.cac_node_khac.keys()))
                
                if len(tat_ca_cac_node) <= self.he_so_nhan_ban:
                    return tat_ca_cac_node
                    
                key_hash = self.hash_key(key)
                cac_node_hash = sorted([(self.hash_node(nid), nid) for nid in tat_ca_cac_node])
                
                cac_node_chiu_trach_nhiem = []
                # Tìm node đầu tiên có hash >= key_hash
                start_idx = 0
                for i, (n_hash, nid) in enumerate(cac_node_hash):
                    if n_hash >= key_hash:
                        start_idx = i
                        break
                
                # Lấy N node kế tiếp theo chiều kim đồng hồ
                for i in range(len(cac_node_hash)):
                    idx = (start_idx + i) % len(cac_node_hash)
                    nid = cac_node_hash[idx][1]
                    if nid not in cac_node_chiu_trach_nhiem:
                        cac_node_chiu_trach_nhiem.append(nid)
                    if len(cac_node_chiu_trach_nhiem) >= self.he_so_nhan_ban:
                        break
                        
                return cac_node_chiu_trach_nhiem
    
    def bat_dau(self):
        """
        Khởi động node server
        
        Quy trình:
        1. Tạo và bind server socket
        2. Khởi động các background threads (heartbeat, failure detector)
        3. Vào vòng lặp chính để nhận client connections
        """
        self.dang_chay = True
        
        # Tạo server socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(10)
            self.logger.info(f"✓ Node đã khởi động thành công tại {self.host}:{self.port}")
        except OSError as e:
            self.logger.error(f"✗ Lỗi bind tới {self.host}:{self.port}: {e}")
            raise
        
        # Khởi động các background threads
        threading.Thread(target=self._thread_gui_heartbeat, daemon=True, name="GuiHeartbeat").start()
        threading.Thread(target=self._thread_phat_hien_loi, daemon=True, name="PhatHienLoi").start()
        threading.Thread(target=self._thread_bao_cao_thong_ke, daemon=True, name="BaoCaoThongKe").start()
        
        # FIX QUAN TRỌNG: Thêm thread đồng bộ định kỳ
        threading.Thread(target=self._thread_dong_bo_dinh_ky, daemon=True, name="DongBoDinhKy").start()
        
        self.logger.info("✓ Tất cả background threads đã khởi động")
        
        # Vòng lặp chính accept connections
        while self.dang_chay:
            try:
                self.server_socket.settimeout(1.0)
                client_socket, client_addr = self.server_socket.accept()
                self.logger.debug(f"Nhận kết nối từ {client_addr}")
                
                # Xử lý mỗi client trong thread riêng
                threading.Thread(
                    target=self._xu_ly_client,
                    args=(client_socket,),
                    daemon=True,
                    name=f"XuLyClient-{client_addr}"
                ).start()
                
            except socket.timeout:
                continue
            except Exception as e:
                if self.dang_chay:
                    self.logger.error(f"✗ Lỗi accept connection: {e}")
    
    def _xu_ly_client(self, client_socket: socket.socket):
        """
        Xử lý một client connection
        
        Quy trình:
        1. Nhận dữ liệu từ client
        2. Parse JSON request
        3. Xử lý request
        4. Gửi response
        """
        try:
            # Nhận dữ liệu request
            data = b""
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                data += chunk
                if b"\n" in data:
                    break
            
            if not data:
                return
            
            # Parse và xử lý request
            request = json.loads(data.decode())
            self.logger.debug(f"Nhận request: {request.get('command')}")
            
            response = self._xu_ly_request(request)
            
            # Gửi response
            client_socket.sendall(json.dumps(response).encode() + b"\n")
            
        except json.JSONDecodeError as e:
            self.logger.error(f"✗ JSON không hợp lệ: {e}")
            error_response = {"status": "error", "message": "JSON không hợp lệ"}
            client_socket.sendall(json.dumps(error_response).encode() + b"\n")
        except Exception as e:
            self.logger.error(f"✗ Lỗi xử lý client: {e}", exc_info=True)
        finally:
            try:
                client_socket.close()
            except:
                pass
    
    def _xu_ly_request(self, request: dict) -> dict:
        """
        Xử lý một client request
        
        Hỗ trợ các lệnh:
        - PUT: Lưu key-value
        - GET: Lấy value
        - DELETE: Xóa key
        - JOIN: Node mới tham gia cluster
        - HEARTBEAT: Kiểm tra node còn sống
        - REPLICATE: Nhân bản dữ liệu
        - GET_ALL_DATA: Lấy tất cả dữ liệu
        - SYNC_DATA: Đồng bộ dữ liệu
        - GET_STATS: Lấy thống kê
        """
        cmd = request.get("command")
        
        if cmd == "PUT":
            return self._xu_ly_put(request["key"], request["value"])
        elif cmd == "GET":
            return self._xu_ly_get(request["key"])
        elif cmd == "DELETE":
            return self._xu_ly_delete(request["key"])
        elif cmd == "JOIN":
            return self._xu_ly_join(request["node_id"], request["host"], request["port"])
        elif cmd == "HEARTBEAT":
            return self._xu_ly_heartbeat(request["node_id"])
        elif cmd == "REPLICATE":
            return self._xu_ly_nhan_ban(request["key"], request.get("value"))
        elif cmd == "GET_ALL_DATA":
            return self._xu_ly_lay_tat_ca_du_lieu()
        elif cmd == "SYNC_DATA":
            return self._xu_ly_dong_bo_du_lieu(request["data"])
        elif cmd == "GET_STATS":
            return self._xu_ly_lay_thong_ke()
        else:
            return {"status": "error", "message": f"Lệnh không xác định: {cmd}"}
    
    # ==================== CÁC THAO TÁC DỮ LIỆU ====================
    
    # def _xu_ly_put(self, key: str, value: str) -> dict:
    #     """
    #     Xử lý thao tác PUT
        
    #     Quy trình FIX:
    #     1. Tìm các node chịu trách nhiệm cho key này (dùng consistent hashing)
    #     2. Nếu node này KHÔNG chịu trách nhiệm -> chuyển tiếp đến node chính
    #     3. Nếu node này CHỊU TRÁCH NHIỆM:
    #        a. Lưu dữ liệu tại local
    #        b. Nhân bản đến TẤT CẢ các node chịu trách nhiệm khác
        
    #     Ví dụ: PUT name=John
    #     - Hash "name" -> ra node chịu trách nhiệm là [Node1, Node2]
    #     - Nếu request đến Node3 -> chuyển tiếp đến Node1
    #     - Node1 nhận request -> lưu local -> gửi REPLICATE đến Node2
    #     """
    #     cac_node_chiu_trach_nhiem = self.lay_cac_node_chiu_trach_nhiem(key)
        
    #     # Nếu node này KHÔNG chịu trách nhiệm -> chuyển tiếp
    #     if self.node_id not in cac_node_chiu_trach_nhiem:
    #         node_chinh = cac_node_chiu_trach_nhiem[0]
    #         if node_chinh in self.cac_node_khac:
    #             self.logger.info(f"→ Chuyển tiếp PUT {key} đến {node_chinh}")
    #             with self.khoa_thong_ke:
    #                 self.thong_ke['so_lan_chuyen_tiep'] += 1
    #             return self._chuyen_tiep_request(node_chinh, {"command": "PUT", "key": key, "value": value})
    #         return {"status": "error", "message": "Node chịu trách nhiệm không khả dụng"}
        
    #     # Lưu tại local
    #     with self.khoa_du_lieu:
    #         self.du_lieu[key] = value
        
    #     with self.khoa_thong_ke:
    #         self.thong_ke['so_lan_put'] += 1
        
    #     # FIX: Nhân bản đến TẤT CẢ các node chịu trách nhiệm khác
    #     for node_id in cac_node_chiu_trach_nhiem:
    #         if node_id != self.node_id and node_id in self.cac_node_khac:
    #             threading.Thread(
    #                 target=self._nhan_ban_den_node,
    #                 args=(node_id, key, value),
    #                 daemon=True,
    #                 name=f"NhanBan-{node_id}"
    #             ).start()
        
    #     self.logger.info(f"✓ PUT {key}={value}, đã nhân bản đến {cac_node_chiu_trach_nhiem}")
    #     return {"status": "success"}
    
    def _xu_ly_put(self, key: str, value: str) -> dict:
        responsible_nodes = self.lay_cac_node_chiu_trach_nhiem(key)
        if self.node_id not in responsible_nodes:
            node_chinh = responsible_nodes[0]
            if node_chinh in self.cac_node_khac:
                with self.khoa_thong_ke:
                    self.thong_ke['so_lan_chuyen_tiep'] += 1
                return self._chuyen_tiep_request(node_chinh, {
                    "command": "PUT",
                    "key": key,
                    "value": value
                })
            return {"status": "error", "message": "Node chính không khả dụng"}

        # Ghi local
        with self.khoa_du_lieu:
            self.du_lieu[key] = value
        with self.khoa_thong_ke:
            self.thong_ke['so_lan_put'] += 1

        # Nhân bản đến replica
        for nid in responsible_nodes:
            if nid != self.node_id and nid in self.cac_node_khac:
                threading.Thread(
                    target=self._nhan_ban_den_node,
                    args=(nid, key, value),
                    daemon=True
                ).start()

        return {"status": "success"}

    def _xu_ly_get(self, key: str) -> dict:
        """
        Xử lý thao tác GET
        
        Quy trình:
        1. Tìm các node chịu trách nhiệm cho key
        2. Nếu node này CÓ dữ liệu -> trả về
        3. Nếu KHÔNG có -> chuyển tiếp đến node chính
        
        Ví dụ: GET name
        - Kiểm tra local có "name" không
        - Nếu có -> trả về value
        - Nếu không -> chuyển tiếp đến node chịu trách nhiệm
        """
        cac_node_chiu_trach_nhiem = self.lay_cac_node_chiu_trach_nhiem(key)
        
        # Kiểm tra xem node này có phải chịu trách nhiệm không
        if self.node_id in cac_node_chiu_trach_nhiem:
            with self.khoa_du_lieu:
                value = self.du_lieu.get(key)
            
            with self.khoa_thong_ke:
                self.thong_ke['so_lan_get'] += 1
            
            if value is not None:
                self.logger.debug(f"✓ GET {key} = {value}")
                return {"status": "success", "value": value}
            else:
                return {"status": "error", "message": "Không tìm thấy key"}
        
        # Chuyển tiếp đến node chính
        node_chinh = cac_node_chiu_trach_nhiem[0]
        if node_chinh in self.cac_node_khac:
            self.logger.debug(f"→ Chuyển tiếp GET {key} đến {node_chinh}")
            with self.khoa_thong_ke:
                self.thong_ke['so_lan_chuyen_tiep'] += 1
            return self._chuyen_tiep_request(node_chinh, {"command": "GET", "key": key})
        
        return {"status": "error", "message": "Node chịu trách nhiệm không khả dụng"}
    
    def _xu_ly_delete(self, key: str) -> dict:
        """
        Xử lý thao tác DELETE
        
        Quy trình:
        1. Tìm các node chịu trách nhiệm
        2. Nếu không chịu trách nhiệm -> chuyển tiếp
        3. Xóa tại local và lan truyền đến các replicas
        """
        cac_node_chiu_trach_nhiem = self.lay_cac_node_chiu_trach_nhiem(key)
        
        # Chuyển tiếp nếu không chịu trách nhiệm
        if self.node_id not in cac_node_chiu_trach_nhiem:
            node_chinh = cac_node_chiu_trach_nhiem[0]
            if node_chinh in self.cac_node_khac:
                self.logger.info(f"→ Chuyển tiếp DELETE {key} đến {node_chinh}")
                with self.khoa_thong_ke:
                    self.thong_ke['so_lan_chuyen_tiep'] += 1
                return self._chuyen_tiep_request(node_chinh, {"command": "DELETE", "key": key})
            return {"status": "error", "message": "Node chịu trách nhiệm không khả dụng"}
        
        # Xóa tại local
        with self.khoa_du_lieu:
            da_xoa = self.du_lieu.pop(key, None) is not None
        
        with self.khoa_thong_ke:
            self.thong_ke['so_lan_delete'] += 1
        
        # Lan truyền xóa đến replicas
        for node_id in cac_node_chiu_trach_nhiem:
            if node_id != self.node_id and node_id in self.cac_node_khac:
                threading.Thread(
                    target=self._xoa_tu_node,
                    args=(node_id, key),
                    daemon=True,
                    name=f"Xoa-{node_id}"
                ).start()
        
        self.logger.info(f"✓ DELETE {key}")
        return {
            "status": "success" if da_xoa else "error",
            "message": "Đã xóa key" if da_xoa else "Không tìm thấy key"
        }
    
    def _xu_ly_nhan_ban(self, key: str, value: Optional[str]) -> dict:
        """
        Xử lý request nhân bản từ node khác
        
        FIX QUAN TRỌNG: Đây là nơi node nhận dữ liệu được nhân bản
        
        Tham số:
            key: Key cần nhân bản
            value: Value cần lưu (None = xóa)
        """
        # with self.khoa_du_lieu:
        #     if value is None:
        #         # Xóa
        #         self.du_lieu.pop(key, None)
        #         self.logger.debug(f"✓ Đã nhân bản DELETE {key}")
        #     else:
        #         # Lưu
        #         self.du_lieu[key] = value
        #         self.logger.debug(f"✓ Đã nhân bản PUT {key}={value}")
        
        # with self.khoa_thong_ke:
        #     self.thong_ke['so_lan_nhan_ban'] += 1
        
        # return {"status": "success"}
        
    # Bỏ qua việc kiểm tra lay_cac_node_chiu_trach_nhiem tại đây để tránh sai số vòng băm
        
        with self.khoa_du_lieu:
            if value is None:
                self.du_lieu.pop(key, None)
            else:
                self.du_lieu[key] = value
        
        with self.khoa_thong_ke:
            # Tăng thống kê để dễ theo dõi trong log
            self.thong_ke['so_lan_nhan_ban'] += 1
        return {"status": "success"}
    
    # ==================== QUẢN LÝ CLUSTER ====================
    
    # def _xu_ly_join(self, node_id: str, host: str, port: int) -> dict:
    #     """
    #     Xử lý request JOIN từ node mới
        
    #     FIX: Bây giờ sẽ trả về danh sách peers đầy đủ
        
    #     Quy trình:
    #     1. Thêm node mới vào danh sách peers
    #     2. Cập nhật heartbeat
    #     3. Thông báo cho các peers khác
    #     4. Trả về danh sách tất cả peers
    #     """
    #     with self.khoa_node_khac:
    #         self.cac_node_khac[node_id] = (host, port)
        
    #     with self.khoa_heartbeat:
    #         self.heartbeat_cuoi[node_id] = time.time()
        
    #     self.logger.info(f"✓ Node {node_id} đã tham gia cluster")
        
    #     # Thông báo cho các peers khác về node mới
    #     self._phat_thong_tin_node_moi(node_id, host, port)
        
    #     with self.khoa_node_khac:
    #         return {"status": "success", "peers": dict(self.cac_node_khac)}
    def _xu_ly_join(self, node_id: str, host: str, port: int) -> dict:
        if node_id == self.node_id:
            return {"status": "success", "peers": dict(self.cac_node_khac)}

        with self.khoa_node_khac:
            # Nếu node mới chưa có trong danh sách
            if node_id not in self.cac_node_khac:
                self.cac_node_khac[node_id] = (host, port)

        # Thông báo cho tất cả peers về node mới
        self._phat_thong_tin_node_moi(node_id, host, port)

        # Trả về danh sách peers đầy đủ (bao gồm cả node mới)
        with self.khoa_node_khac:
            peers = dict(self.cac_node_khac)
        peers[self.node_id] = (self.host, self.port)  # thêm chính node
        return {"status": "success", "peers": peers}



    def _xu_ly_heartbeat(self, node_id: str) -> dict:
        """
        Xử lý heartbeat từ node khác
        
        Giải thích: Cập nhật thời gian heartbeat cuối cùng
        Dùng để phát hiện node bị lỗi
        """
        with self.khoa_heartbeat:
            self.heartbeat_cuoi[node_id] = time.time()
        
        self.logger.debug(f"♥ Nhận heartbeat từ {node_id}")
        return {"status": "success"}
    
    def _xu_ly_lay_tat_ca_du_lieu(self) -> dict:
        """
        Trả về tất cả dữ liệu được lưu trong node này
        
        Dùng cho: Đồng bộ dữ liệu khi node mới join
        """
        with self.khoa_du_lieu:
            return {"status": "success", "data": dict(self.du_lieu)}
    
    # def _xu_ly_dong_bo_du_lieu(self, data: dict) -> dict:
    #     """
    #     Đồng bộ dữ liệu từ node khác
        
    #     FIX: Chỉ cập nhật dữ liệu mà node này chịu trách nhiệm
    #     """
    #     so_key_dong_bo = 0
    #     with self.khoa_du_lieu:
    #         for key, value in data.items():
    #             cac_node_chiu_trach_nhiem = self.lay_cac_node_chiu_trach_nhiem(key)
    #             if self.node_id in cac_node_chiu_trach_nhiem:
    #                 self.du_lieu[key] = value
    #                 so_key_dong_bo += 1
        
    #     self.logger.info(f"✓ Đã đồng bộ {so_key_dong_bo} keys từ peer")
    #     return {"status": "success"}
    def _xu_ly_dong_bo_du_lieu(self, data: dict) -> dict:
        so_key_dong_bo = 0
        with self.khoa_du_lieu:
            for key, value in data.items():
                cac_node_chiu_trach_nhiem = self.lay_cac_node_chiu_trach_nhiem(key)
                if self.node_id in cac_node_chiu_trach_nhiem:
                    self.du_lieu[key] = value
                    so_key_dong_bo += 1
        self.logger.info(f"🔄 Đồng bộ {so_key_dong_bo} keys từ peer")
        return {"status": "success"}

    
    def _xu_ly_lay_thong_ke(self) -> dict:
        """
        Trả về thống kê của node
        """
        with self.khoa_thong_ke:
            thoi_gian_hoat_dong = time.time() - self.thong_ke['thoi_gian_bat_dau']
            return {
                "status": "success",
                "stats": {
                    **self.thong_ke,
                    "thoi_gian_hoat_dong": thoi_gian_hoat_dong,
                    "so_key": len(self.du_lieu),
                    "so_peer": len(self.cac_node_khac)
                }
            }
    
    # ==================== GIAO TIẾP MẠNG ====================
    
    # def _chuyen_tiep_request(self, node_id: str, request: dict) -> dict:
    #     """
    #     Chuyển tiếp request đến node khác
        
    #     Giải thích: Khi node này không chịu trách nhiệm cho một key,
    #     nó sẽ chuyển tiếp request đến node chịu trách nhiệm
    #     """
    #     if node_id not in self.cac_node_khac:
    #         return {"status": "error", "message": "Không tìm thấy node"}
        
    #     host, port = self.cac_node_khac[node_id]
        
    #     try:
    #         sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #         sock.settimeout(5.0)
    #         sock.connect((host, port))
            
    #         # Gửi request
    #         sock.sendall(json.dumps(request).encode() + b"\n")
            
    #         # Nhận response
    #         data = b""
    #         while True:
    #             chunk = sock.recv(4096)
    #             if not chunk:
    #                 break
    #             data += chunk
    #             if b"\n" in data:
    #                 break
            
    #         sock.close()
    #         return json.loads(data.decode())
            
    #     except socket.timeout:
    #         self.logger.error(f"✗ Timeout khi chuyển tiếp đến {node_id}")
    #         return {"status": "error", "message": "Request timeout"}
    #     except Exception as e:
    #         self.logger.error(f"✗ Lỗi chuyển tiếp đến {node_id}: {e}")
    #         return {"status": "error", "message": str(e)}
    def _chuyen_tiep_request(self, node_id: str, request: dict) -> dict:
        if node_id == self.node_id:
            return {"status": "error", "message": "Không forward về chính mình"}

        if node_id not in self.cac_node_khac:
            return {"status": "error", "message": "Không tìm thấy node"}

        host, port = self.cac_node_khac[node_id]

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(5.0)
                sock.connect((host, port))
                sock.sendall(json.dumps(request).encode() + b"\n")

                data = b""
                while True:
                    chunk = sock.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                    if b"\n" in data:
                        break

            return json.loads(data.decode().strip())

        except socket.timeout:
            self.logger.error(f"✗ Timeout khi chuyển tiếp đến {node_id}")
            return {"status": "error", "message": "Request timeout"}
        except Exception as e:
            self.logger.error(f"✗ Lỗi chuyển tiếp đến {node_id}: {e}")
            return {"status": "error", "message": str(e)}

    
    # def _nhan_ban_den_node(self, node_id: str, key: str, value: str):
    #     """
    #     Nhân bản một cặp key-value đến node khác
        
    #     FIX QUAN TRỌNG: Đây là hàm gửi dữ liệu đến node khác để nhân bản
    #     """
    #     response = self._chuyen_tiep_request(node_id, {
    #         "command": "REPLICATE",
    #         "key": key,
    #         "value": value
    #     })
        
    #     if response.get("status") != "success":
    #         self.logger.warning(f"⚠ Lỗi nhân bản {key} đến {node_id}")
    #     else:
    #         self.logger.debug(f"✓ Đã nhân bản {key} đến {node_id}")
    def _nhan_ban_den_node(self, node_id: str, key: str, value: str):
        max_retries = 3
        for attempt in range(max_retries):
            response = self._chuyen_tiep_request(node_id, {
                "command": "REPLICATE",
                "key": key,
                "value": value
            })
            
            if response.get("status") == "success":
                self.logger.debug(f"✓ Nhân bản thành công {key} đến {node_id}")
                return
            
            time.sleep(0.5 * (attempt + 1)) # Backoff cơ bản
        
        self.logger.error(f"✗ Thất bại vĩnh viễn khi nhân bản {key} đến {node_id}")
    
    def _xoa_tu_node(self, node_id: str, key: str):
        """
        Xóa một key từ node khác
        """
        self._chuyen_tiep_request(node_id, {
            "command": "REPLICATE",
            "key": key,
            "value": None  # None = xóa
        })
    
    # def _phat_thong_tin_node_moi(self, node_id: str, host: str, port: int):
    #     """
    #     Thông báo cho tất cả peers về node mới
        
    #     FIX: Đảm bảo tất cả nodes đều biết về nhau
    #     """
    #     with self.khoa_node_khac:
    #         peers = list(self.cac_node_khac.keys())
        
    #     for peer_id in peers:
    #         if peer_id != node_id:
    #             try:
    #                 self._chuyen_tiep_request(peer_id, {
    #                     "command": "JOIN",
    #                     "node_id": node_id,
    #                     "host": host,
    #                     "port": port
    #                 })
    #                 self.logger.debug(f"→ Đã thông báo {peer_id} về node mới {node_id}")
    #             except:
    #                 pass
    def _phat_thong_tin_node_moi(self, node_id: str, host: str, port: int):
        with self.khoa_node_khac:
            peers = list(self.cac_node_khac.keys())
        
        for peer_id in peers:
            if peer_id != node_id:
                try:
                    self._chuyen_tiep_request(peer_id, {
                        "command": "JOIN",
                        "node_id": node_id,
                        "host": host,
                        "port": port
                    })
                except Exception as e:
                    self.logger.debug(f"⚠ Lỗi thông báo node mới {node_id} đến {peer_id}: {e}")


    # ==================== CÁC BACKGROUND THREADS ====================
    
    def _thread_gui_heartbeat(self):
        """
        Background thread: Gửi heartbeat định kỳ đến tất cả peers
        
        Giải thích: Mỗi 3 giây, gửi heartbeat đến tất cả nodes
        để cho biết node này vẫn còn sống
        """
        self.logger.info("✓ Thread gửi heartbeat đã khởi động")
        
        while self.dang_chay:
            time.sleep(self.khoang_thoi_gian_heartbeat)
            
            with self.khoa_node_khac:
                peers = list(self.cac_node_khac.keys())
            
            for node_id in peers:
                try:
                    self._chuyen_tiep_request(node_id, {
                        "command": "HEARTBEAT",
                        "node_id": self.node_id
                    })
                except Exception as e:
                    self.logger.debug(f"⚠ Heartbeat đến {node_id} thất bại: {e}")
    
    def _thread_phat_hien_loi(self):
        """
        Background thread: Phát hiện các node bị lỗi
        
        Giải thích: Mỗi 5 giây, kiểm tra xem node nào quá 10 giây
        không gửi heartbeat thì coi như bị lỗi
        """
        self.logger.info("✓ Thread phát hiện lỗi đã khởi động")
        
        while self.dang_chay:
            time.sleep(5)
            thoi_gian_hien_tai = time.time()
            
            with self.khoa_heartbeat:
                cac_node_loi = [
                    node_id for node_id, thoi_gian_cuoi in self.heartbeat_cuoi.items()
                    if thoi_gian_hien_tai - thoi_gian_cuoi > self.thoi_gian_timeout_heartbeat
                ]
            
            for node_id in cac_node_loi:
                self.logger.warning(f"✗ Phát hiện node {node_id} bị lỗi")
                
                with self.khoa_node_khac:
                    self.cac_node_khac.pop(node_id, None)
                
                with self.khoa_heartbeat:
                    self.heartbeat_cuoi.pop(node_id, None)
    
    def _thread_bao_cao_thong_ke(self):
        """
        Background thread: Báo cáo thống kê định kỳ
        
        Giải thích: Mỗi 60 giây, in ra thống kê
        """
        while self.dang_chay:
            time.sleep(60)  # Báo cáo mỗi phút
            
            with self.khoa_thong_ke:
                thoi_gian_hoat_dong = time.time() - self.thong_ke['thoi_gian_bat_dau']
                self.logger.info(
                    f"📊 Thống kê - Thời gian hoạt động: {thoi_gian_hoat_dong:.0f}s, "
                    f"PUT: {self.thong_ke['so_lan_put']}, "
                    f"GET: {self.thong_ke['so_lan_get']}, "
                    f"DEL: {self.thong_ke['so_lan_delete']}, "
                    f"Nhân bản: {self.thong_ke['so_lan_nhan_ban']}, "
                    f"Dữ liệu: {len(self.du_lieu)} keys, "
                    f"Peers: {len(self.cac_node_khac)}"
                )
    
    def _thread_dong_bo_dinh_ky(self):
        """
        FIX QUAN TRỌNG: Background thread đồng bộ dữ liệu định kỳ
        
        Giải thích: Mỗi 30 giây, kiểm tra và đồng bộ dữ liệu
        với các peers để đảm bảo tính nhất quán
        """
        self.logger.info("✓ Thread đồng bộ định kỳ đã khởi động")
        
        # Đợi 10 giây trước khi bắt đầu đồng bộ lần đầu
        time.sleep(10)
        
        while self.dang_chay:
            try:
                with self.khoa_node_khac:
                    peers = list(self.cac_node_khac.keys())
                
                if not peers:
                    time.sleep(30)
                    continue
                
                # Lấy dữ liệu từ một peer ngẫu nhiên
                for peer_id in peers:
                    try:
                        response = self._chuyen_tiep_request(peer_id, {"command": "GET_ALL_DATA"})
                        
                        if response.get("status") == "success":
                            peer_data = response.get("data", {})
                            
                            # Chỉ đồng bộ keys mà node này chịu trách nhiệm
                            so_key_dong_bo = 0
                            with self.khoa_du_lieu:
                                for key, value in peer_data.items():
                                    cac_node_chiu_trach_nhiem = self.lay_cac_node_chiu_trach_nhiem(key)
                                    if self.node_id in cac_node_chiu_trach_nhiem:
                                        if key not in self.du_lieu:
                                            self.du_lieu[key] = value
                                            so_key_dong_bo += 1
                            
                            if so_key_dong_bo > 0:
                                self.logger.info(f"🔄 Đã đồng bộ {so_key_dong_bo} keys mới từ {peer_id}")
                            
                            break  # Chỉ cần đồng bộ từ 1 peer
                            
                    except Exception as e:
                        self.logger.debug(f"⚠ Lỗi đồng bộ từ {peer_id}: {e}")
                        continue
                
            except Exception as e:
                self.logger.error(f"✗ Lỗi trong thread đồng bộ: {e}")
            
            time.sleep(30)  # Đồng bộ mỗi 30 giây
    
    # ==================== KHÔI PHỤC ====================
    
    def tham_gia_cluster(self, seed_host: str, seed_port: int) -> bool:
        """
        Tham gia vào một cluster đang tồn tại
        
        FIX: Quy trình đã được cải thiện
        
        Quy trình:
        1. Kết nối đến seed node
        2. Gửi request JOIN
        3. Nhận danh sách peers
        4. Đồng bộ dữ liệu từ peers
        """
        try:
            self.logger.info(f"→ Đang thử tham gia cluster qua {seed_host}:{seed_port}")
            
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10.0)
            sock.connect((seed_host, seed_port))
            
            # Gửi request JOIN
            request = {
                "command": "JOIN",
                "node_id": self.node_id,
                "host": self.host,
                "port": self.port
            }
            sock.sendall(json.dumps(request).encode() + b"\n")
            
            # Nhận response
            data = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                data += chunk
                if b"\n" in data:
                    break
            
            sock.close()
            
            response = json.loads(data.decode())
            
            if response.get("status") == "success":
                # Cập nhật danh sách peers
                with self.khoa_node_khac:
                    peers_moi = response.get("peers", {})
                    self.cac_node_khac.update(peers_moi)
                    
                    # Thêm seed node vào peers nếu chưa có
                    seed_id = f"{seed_host}:{seed_port}"
                    if seed_id not in self.cac_node_khac:
                        self.cac_node_khac[seed_id] = (seed_host, seed_port)
                
                self.logger.info(f"✓ Đã tham gia cluster thành công. Peers: {len(self.cac_node_khac)}")
                
                # Khôi phục dữ liệu
                self._phuc_hoi_du_lieu()
                
                return True
            else:
                self.logger.error(f"✗ JOIN bị từ chối: {response.get('message')}")
                return False
                
        except Exception as e:
            self.logger.error(f"✗ Lỗi tham gia cluster: {e}")
            return False
    
    def _phuc_hoi_du_lieu(self):
        """
        Khôi phục dữ liệu từ peers sau khi join hoặc restart
        
        FIX: Quy trình được cải thiện
        
        Quy trình:
        1. Lấy tất cả dữ liệu từ một peer
        2. Chỉ lưu các keys mà node này chịu trách nhiệm
        3. Đảm bảo dữ liệu nhất quán
        """
        self.dang_phuc_hoi = True
        self.logger.info("🔄 Bắt đầu khôi phục dữ liệu...")
        
        with self.khoa_node_khac:
            peers = list(self.cac_node_khac.keys())
        
        if not peers:
            self.logger.warning("⚠ Không có peers để khôi phục dữ liệu")
            self.dang_phuc_hoi = False
            return
        
        for peer_id in peers:
            try:
                # Yêu cầu tất cả dữ liệu từ peer
                response = self._chuyen_tiep_request(peer_id, {"command": "GET_ALL_DATA"})
                
                if response.get("status") == "success":
                    peer_data = response.get("data", {})
                    so_key_phuc_hoi = 0
                    
                    # Chỉ lưu các keys mà node này chịu trách nhiệm
                    with self.khoa_du_lieu:
                        for key, value in peer_data.items():
                            cac_node_chiu_trach_nhiem = self.lay_cac_node_chiu_trach_nhiem(key)
                            if self.node_id in cac_node_chiu_trach_nhiem:
                                self.du_lieu[key] = value
                                so_key_phuc_hoi += 1
                    
                    self.logger.info(f"✓ Đã khôi phục {so_key_phuc_hoi} keys từ {peer_id}")
                    break
                    
            except Exception as e:
                self.logger.error(f"✗ Khôi phục từ {peer_id} thất bại: {e}")
                continue
        
        self.dang_phuc_hoi = False
        self.logger.info("✓ Hoàn tất khôi phục dữ liệu")
    
    def dung_lai(self):
        """
        Dừng node một cách graceful
        """
        self.logger.info("→ Đang dừng node...")
        self.dang_chay = False
        
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        
        self.logger.info("✓ Node đã dừng")


# ==================== ĐIỂM VÀO CHƯƠNG TRÌNH ====================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("=" * 70)
        print("HỆ THỐNG LƯU TRỮ PHÂN TÁN KEY-VALUE")
        print("=" * 70)
        print("\nCách sử dụng:")
        print("  python node.py <port> [seed_host seed_port]")
        print("\nVí dụ:")
        print("  python node.py 5001                    # Khởi động node đầu tiên")
        print("  python node.py 5002 127.0.0.1 5001     # Tham gia cluster hiện có")
        print("  python node.py 5003 127.0.0.1 5001     # Tham gia cluster hiện có")
        print("\nGhi chú:")
        print("  - Node đầu tiên sẽ tạo cluster mới")
        print("  - Các node sau sẽ tham gia cluster thông qua seed node")
        print("  - Hệ số nhân bản mặc định = 2 (mỗi key có 2 bản sao)")
        print("=" * 70)
        sys.exit(1)
    
    host = "127.0.0.1"
    port = int(sys.argv[1])
    node_id = f"{host}:{port}"
    
    # Tạo node với hệ số nhân bản = 2
    node = Node(node_id, host, port, he_so_nhan_ban=2)
    
    # Tham gia cluster nếu có seed node
    if len(sys.argv) == 4:
        seed_host = sys.argv[2]
        seed_port = int(sys.argv[3])
        
        print(f"\n✓ Đang khởi động node {node_id}...")
        print(f"→ Sẽ tham gia cluster qua seed node {seed_host}:{seed_port}\n")
        
        # Khởi động server trước
        server_thread = threading.Thread(target=node.bat_dau, daemon=True)
        server_thread.start()
        
        # Đợi server khởi động
        time.sleep(1)
        
        # Tham gia cluster
        if not node.tham_gia_cluster(seed_host, seed_port):
            print("✗ Lỗi: Không thể tham gia cluster")
            sys.exit(1)
        
        print("✓ Node đã sẵn sàng!\n")
        
        # Giữ chương trình chạy
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n→ Đang tắt node...")
            node.dung_lai()
    else:
        # Khởi động node đầu tiên
        print(f"\n✓ Đang khởi động node đầu tiên {node_id}...")
        print("→ Tạo cluster mới\n")
        
        try:
            node.bat_dau()
        except KeyboardInterrupt:
            print("\n→ Đang tắt node...")
            node.dung_lai()