"""
Test Thủ Công Cho Hệ Thống KV Phân Tán
Giao diện tương tác để test từng bước với hướng dẫn chi tiết
"""

import time
import sys
from client import KVStoreClient

# Cấu hình nodes
NODES = [
    ("127.0.0.1", 5001),
    ("127.0.0.1", 5002),
    ("127.0.0.1", 5003)
]


class ManualTestSuite:
    def __init__(self):
        self.client = KVStoreClient(NODES, timeout=3.0)
        self.test_data = {}
    
    def print_menu(self):
        """Hiển thị menu test"""
        print("\n" + "=" * 70)
        print(" MENU TEST THỦ CÔNG")
        print("=" * 70)
        print("\n📋 CÁC KỊCH BẢN TEST:\n")
        print("  1. Test Thao Tác Cơ Bản (PUT/GET/DELETE)")
        print("  2. Test Nhân Bản Dữ Liệu")
        print("  3. Test Consistent Hashing")
        print("  4. Test Tính Nhất Quán")
        print("  5. Test Failover Khi Node Lỗi")
        print("  6. Test Recovery Sau Khi Node Khôi Phục")
        print("  7. Test Phân Phối Tải")
        print("  8. Test Đồng Bộ Dữ Liệu")
        print("\n🔧 CÔNG CỤ:\n")
        print("  9. Xem Trạng Thái Cluster")
        print(" 10. Xem Dữ Liệu Trên Từng Node")
        print(" 11. Xem Thống Kê Client")
        print(" 12. Xóa Tất Cả Dữ Liệu Test")
        print("\n 0. Thoát")
        print("=" * 70)
    
    def wait_user(self, message="Nhấn Enter để tiếp tục..."):
        """Đợi người dùng nhấn Enter"""
        input(f"\n⏸  {message}")
    
    def print_step(self, step_num, description):
        """In bước test"""
        print(f"\n📍 Bước {step_num}: {description}")
    
    def print_result(self, success, message):
        """In kết quả"""
        icon = "✓" if success else "✗"
        print(f"  {icon} {message}")
    
    def show_cluster_status(self):
        """Hiển thị trạng thái cluster"""
        print("\n" + "=" * 70)
        print(" TRẠNG THÁI CLUSTER")
        print("=" * 70)
        self.client.hien_thi_trang_thai_cluster()
    
    def show_node_data(self):
        """Hiển thị dữ liệu trên từng node"""
        print("\n" + "=" * 70)
        print(" DỮ LIỆU TRÊN TỪNG NODE")
        print("=" * 70)
        
        for i, (host, port) in enumerate(NODES):
            print(f"\n[Node {i+1}] {host}:{port}")
            client_node = KVStoreClient([NODES[i]], timeout=2.0)
            
            # Thử lấy một vài keys để xem
            test_keys = ["test_key", "user:1", "product:1", "consistency_test"]
            found_any = False
            
            for key in test_keys:
                value = client_node.get(key, hien_thi=False)
                if value is not None:
                    print(f"  {key} = {value}")
                    found_any = True
            
            if not found_any:
                print("  (không có dữ liệu test hoặc node offline)")
    
    def show_client_stats(self):
        """Hiển thị thống kê client"""
        print("\n" + "=" * 70)
        print(" THỐNG KÊ CLIENT")
        print("=" * 70)
        stats = self.client.lay_thong_ke_client()
        print(f"\n  Tổng số requests: {stats['so_request']}")
        print(f"  ✓ Thành công: {stats['thanh_cong']}")
        print(f"  ✗ Thất bại: {stats['that_bai']}")
        print(f"  🔄 Số lần retry: {stats['so_lan_thu_lai']}")
        if stats['so_request'] > 0:
            success_rate = (stats['thanh_cong'] / stats['so_request']) * 100
            print(f"  📊 Tỷ lệ thành công: {success_rate:.1f}%")
    
    def clear_test_data(self):
        """Xóa tất cả dữ liệu test"""
        print("\n⚠️  Đang xóa tất cả dữ liệu test...")
        
        test_keys = [
            "test_key", "replicated_key", "consistency_test",
            "failover_test", "failover_test_2"
        ]
        
        # Thêm các keys user, product, order
        for i in range(1, 11):
            test_keys.append(f"user:{i}")
            test_keys.append(f"product:{i}")
            test_keys.append(f"order:{i}")
            test_keys.append(f"load_test_{i}")
        
        deleted = 0
        for key in test_keys:
            if self.client.delete(key, hien_thi=False):
                deleted += 1
        
        print(f"✓ Đã xóa {deleted} keys")
    
    # ==================== CÁC KỊCH BẢN TEST ====================
    
    def test_1_basic_operations(self):
        """Test 1: Thao tác cơ bản"""
        print("\n" + "=" * 70)
        print(" TEST 1: THAO TÁC CƠ BẢN (PUT/GET/DELETE)")
        print("=" * 70)
        print("\n📖 MỤC TIÊU:")
        print("  - Kiểm tra khả năng lưu, đọc, và xóa dữ liệu")
        print("  - Xác nhận các thao tác cơ bản hoạt động đúng")
        
        self.wait_user()
        
        # PUT
        self.print_step(1, "PUT một cặp key-value")
        print("  → PUT test_key = Hello World")
        result = self.client.put("test_key", "Hello World")
        self.print_result(result, "PUT thành công" if result else "PUT thất bại")
        self.wait_user()
        
        # GET
        self.print_step(2, "GET giá trị vừa PUT")
        print("  → GET test_key")
        value = self.client.get("test_key")
        self.print_result(value == "Hello World", 
                         f"GET đúng giá trị: {value}" if value else "GET thất bại")
        self.wait_user()
        
        # UPDATE
        self.print_step(3, "UPDATE giá trị")
        print("  → PUT test_key = Hello Vietnam")
        result = self.client.put("test_key", "Hello Vietnam")
        value = self.client.get("test_key", hien_thi=False)
        self.print_result(value == "Hello Vietnam", 
                         f"UPDATE thành công: {value}" if value else "UPDATE thất bại")
        self.wait_user()
        
        # DELETE
        self.print_step(4, "DELETE key")
        print("  → DELETE test_key")
        result = self.client.delete("test_key")
        value = self.client.get("test_key", hien_thi=False)
        self.print_result(value is None, 
                         "DELETE thành công, key không còn tồn tại" if value is None 
                         else f"DELETE thất bại, vẫn còn giá trị: {value}")
        
        print("\n✓ Hoàn thành Test 1")
        self.wait_user("Nhấn Enter để quay lại menu...")
    
    def test_2_replication(self):
        """Test 2: Nhân bản dữ liệu"""
        print("\n" + "=" * 70)
        print(" TEST 2: NHÂN BẢN DỮ LIỆU")
        print("=" * 70)
        print("\n📖 MỤC TIÊU:")
        print("  - Kiểm tra dữ liệu được nhân bản sang nhiều nodes")
        print("  - Với replication_factor=2, mỗi key nên có 2 bản sao")
        
        self.wait_user()
        
        self.print_step(1, "PUT dữ liệu vào cluster")
        print("  → PUT replicated_key = This should be replicated")
        self.client.put("replicated_key", "This should be replicated")
        
        self.print_step(2, "Đợi dữ liệu được nhân bản")
        print("  ⏳ Đợi 3 giây...")
        time.sleep(3)
        
        self.print_step(3, "Kiểm tra dữ liệu trên từng node")
        print("\n  → Đang kiểm tra từng node...")
        
        nodes_with_data = 0
        for i in range(len(NODES)):
            client_node = KVStoreClient([NODES[i]], timeout=2.0)
            value = client_node.get("replicated_key", hien_thi=False)
            
            if value is not None:
                print(f"    ✓ Node {i+1}: Có dữ liệu (value={value})")
                nodes_with_data += 1
            else:
                print(f"    ○ Node {i+1}: Không có dữ liệu (không chịu trách nhiệm)")
        
        print(f"\n  📊 Kết quả: {nodes_with_data}/{len(NODES)} nodes có dữ liệu")
        self.print_result(nodes_with_data >= 2, 
                         "Dữ liệu đã được nhân bản đúng cách" if nodes_with_data >= 2 
                         else "Nhân bản chưa đầy đủ")
        
        print("\n✓ Hoàn thành Test 2")
        self.wait_user("Nhấn Enter để quay lại menu...")
    
    def test_3_consistent_hashing(self):
        """Test 3: Consistent hashing"""
        print("\n" + "=" * 70)
        print(" TEST 3: CONSISTENT HASHING")
        print("=" * 70)
        print("\n📖 MỤC TIÊU:")
        print("  - Kiểm tra dữ liệu được phân phối đúng theo consistent hash")
        print("  - Mỗi key sẽ được gửi đến đúng node chịu trách nhiệm")
        
        self.wait_user()
        
        self.print_step(1, "PUT nhiều keys với prefixes khác nhau")
        test_data = {
            "user:1": "Alice",
            "user:2": "Bob",
            "user:3": "Charlie",
            "product:1": "Laptop",
            "product:2": "Phone",
            "product:3": "Tablet",
            "order:1": "Order#001",
            "order:2": "Order#002",
        }
        
        for key, value in test_data.items():
            print(f"  → PUT {key} = {value}")
            self.client.put(key, value, hien_thi=False)
        
        self.print_step(2, "Đợi dữ liệu được phân phối")
        print("  ⏳ Đợi 3 giây...")
        time.sleep(3)
        
        self.print_step(3, "Kiểm tra tất cả keys đều có thể GET được")
        success_count = 0
        for key, expected_value in test_data.items():
            value = self.client.get(key, hien_thi=False)
            if value == expected_value:
                print(f"  ✓ {key} = {value}")
                success_count += 1
            else:
                print(f"  ✗ {key} = {value} (mong đợi: {expected_value})")
        
        print(f"\n  📊 Kết quả: {success_count}/{len(test_data)} keys đúng")
        self.print_result(success_count == len(test_data), 
                         "Consistent hashing hoạt động đúng" 
                         if success_count == len(test_data) 
                         else "Có lỗi trong phân phối dữ liệu")
        
        print("\n✓ Hoàn thành Test 3")
        self.wait_user("Nhấn Enter để quay lại menu...")
    
    def test_4_consistency(self):
        """Test 4: Tính nhất quán"""
        print("\n" + "=" * 70)
        print(" TEST 4: TÍNH NHẤT QUÁN DỮ LIỆU")
        print("=" * 70)
        print("\n📖 MỤC TIÊU:")
        print("  - Kiểm tra tất cả replicas có cùng giá trị")
        print("  - Kiểm tra updates được đồng bộ đúng")
        
        self.wait_user()
        
        self.print_step(1, "PUT dữ liệu ban đầu")
        print("  → PUT consistency_test = version_1")
        self.client.put("consistency_test", "version_1")
        
        print("  ⏳ Đợi 3 giây để đồng bộ...")
        time.sleep(3)
        
        self.print_step(2, "GET từ tất cả nodes")
        values = []
        for i in range(len(NODES)):
            client_node = KVStoreClient([NODES[i]], timeout=2.0)
            value = client_node.get("consistency_test", hien_thi=False)
            values.append(value)
            print(f"  Node {i+1}: {value if value else '(không có)'}")
        
        non_none = [v for v in values if v is not None]
        all_same = all(v == non_none[0] for v in non_none) if non_none else False
        self.print_result(all_same, 
                         "Tất cả replicas có cùng giá trị" if all_same 
                         else "Có sự không nhất quán!")
        
        self.wait_user()
        
        self.print_step(3, "UPDATE dữ liệu")
        print("  → PUT consistency_test = version_2")
        self.client.put("consistency_test", "version_2")
        
        print("  ⏳ Đợi 3 giây để đồng bộ...")
        time.sleep(3)
        
        self.print_step(4, "GET lại từ tất cả nodes")
        updated_values = []
        for i in range(len(NODES)):
            client_node = KVStoreClient([NODES[i]], timeout=2.0)
            value = client_node.get("consistency_test", hien_thi=False)
            updated_values.append(value)
            print(f"  Node {i+1}: {value if value else '(không có)'}")
        
        non_none_updated = [v for v in updated_values if v is not None]
        all_updated = all(v == "version_2" for v in non_none_updated) if non_none_updated else False
        self.print_result(all_updated, 
                         "Update đã được đồng bộ đúng" if all_updated 
                         else "Update chưa được đồng bộ!")
        
        print("\n✓ Hoàn thành Test 4")
        self.wait_user("Nhấn Enter để quay lại menu...")
    
    def test_5_failover(self):
        """Test 5: Failover khi node lỗi"""
        print("\n" + "=" * 70)
        print(" TEST 5: FAILOVER KHI NODE LỖI")
        print("=" * 70)
        print("\n📖 MỤC TIÊU:")
        print("  - Kiểm tra hệ thống vẫn hoạt động khi có node bị lỗi")
        print("  - Kiểm tra client tự động failover sang node khác")
        
        print("\n⚠️  YÊU CẦU:")
        print("  - Bạn cần TẮT một node thủ công khi được yêu cầu")
        print("  - Sử dụng Ctrl+C trên terminal của node đó")
        
        self.wait_user("Nhấn Enter khi đã sẵn sàng...")
        
        self.print_step(1, "Kiểm tra trạng thái cluster ban đầu")
        self.show_cluster_status()
        self.wait_user()
        
        self.print_step(2, "PUT dữ liệu trước khi tắt node")
        print("  → PUT failover_test = data_before_failure")
        self.client.put("failover_test", "data_before_failure")
        time.sleep(2)
        
        print("\n🔴 NGAY BÂY GIỜ: Hãy TẮT Node 2 (port 5002)")
        print("   → Đến terminal của Node 2 và nhấn Ctrl+C")
        self.wait_user("Nhấn Enter sau khi đã TẮT Node 2...")
        
        print("  ⏳ Đợi 12 giây để hệ thống phát hiện node lỗi...")
        time.sleep(12)
        
        self.print_step(3, "Kiểm tra trạng thái cluster sau khi tắt node")
        self.show_cluster_status()
        self.wait_user()
        
        self.print_step(4, "Thử PUT dữ liệu mới")
        print("  → PUT failover_test_2 = data_after_failure")
        result = self.client.put("failover_test_2", "data_after_failure")
        self.print_result(result, "PUT thành công dù có node lỗi" if result 
                         else "PUT thất bại")
        self.wait_user()
        
        self.print_step(5, "Thử GET dữ liệu cũ")
        print("  → GET failover_test")
        value = self.client.get("failover_test")
        self.print_result(value == "data_before_failure", 
                         "Vẫn có thể GET dữ liệu cũ" if value 
                         else "Không thể GET dữ liệu cũ")
        
        print("\n✓ Hoàn thành Test 5")
        print("ℹ️  Để test recovery, chạy Test 6")
        self.wait_user("Nhấn Enter để quay lại menu...")
    
    def test_6_recovery(self):
        """Test 6: Recovery sau khi node khôi phục"""
        print("\n" + "=" * 70)
        print(" TEST 6: RECOVERY SAU KHI NODE KHÔI PHỤC")
        print("=" * 70)
        print("\n📖 MỤC TIÊU:")
        print("  - Kiểm tra node có thể join lại cluster")
        print("  - Kiểm tra dữ liệu được đồng bộ lại")
        
        print("\n⚠️  YÊU CẦU:")
        print("  - Đảm bảo Node 2 đang TẮT (từ Test 5)")
        print("  - Bạn sẽ cần KHỞI ĐỘNG LẠI node khi được yêu cầu")
        
        self.wait_user("Nhấn Enter khi đã sẵn sàng...")
        
        self.print_step(1, "Kiểm tra trạng thái cluster hiện tại")
        self.show_cluster_status()
        self.wait_user()
        
        print("\n🟢 NGAY BÂY GIỜ: Hãy KHỞI ĐỘNG LẠI Node 2")
        print("   → Chạy lệnh: python node.py 5002 127.0.0.1 5001")
        self.wait_user("Nhấn Enter sau khi đã KHỞI ĐỘNG Node 2...")
        
        print("  ⏳ Đợi 15 giây để node join và đồng bộ dữ liệu...")
        time.sleep(15)
        
        self.print_step(2, "Kiểm tra trạng thái cluster sau khi khôi phục")
        self.show_cluster_status()
        self.wait_user()
        
        self.print_step(3, "Kiểm tra dữ liệu trên Node 2")
        client_node2 = KVStoreClient([NODES[1]], timeout=2.0)
        
        print("  → GET failover_test từ Node 2")
        value1 = client_node2.get("failover_test", hien_thi=False)
        print(f"    {value1 if value1 else '(không có)'}")
        
        print("  → GET failover_test_2 từ Node 2")
        value2 = client_node2.get("failover_test_2", hien_thi=False)
        print(f"    {value2 if value2 else '(không có)'}")
        
        stats = client_node2.lay_thong_ke_node(0)
        if stats:
            print(f"\n  📊 Node 2 có {stats.get('so_key', 0)} keys")
            self.print_result(stats.get('so_key', 0) > 0, 
                             "Node 2 đã đồng bộ dữ liệu thành công")
        else:
            self.print_result(False, "Node 2 vẫn offline hoặc chưa kết nối")
        
        print("\n✓ Hoàn thành Test 6")
        self.wait_user("Nhấn Enter để quay lại menu...")
    
    def test_7_load_distribution(self):
        """Test 7: Phân phối tải"""
        print("\n" + "=" * 70)
        print(" TEST 7: PHÂN PHỐI TẢI")
        print("=" * 70)
        print("\n📖 MỤC TIÊU:")
        print("  - Kiểm tra dữ liệu được phân phối đều trên các nodes")
        print("  - Không có node nào quá tải")
        
        self.wait_user()
        
        self.print_step(1, "PUT nhiều keys vào cluster")
        num_keys = 20
        print(f"  → Đang PUT {num_keys} keys...")
        
        for i in range(num_keys):
            key = f"load_test_{i}"
            value = f"value_{i}"
            self.client.put(key, value, hien_thi=False)
            if (i + 1) % 5 == 0:
                print(f"    Đã PUT {i + 1}/{num_keys} keys")
        
        self.print_step(2, "Đợi dữ liệu được phân phối")
        print("  ⏳ Đợi 3 giây...")
        time.sleep(3)
        
        self.print_step(3, "Kiểm tra phân phối trên từng node")
        key_counts = []
        for i in range(len(NODES)):
            client_node = KVStoreClient([NODES[i]], timeout=2.0)
            stats = client_node.lay_thong_ke_node(0)
            if stats:
                count = stats.get('so_key', 0)
                key_counts.append(count)
                print(f"  Node {i+1}: {count} keys")
        
        if key_counts:
            avg = sum(key_counts) / len(key_counts)
            print(f"\n  📊 Trung bình: {avg:.1f} keys/node")
            
            max_count = max(key_counts)
            min_count = min(key_counts)
            print(f"  📊 Phạm vi: {min_count} - {max_count} keys")
            
            # Với replication factor = 2, độ lệch là chấp nhận được
            balanced = (max_count - min_count) <= avg * 0.5
            self.print_result(balanced, 
                             "Phân phối tải cân bằng" if balanced 
                             else "Có sự mất cân bằng trong phân phối")
        
        print("\n✓ Hoàn thành Test 7")
        self.wait_user("Nhấn Enter để quay lại menu...")
    
    def test_8_sync(self):
        """Test 8: Đồng bộ dữ liệu"""
        print("\n" + "=" * 70)
        print(" TEST 8: ĐỒNG BỘ DỮ LIỆU")
        print("=" * 70)
        print("\n📖 MỤC TIÊU:")
        print("  - Kiểm tra cơ chế đồng bộ định kỳ")
        print("  - Kiểm tra dữ liệu được cập nhật liên tục")
        
        self.wait_user()
        
        self.print_step(1, "Kiểm tra số lần đồng bộ ban đầu")
        initial_sync = {}
        for i in range(len(NODES)):
            client_node = KVStoreClient([NODES[i]], timeout=2.0)
            stats = client_node.lay_thong_ke_node(0)
            if stats:
                sync_count = stats.get('so_lan_nhan_ban', 0)
                initial_sync[i] = sync_count
                print(f"  Node {i+1}: {sync_count} lần đồng bộ")
        
        self.wait_user()
        
        self.print_step(2, "PUT thêm dữ liệu mới")
        for i in range(5):
            key = f"sync_test_{i}"
            value = f"sync_value_{i}"
            print(f"  → PUT {key} = {value}")
            self.client.put(key, value, hien_thi=False)
        
        self.print_step(3, "Đợi đồng bộ tự động")
        print("  ⏳ Đợi 35 giây cho chu kỳ đồng bộ...")
        for remaining in range(35, 0, -5):
            print(f"    Còn {remaining} giây...")
            time.sleep(5)
        
        self.print_step(4, "Kiểm tra số lần đồng bộ sau")
        final_sync = {}
        for i in range(len(NODES)):
            client_node = KVStoreClient([NODES[i]], timeout=2.0)
            stats = client_node.lay_thong_ke_node(0)
            if stats:
                sync_count = stats.get('so_lan_nhan_ban', 0)
                final_sync[i] = sync_count
                increase = sync_count - initial_sync.get(i, 0)
                print(f"  Node {i+1}: {sync_count} lần đồng bộ (+{increase})")
        
        # Kiểm tra có tăng không
        increased = any(final_sync.get(i, 0) > initial_sync.get(i, 0) 
                       for i in range(len(NODES)))
        self.print_result(increased, 
                         "Đồng bộ tự động đang hoạt động" if increased 
                         else "Không phát hiện đồng bộ mới")
        
        print("\n✓ Hoàn thành Test 8")
        self.wait_user("Nhấn Enter để quay lại menu...")
    
    def run(self):
        """Chạy test suite"""
        print("""
╔══════════════════════════════════════════════════════════════════════╗
║               TEST THỦ CÔNG HỆ THỐNG KV PHÂN TÁN                    ║
╚══════════════════════════════════════════════════════════════════════╝

HƯỚNG DẪN SỬ DỤNG:
------------------
1. Đảm bảo tất cả 3 nodes đang chạy:
   Terminal 1: python node.py 5001
   Terminal 2: python node.py 5002 127.0.0.1 5001
   Terminal 3: python node.py 5003 127.0.0.1 5001

2. Chọn test muốn chạy từ menu

3. Làm theo hướng dẫn từng bước

4. Một số test yêu cầu thao tác thủ công:
   - Test 5: Tắt node
   - Test 6: Khởi động lại node

        """)
        
        while True:
            try:
                self.print_menu()
                choice = input("\nChọn test (0-12): ").strip()
                
                if choice == "0":
                    print("\nTạm biệt!")
                    break
                elif choice == "1":
                    self.test_1_basic_operations()
                elif choice == "2":
                    self.test_2_replication()
                elif choice == "3":
                    self.test_3_consistent_hashing()
                elif choice == "4":
                    self.test_4_consistency()
                elif choice == "5":
                    self.test_5_failover()
                elif choice == "6":
                    self.test_6_recovery()
                elif choice == "7":
                    self.test_7_load_distribution()
                elif choice == "8":
                    self.test_8_sync()
                elif choice == "9":
                    self.show_cluster_status()
                    self.wait_user("Nhấn Enter để quay lại menu...")
                elif choice == "10":
                    self.show_node_data()
                    self.wait_user("Nhấn Enter để quay lại menu...")
                elif choice == "11":
                    self.show_client_stats()
                    self.wait_user("Nhấn Enter để quay lại menu...")
                elif choice == "12":
                    self.clear_test_data()
                    self.wait_user("Nhấn Enter để quay lại menu...")
                else:
                    print("⚠️  Lựa chọn không hợp lệ!")
                    time.sleep(1)
            
            except KeyboardInterrupt:
                print("\n\nTạm biệt!")
                break
            except Exception as e:
                print(f"\n✗ Lỗi: {e}")
                import traceback
                traceback.print_exc()
                self.wait_user("Nhấn Enter để tiếp tục...")


if __name__ == "__main__":
    suite = ManualTestSuite()
    suite.run()