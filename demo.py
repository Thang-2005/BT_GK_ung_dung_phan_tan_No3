#!/usr/bin/env python3
"""
DEMO NHANH - Test các chức năng chính
Chạy sau khi đã khởi động 3 nodes
"""

import time
import sys
from client import KVStoreClient

def print_section(title):
    print("\n" + "="*70)
    print(f"  {title}")
    print("="*70)

def main():
    print("""
╔══════════════════════════════════════════════════════════════════════════╗
║                     DEMO HỆ THỐNG KEY-VALUE PHÂN TÁN                     ║
╚══════════════════════════════════════════════════════════════════════════╝

Đảm bảo đã khởi động 3 nodes:
  Terminal 1: python node.py 5001
  Terminal 2: python node.py 5002 127.0.0.1 5001
  Terminal 3: python node.py 5003 127.0.0.1 5001
    """)
    
    input("Nhấn Enter để bắt đầu demo...")
    
    # Kết nối cluster
    nodes = [
        ("localhost", 5001),
        ("localhost", 5002),
        ("localhost", 5003)
    ]
    client = KVStoreClient(nodes)
    
    # Test 1: Basic Operations
    print_section("TEST 1: THAO TÁC CƠ BẢN (PUT, GET, DELETE)")
    
    print("\n1. Lưu dữ liệu (PUT):")
    client.put("name", "Alice")
    client.put("age", "25")
    client.put("city", "Hanoi")
    client.put("job", "Engineer")
    
    time.sleep(1)
    
    print("\n2. Đọc dữ liệu (GET):")
    client.get("name")
    client.get("age")
    client.get("city")
    client.get("job")
    
    time.sleep(1)
    
    print("\n3. Cập nhật dữ liệu:")
    client.put("age", "26")
    client.get("age")
    
    time.sleep(1)
    
    print("\n4. Xóa dữ liệu (DELETE):")
    client.delete("job")
    client.get("job")
    
    input("\n✓ Test 1 hoàn thành. Nhấn Enter để tiếp tục...")
    
    # Test 2: Replication
    print_section("TEST 2: SAO LƯU DỮ LIỆU (REPLICATION)")
    
    print("\n1. Lưu dữ liệu qua các nodes khác nhau:")
    
    client1 = KVStoreClient([nodes[0]])  # Node 1
    client1.put("product:1", "Laptop")
    client1.put("product:2", "Mouse")
    
    client2 = KVStoreClient([nodes[1]])  # Node 2
    client2.put("product:3", "Keyboard")
    client2.put("product:4", "Monitor")
    
    client3 = KVStoreClient([nodes[2]])  # Node 3
    client3.put("product:5", "Speaker")
    
    time.sleep(2)  # Chờ replication
    
    print("\n2. Kiểm tra có thể đọc từ bất kỳ node nào:")
    print("\nĐọc từ Node 1:")
    client1.get("product:3")  # Được lưu bởi Node 2
    client1.get("product:5")  # Được lưu bởi Node 3
    
    print("\nĐọc từ Node 2:")
    client2.get("product:1")  # Được lưu bởi Node 1
    client2.get("product:5")  # Được lưu bởi Node 3
    
    input("\n✓ Test 2 hoàn thành. Nhấn Enter để tiếp tục...")
    
    # Test 3: Cluster Status
    print_section("TEST 3: TRẠNG THÁI CLUSTER")
    
    client.print_cluster_status()
    
    input("\n✓ Test 3 hoàn thành. Nhấn Enter để tiếp tục...")
    
    # Test 4: Consistent Hashing
    print_section("TEST 4: PHÂN TÁN DỮ LIỆU (CONSISTENT HASHING)")
    
    print("\n1. Lưu 20 keys:")
    for i in range(1, 21):
        client.put(f"key_{i}", f"value_{i}", verbose=False)
        print(f"  ✓ Đã lưu key_{i}")
    
    time.sleep(2)
    
    print("\n2. Phân bổ dữ liệu trên các nodes:")
    for i, node in enumerate(nodes):
        test_client = KVStoreClient([node])
        stats = test_client.get_node_stats(0)
        if stats:
            print(f"  Node {i+1}: {stats.get('data_count', 0)} keys")
    
    input("\n✓ Test 4 hoàn thành. Nhấn Enter để tiếp tục...")
    
    # Test 5: Statistics
    print_section("TEST 5: THỐNG KÊ HỆ THỐNG")
    
    print("\n1. Thống kê từng node:")
    for i, node in enumerate(nodes):
        print(f"\nNode {i+1} ({node[0]}:{node[1]}):")
        test_client = KVStoreClient([node])
        stats = test_client.get_node_stats(0)
        if stats:
            print(f"  Uptime: {stats.get('uptime', 0):.1f}s")
            print(f"  Data: {stats.get('data_count', 0)} keys")
            print(f"  Peers: {stats.get('peer_count', 0)}")
            print(f"  PUTs: {stats.get('puts', 0)}")
            print(f"  GETs: {stats.get('gets', 0)}")
            print(f"  Replications: {stats.get('replications', 0)}")
        else:
            print("  ✗ Node offline")
    
    print("\n2. Thống kê client:")
    client_stats = client.get_client_stats()
    print(f"  Total requests: {client_stats['requests']}")
    print(f"  Successes: {client_stats['successes']}")
    print(f"  Failures: {client_stats['failures']}")
    print(f"  Retries: {client_stats['retries']}")
    if client_stats['requests'] > 0:
        success_rate = (client_stats['successes'] / client_stats['requests']) * 100
        print(f"  Success rate: {success_rate:.1f}%")
    
    input("\n✓ Test 5 hoàn thành. Nhấn Enter để tiếp tục...")
    
    # Test 6: Manual Failover Test
    print_section("TEST 6: KIỂM TRA CHỊU LỖI (YÊU CẦU TƯƠNG TÁC)")
    
    print("""
Bài test này yêu cầu bạn thao tác thủ công:

1. Lưu dữ liệu quan trọng:
""")
    
    client.put("critical_1", "important_data_1")
    client.put("critical_2", "important_data_2")
    
    print("""
2. BÂY GIỜ:
   - Chuyển sang Terminal chạy Node 2 (port 5002)
   - Nhấn Ctrl+C để dừng node
   - Chờ 10 giây
""")
    
    input("Nhấn Enter sau khi đã dừng Node 2...")
    
    print("\n3. Kiểm tra hệ thống vẫn hoạt động:")
    print("\nĐọc dữ liệu (sẽ tự động failover sang node khác):")
    client.get("critical_1")
    client.get("critical_2")
    
    print("\nGhi dữ liệu mới:")
    client.put("after_failure", "new_data")
    client.get("after_failure")
    
    print("\n4. Trạng thái cluster:")
    client.print_cluster_status()
    
    print("""
5. KHÔI PHỤC NODE:
   - Quay lại Terminal Node 2
   - Chạy lại: python node.py 5002 127.0.0.1 5001
   - Chờ vài giây để recovery
""")
    
    input("Nhấn Enter sau khi đã khởi động lại Node 2...")
    
    time.sleep(3)
    
    print("\n6. Trạng thái cluster sau recovery:")
    client.print_cluster_status()
    
    input("\n✓ Test 6 hoàn thành. Nhấn Enter để kết thúc...")
    
    # Summary
    print_section("TỔNG KẾT DEMO")
    
    print("""
✓ HOÀN THÀNH CÁC TEST:

1. ✓ Thao tác cơ bản (PUT, GET, DELETE, UPDATE)
2. ✓ Sao lưu dữ liệu trên nhiều nodes
3. ✓ Xem trạng thái cluster
4. ✓ Phân tán dữ liệu với consistent hashing
5. ✓ Thống kê hệ thống
6. ✓ Xử lý lỗi và khôi phục (nếu đã test)

CÁC TÍNH NĂNG ĐÃ DEMO:
• Kiến trúc phân tán với 3 nodes
• Replication với replication factor = 2
• Heartbeat và failure detection
• Automatic failover
• Data recovery
• Request forwarding
• Consistent hashing
• Thread-safe operations
• Client-side retry logic

ĐỂ TEST THÊM:
- Chạy: python test_system.py (test suite đầy đủ)
- Chạy: python cli_client.py (client tương tác)
- Xem: README.md (tài liệu đầy đủ)

Cảm ơn đã sử dụng hệ thống!
    """)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nDemo bị hủy.")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Lỗi: {e}")
        print("\nĐảm bảo đã khởi động đủ 3 nodes trước khi chạy demo!")
        sys.exit(1)