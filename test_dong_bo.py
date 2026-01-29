"""
Script Test Đồng Bộ Dữ Liệu
Kiểm tra xem 3 nodes có đồng bộ dữ liệu đúng không
"""

import time
import sys
sys.path.append('.')
from client import KVStoreClient

# Cấu hình 3 nodes
NODES = [
    ("127.0.0.1", 5001),
    ("127.0.0.1", 5002),
    ("127.0.0.1", 5003)
]

def hien_thi_tieu_de(tieu_de):
    """Hiển thị tiêu đề đẹp"""
    print("\n" + "=" * 70)
    print(tieu_de.center(70))
    print("=" * 70 + "\n")

def kiem_tra_ket_noi():
    """Kiểm tra kết nối đến tất cả nodes"""
    hien_thi_tieu_de("KIỂM TRA KẾT NỐI")
    
    for i, (host, port) in enumerate(NODES):
        client = KVStoreClient([NODES[i]], timeout=2.0)
        stats = client.lay_thong_ke_node(0)
        
        if stats:
            print(f"✓ Node {i+1} ({host}:{port}) - ONLINE")
            print(f"  └─ Peers: {stats.get('so_peer', 0)}")
        else:
            print(f"✗ Node {i+1} ({host}:{port}) - OFFLINE")
            return False
    
    return True

def test_put_va_kiem_tra_dong_bo():
    """
    Test chính: PUT dữ liệu vào một node và kiểm tra xem
    các nodes khác có nhận được dữ liệu nhân bản không
    """
    hien_thi_tieu_de("TEST 1: PUT VÀ KIỂM TRA ĐỒNG BỘ")
    
    # Tạo client kết nối đến Node 1
    client_node1 = KVStoreClient([NODES[0]])
    
    # Các cặp key-value để test
    du_lieu_test = {
        "ten": "Nguyen Van A",
        "tuoi": "25",
        "thanh_pho": "Ha Noi",
        "nghe_nghiep": "Ky Su",
        "so_thich": "Lap Trinh"
    }
    
    print("→ Đang PUT dữ liệu vào Node 1...")
    for key, value in du_lieu_test.items():
        client_node1.put(key, value, hien_thi=False)
        print(f"  ✓ PUT {key} = {value}")
    
    # Đợi để replication hoàn tất
    print("\n→ Đợi 3 giây để replication hoàn tất...")
    time.sleep(3)
    
    # Kiểm tra dữ liệu trên tất cả 3 nodes
    print("\n→ Kiểm tra dữ liệu trên tất cả nodes:\n")
    
    ket_qua_test = True
    for i, (host, port) in enumerate(NODES):
        print(f"Node {i+1} ({host}:{port}):")
        client = KVStoreClient([NODES[i]])
        
        node_co_du_lieu = True
        for key in du_lieu_test.keys():
            value = client.get(key, hien_thi=False)
            
            if value == du_lieu_test[key]:
                print(f"  ✓ {key} = {value}")
            elif value is None:
                print(f"  ✗ {key} = KHÔNG TÌM THẤY")
                node_co_du_lieu = False
                ket_qua_test = False
            else:
                print(f"  ⚠ {key} = {value} (mong đợi: {du_lieu_test[key]})")
                node_co_du_lieu = False
                ket_qua_test = False
        
        if node_co_du_lieu:
            print(f"  → Node {i+1}: ✓ ĐÃ ĐỒNG BỘ")
        else:
            print(f"  → Node {i+1}: ✗ CHƯA ĐỒNG BỘ")
        print()
    
    return ket_qua_test

def test_delete_va_kiem_tra_dong_bo():
    """Test DELETE và kiểm tra xem có đồng bộ không"""
    hien_thi_tieu_de("TEST 2: DELETE VÀ KIỂM TRA ĐỒNG BỘ")
    
    # Tạo client kết nối đến Node 2
    client_node2 = KVStoreClient([NODES[1]])
    
    # Xóa một key
    key_can_xoa = "so_thich"
    print(f"→ Đang DELETE key '{key_can_xoa}' từ Node 2...")
    client_node2.delete(key_can_xoa, hien_thi=False)
    print(f"  ✓ DELETE {key_can_xoa}")
    
    # Đợi để replication hoàn tất
    print("\n→ Đợi 3 giây để replication hoàn tất...")
    time.sleep(3)
    
    # Kiểm tra key đã bị xóa trên tất cả nodes
    print("\n→ Kiểm tra key đã bị xóa trên tất cả nodes:\n")
    
    ket_qua_test = True
    for i, (host, port) in enumerate(NODES):
        print(f"Node {i+1} ({host}:{port}):")
        client = KVStoreClient([NODES[i]])
        
        value = client.get(key_can_xoa, hien_thi=False)
        
        if value is None:
            print(f"  ✓ Key '{key_can_xoa}' đã bị xóa")
        else:
            print(f"  ✗ Key '{key_can_xoa}' vẫn còn (value = {value})")
            ket_qua_test = False
        print()
    
    return ket_qua_test

def test_put_tu_cac_node_khac_nhau():
    """Test PUT từ các nodes khác nhau"""
    hien_thi_tieu_de("TEST 3: PUT TỪ CÁC NODES KHÁC NHAU")
    
    # PUT từ Node 1
    print("→ PUT từ Node 1:")
    client1 = KVStoreClient([NODES[0]])
    client1.put("node1_key", "value_from_node1", hien_thi=True)
    
    # PUT từ Node 2
    print("\n→ PUT từ Node 2:")
    client2 = KVStoreClient([NODES[1]])
    client2.put("node2_key", "value_from_node2", hien_thi=True)
    
    # PUT từ Node 3
    print("\n→ PUT từ Node 3:")
    client3 = KVStoreClient([NODES[2]])
    client3.put("node3_key", "value_from_node3", hien_thi=True)
    
    # Đợi để replication hoàn tất
    print("\n→ Đợi 3 giây để replication hoàn tất...")
    time.sleep(3)
    
    # Kiểm tra tất cả keys trên tất cả nodes
    print("\n→ Kiểm tra tất cả keys trên tất cả nodes:\n")
    
    keys_test = ["node1_key", "node2_key", "node3_key"]
    values_mong_doi = {
        "node1_key": "value_from_node1",
        "node2_key": "value_from_node2",
        "node3_key": "value_from_node3"
    }
    
    ket_qua_test = True
    for i, (host, port) in enumerate(NODES):
        print(f"Node {i+1} ({host}:{port}):")
        client = KVStoreClient([NODES[i]])
        
        for key in keys_test:
            value = client.get(key, hien_thi=False)
            value_mong_doi = values_mong_doi[key]
            
            if value == value_mong_doi:
                print(f"  ✓ {key} = {value}")
            else:
                print(f"  ✗ {key} = {value} (mong đợi: {value_mong_doi})")
                ket_qua_test = False
        print()
    
    return ket_qua_test
def test_mat_ket_noi_va_phuc_hoi():
    """Test kịch bản tắt node, ghi dữ liệu, và phục hồi"""
    hien_thi_tieu_de("TEST 4: MẤT KẾT NỐI VÀ TỰ ĐỘNG PHỤC HỒI")
    
    # 1. Yêu cầu người dùng tắt Node 3
    print("⚠️  HÀNH ĐỘNG CẦN THIẾT:")
    print("   - Hãy sang Terminal đang chạy Node 3 (Port 5003)")
    print("   - Nhấn Ctrl+C để dừng Node")
    input("\n>> Sau khi đã dừng Node 3, nhấn Enter để tiếp tục test...")
    
    # 2. Ghi dữ liệu khi Node 3 đang chết
    client_he_thong = KVStoreClient(NODES)
    key_missed = "data_recovery_test"
    val_missed = "Phuc_Hoi_Thanh_Cong"
    
    print(f"\n→ Đang ghi '{key_missed}' khi Node 3 đang OFFLINE...")
    if client_he_thong.put(key_missed, val_missed, hien_thi=False):
        print(f"   ✓ Đã ghi dữ liệu vào các Node còn lại (1 & 2)")
    
    # 3. Yêu cầu người dùng bật lại Node 3
    print("\n⚠️  HÀNH ĐỘNG CẦN THIẾT:")
    print("   - Hãy bật lại Node 3: python node.py 5003 127.0.0.1 5001")
    input("\n>> Sau khi đã bật lại Node 3, nhấn Enter để kiểm tra đồng bộ...")
    
    # 4. Đợi để hệ thống nhận diện lại Node và đồng bộ (nếu có cơ chế)
    print("\n→ Đợi 5 giây để Node 3 khởi động và đồng bộ lại...")
    time.sleep(5)
    
    # 5. Kiểm tra trực tiếp trên Node 3
    print(f"→ Đang truy vấn trực tiếp Node 3 cho key '{key_missed}'...")
    client_n3 = KVStoreClient([NODES[2]]) # Chỉ kết nối duy nhất tới Node 3
    val_check = client_n3.get(key_missed, hien_thi=False)
    
    if val_check == val_missed:
        print(f"   ✅ Node 3: ĐÃ PHỤC HỒI dữ liệu (Giá trị: {val_check})")
        return True
    else:
        print(f"   ❌ Node 3: CHƯA CÓ dữ liệu (Giá trị: {val_check})")
        print("   (Lưu ý: Nếu thất bại, code Node cần thêm hàm request_sync khi Startup)")
        return False
def hien_thi_thong_ke_cuoi_cung():
    """Hiển thị thống kê cuối cùng của tất cả nodes"""
    hien_thi_tieu_de("THỐNG KÊ CUỐI CÙNG")
    
    for i, (host, port) in enumerate(NODES):
        client = KVStoreClient([NODES[i]])
        stats = client.lay_thong_ke_node(0)
        
        if stats:
            print(f"Node {i+1} ({host}:{port}):")
            print(f"  ├─ Thời gian hoạt động: {stats.get('thoi_gian_hoat_dong', 0):.1f}s")
            print(f"  ├─ Số keys: {stats.get('so_key', 0)}")
            print(f"  ├─ Số peers: {stats.get('so_peer', 0)}")
            print(f"  ├─ PUT: {stats.get('so_lan_put', 0)}")
            print(f"  ├─ GET: {stats.get('so_lan_get', 0)}")
            print(f"  ├─ DELETE: {stats.get('so_lan_delete', 0)}")
            print(f"  └─ Nhân bản: {stats.get('so_lan_nhan_ban', 0)}")
            print()

def main():
    """Chạy tất cả các tests"""
    print("\n")
    print("╔" + "=" * 68 + "╗")
    print("║" + " TEST ĐỒNG BỘ DỮ LIỆU - HỆ THỐNG PHÂN TÁN ".center(68) + "║")
    print("╚" + "=" * 68 + "╝")
    
    # Kiểm tra kết nối
    if not kiem_tra_ket_noi():
        print("\n✗ Lỗi: Không thể kết nối đến tất cả nodes!")
        print("Hãy đảm bảo rằng 3 nodes đang chạy:")
        print("  python node.py 5001")
        print("  python node.py 5002 127.0.0.1 5001")
        print("  python node.py 5003 127.0.0.1 5001")
        return
    
    # Đợi một chút để các nodes đồng bộ
    print("\n→ Đợi 5 giây để các nodes hoàn tất kết nối...")
    time.sleep(5)
    
    # Chạy các tests
    ket_qua = []
    
    ket_qua.append(("Test PUT và Đồng Bộ", test_put_va_kiem_tra_dong_bo()))
    ket_qua.append(("Test DELETE và Đồng Bộ", test_delete_va_kiem_tra_dong_bo()))
    ket_qua.append(("Test PUT từ Nhiều Nodes", test_put_tu_cac_node_khac_nhau()))
    
    ket_qua.append(("Test Mất kết nối & Phục hồi", test_mat_ket_noi_va_phuc_hoi()))
    # Hiển thị thống kê
    hien_thi_thong_ke_cuoi_cung()
    
    # Tổng kết
    hien_thi_tieu_de("KẾT QUẢ TEST")
    
    so_test_thanh_cong = sum(1 for _, ket_qua in ket_qua if ket_qua)
    tong_so_test = len(ket_qua)
    
    for ten_test, thanh_cong in ket_qua:
        trang_thai = "✓ PASS" if thanh_cong else "✗ FAIL"
        print(f"{trang_thai} - {ten_test}")
    
    print(f"\nTổng kết: {so_test_thanh_cong}/{tong_so_test} tests passed")
    
    if so_test_thanh_cong == tong_so_test:
        print("\n" + "🎉 " * 20)
        print("✓ TẤT CẢ TESTS ĐỀU PASS!")
        print("✓ HỆ THỐNG ĐỒNG BỘ DỮ LIỆU HOẠT ĐỘNG CHÍNH XÁC!")
        print("🎉 " * 20)
    else:
        print("\n⚠ CÓ MỘT SỐ TESTS THẤT BẠI!")
        print("Hãy kiểm tra logs để xem chi tiết.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n→ Test bị ngắt bởi người dùng")
    except Exception as e:
        print(f"\n✗ Lỗi: {e}")
        import traceback
        traceback.print_exc()
