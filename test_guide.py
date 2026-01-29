"""
HƯỚNG DẪN TEST ĐẦY ĐỦ HỆ THỐNG
Distributed Key-Value Store Testing Guide
"""

# ============================================================================
# PHẦN 1: CHUẨN BỊ
# ============================================================================

print("""
╔══════════════════════════════════════════════════════════════════════════╗
║           HƯỚNG DẪN TEST HỆ THỐNG KEY-VALUE PHÂN TÁN                    ║
╚══════════════════════════════════════════════════════════════════════════╝

BƯỚC 1: MỞ 4 TERMINAL (CỬA SỔ DÒNG LỆNH)
----------------------------------------
Terminal 1: Node 1 (port 5001) - Seed node
Terminal 2: Node 2 (port 5002) - Join cluster
Terminal 3: Node 3 (port 5003) - Join cluster  
Terminal 4: Client - Để test


BƯỚC 2: KHỞI ĐỘNG CỤM (CLUSTER)
-------------------------------
Trong mỗi terminal, chạy lần lượt:

Terminal 1:
    python node.py 5001

Terminal 2 (chờ 2 giây sau khi Terminal 1 khởi động):
    python node.py 5002 127.0.0.1 5001

Terminal 3 (chờ 2 giây sau khi Terminal 2 khởi động):
    python node.py 5003 127.0.0.1 5001

✓ Nếu thành công, bạn sẽ thấy log như:
  "Node started on 127.0.0.1:5001"
  "Joined cluster successfully. Peers: 2"

""")

input("Nhấn Enter khi đã khởi động đủ 3 nodes...")

print("""
╔══════════════════════════════════════════════════════════════════════════╗
║                    PHẦN 2: TEST CÁC CHỨC NĂNG CƠ BẢN                     ║
╚══════════════════════════════════════════════════════════════════════════╝

TEST 1: CÁC THAO TÁC CƠ BẢN (PUT, GET, DELETE)
----------------------------------------------
Trong Terminal 4, chạy:
    python cli_client.py

Sau đó thử các lệnh:

1. Lưu dữ liệu (PUT):
   > PUT name Alice
   > PUT age 25
   > PUT city Hanoi
   
   ✓ Kiểm tra: Mỗi lệnh phải hiển thị "✓ PUT ... = ..."

2. Đọc dữ liệu (GET):
   > GET name
   > GET age
   > GET city
   
   ✓ Kiểm tra: Phải trả về đúng giá trị đã lưu

3. Cập nhật dữ liệu:
   > PUT age 26
   > GET age
   
   ✓ Kiểm tra: GET age phải trả về "26" (giá trị mới)

4. Xóa dữ liệu (DELETE):
   > DELETE city
   > GET city
   
   ✓ Kiểm tra: GET city phải báo lỗi "Key not found"

KẾT QUẢ MONG ĐỢI:
✓ Tất cả thao tác PUT, GET, DELETE hoạt động bình thường
✓ Dữ liệu được cập nhật và xóa chính xác

""")

input("Nhấn Enter sau khi hoàn thành Test 1...")

print("""
TEST 2: SAO LƯU DỮ LIỆU (REPLICATION)
-------------------------------------
Mục đích: Kiểm tra dữ liệu được sao lưu trên nhiều nodes

1. Lưu dữ liệu qua Node 1:
   > NODE 1
   > PUT user:1 John
   > PUT user:2 Jane
   > PUT user:3 Bob

2. Kiểm tra trên Node 2:
   > NODE 2
   > GET user:1
   > GET user:2
   > GET user:3
   
   ✓ Kiểm tra: Có thể đọc được ít nhất 1-2 keys
   (Vì consistent hashing, không phải tất cả keys đều trên mọi nodes)

3. Kiểm tra trên Node 3:
   > NODE 3
   > GET user:1
   > GET user:2
   > GET user:3

4. Xem trạng thái cluster:
   > STATUS
   
   ✓ Kiểm tra: 
   - Cả 3 nodes đều ONLINE
   - Mỗi node có 2 peers
   - Tổng số keys trên tất cả nodes >= số keys đã PUT

KẾT QUẢ MONG ĐỢI:
✓ Dữ liệu được sao lưu trên nhiều nodes
✓ Có thể đọc dữ liệu từ bất kỳ node nào trong cluster

""")

input("Nhấn Enter sau khi hoàn thành Test 2...")

print("""
╔══════════════════════════════════════════════════════════════════════════╗
║              PHẦN 3: TEST KHẢ NĂNG CHỊU LỖI (FAULT TOLERANCE)            ║
╚══════════════════════════════════════════════════════════════════════════╝

TEST 3: XỬ LÝ NODE BỊ LỖI (NODE FAILURE)
----------------------------------------
Mục đích: Kiểm tra hệ thống vẫn hoạt động khi có node bị lỗi

BƯỚC 1: Lưu dữ liệu quan trọng
   > PUT critical_data Important_Value
   > PUT backup_test This_must_survive

BƯỚC 2: Dừng Node 2
   - Chuyển sang Terminal 2 (Node 2)
   - Nhấn Ctrl+C để dừng node
   
   ✓ Bạn sẽ thấy trong Terminal 1 và 3:
     "Node 127.0.0.1:5002 detected as failed"

BƯỚC 3: Kiểm tra dữ liệu vẫn truy cập được
   Trong Terminal 4 (client):
   > GET critical_data
   > GET backup_test
   
   ✓ Kiểm tra: Client tự động chuyển sang node khác
   ✓ Dữ liệu vẫn đọc được (nhờ replication)

BƯỚC 4: Thử ghi dữ liệu mới
   > PUT after_failure New_Data
   > GET after_failure
   
   ✓ Kiểm tra: Vẫn có thể ghi và đọc dữ liệu

BƯỚC 5: Xem trạng thái
   > STATUS
   
   ✓ Kiểm tra: 
   - Node 2 hiển thị OFFLINE
   - Node 1 và 3 vẫn ONLINE
   - Hệ thống vẫn hoạt động

KẾT QUẢ MONG ĐỢI:
✓ Hệ thống phát hiện node lỗi sau ~10 giây
✓ Các node còn lại vẫn phục vụ requests
✓ Dữ liệu không bị mất nhờ replication

""")

input("Nhấn Enter sau khi hoàn thành Test 3...")

print("""
TEST 4: KHÔI PHỤC NODE (NODE RECOVERY)
--------------------------------------
Mục đích: Kiểm tra node có thể join lại cluster và khôi phục dữ liệu

BƯỚC 1: Khởi động lại Node 2
   Trong Terminal 2, chạy lại:
   python node.py 5002 127.0.0.1 5001
   
   ✓ Bạn sẽ thấy:
     "Attempting to join cluster via 127.0.0.1:5001"
     "Joined cluster successfully. Peers: 2"
     "Starting data recovery"
     "Recovered X keys from ..."

BƯỚC 2: Chờ vài giây để recovery hoàn tất

BƯỚC 3: Kiểm tra Node 2 đã có dữ liệu
   Trong Terminal 4:
   > STATUS
   
   ✓ Kiểm tra: Node 2 lại ONLINE và có data

BƯỚC 4: Đọc dữ liệu từ Node 2
   > NODE 2
   > GET critical_data
   > GET after_failure
   
   ✓ Kiểm tra: Node 2 đã khôi phục được các keys nó chịu trách nhiệm

KẾT QUẢ MONG ĐỢI:
✓ Node 2 join lại cluster thành công
✓ Node 2 tự động khôi phục dữ liệu từ peers
✓ Cluster quay lại trạng thái đầy đủ 3 nodes

""")

input("Nhấn Enter sau khi hoàn thành Test 4...")

print("""
╔══════════════════════════════════════════════════════════════════════════╗
║                  PHẦN 4: TEST NÂNG CAO                                   ║
╚══════════════════════════════════════════════════════════════════════════╝

TEST 5: CONSISTENT HASHING & PHÂN TÁN DỮ LIỆU
----------------------------------------------
Mục đích: Kiểm tra dữ liệu được phân bổ đều trên các nodes

BƯỚC 1: Lưu nhiều dữ liệu
   > PUT product:1 Laptop
   > PUT product:2 Mouse
   > PUT product:3 Keyboard
   > PUT product:4 Monitor
   > PUT product:5 Speaker
   > PUT product:6 Webcam
   > PUT product:7 Headset
   > PUT product:8 Microphone
   > PUT product:9 Cable
   > PUT product:10 Adapter

BƯỚC 2: Kiểm tra phân bổ
   > STATUS
   
   ✓ Kiểm tra từng node:
   - Node 1: X keys
   - Node 2: Y keys  
   - Node 3: Z keys
   
   ✓ Lý tưởng: X, Y, Z tương đương nhau (phân bổ đều)
   
   ✓ Tổng keys trên tất cả nodes = 10 * replication_factor (20 với RF=2)

KẾT QUẢ MONG ĐỢI:
✓ Dữ liệu phân bổ tương đối đều giữa các nodes
✓ Mỗi key xuất hiện trên đúng 2 nodes (replication factor = 2)

""")

input("Nhấn Enter sau khi hoàn thành Test 5...")

print("""
TEST 6: FORWARDING REQUESTS
---------------------------
Mục đích: Kiểm tra node tự động forward request đến node đúng

BƯỚC 1: Lưu key đặc biệt
   > NODE 1
   > PUT special_key Special_Value

BƯỚC 2: Xem key này được lưu ở đâu
   > STATUS
   (Xem node nào có key này)

BƯỚC 3: Request từ node KHÔNG có key
   > NODE 2
   > GET special_key
   
   ✓ Kiểm tra: Vẫn đọc được (được forward tự động)

BƯỚC 4: Xóa từ node khác
   > NODE 3
   > DELETE special_key
   
   ✓ Kiểm tra: Xóa thành công (được forward)

BƯỚC 5: Verify xóa
   > NODE 1
   > GET special_key
   
   ✓ Kiểm tra: Báo "Key not found"

KẾT QUẢ MONG ĐỜI:
✓ Client có thể kết nối bất kỳ node nào
✓ Request tự động được forward đến node đúng
✓ Thao tác thành công bất kể node nào nhận request

""")

input("Nhấn Enter sau khi hoàn thành Test 6...")

print("""
TEST 7: CONCURRENT OPERATIONS
------------------------------
Mục đích: Kiểm tra xử lý nhiều requests đồng thời

BƯỚC 1: Mở thêm Terminal 5 với client thứ 2
   Terminal 5:
   python cli_client.py

BƯỚC 2: Hai client cùng ghi dữ liệu
   Terminal 4:
   > PUT concurrent:A Value_from_Client1
   > PUT concurrent:B Value_from_Client1
   
   Terminal 5:
   > PUT concurrent:C Value_from_Client2
   > PUT concurrent:D Value_from_Client2

BƯỚC 3: Đọc chéo
   Terminal 4:
   > GET concurrent:C
   > GET concurrent:D
   
   Terminal 5:
   > GET concurrent:A
   > GET concurrent:B

BƯỚC 4: Cập nhật cùng key
   Terminal 4:
   > PUT race_test Version1
   
   Terminal 5:
   > PUT race_test Version2
   
   > GET race_test
   
   ✓ Kiểm tra: Trả về một trong hai giá trị

KẾT QUẢ MONG ĐỢI:
✓ Nhiều clients có thể hoạt động đồng thời
✓ Không có deadlock hoặc crash
✓ Dữ liệu nhất quán (có thể last-write-wins)

""")

input("Nhấn Enter sau khi hoàn thành Test 7...")

print("""
╔══════════════════════════════════════════════════════════════════════════╗
║                    PHẦN 5: TEST TỰ ĐỘNG                                  ║
╚══════════════════════════════════════════════════════════════════════════╝

Chạy test suite tự động:
    python test_system.py

Test suite sẽ tự động kiểm tra:
✓ Basic operations (PUT, GET, DELETE)
✓ Data replication
✓ Consistent hashing distribution
✓ Failover (manual intervention)
✓ Recovery (manual intervention)

""")

input("Nhấn Enter để xem kết quả tổng kết...")

print("""
╔══════════════════════════════════════════════════════════════════════════╗
║                        BẢNG CHECKLIST TỔNG KẾT                           ║
╚══════════════════════════════════════════════════════════════════════════╝

KIẾN TRÚC:
[ ] 3 nodes chạy thành công trong cluster
[ ] Mỗi node có thể xử lý requests
[ ] Nodes giao tiếp được với nhau qua TCP

THAO TÁC DỮ LIỆU:
[ ] PUT: Lưu dữ liệu thành công
[ ] GET: Đọc dữ liệu chính xác
[ ] DELETE: Xóa dữ liệu đúng
[ ] UPDATE: Cập nhật giá trị

SAO LƯU:
[ ] Mỗi key có 2 bản sao (replication factor = 2)
[ ] Dữ liệu đồng bộ giữa các replicas
[ ] Có thể đọc từ replica nodes

PHÁT HIỆN LỖI:
[ ] Heartbeat được gửi định kỳ (mỗi 3s)
[ ] Node lỗi được phát hiện sau 10s
[ ] Peer list được cập nhật khi có node lỗi

XỬ LÝ LỖI:
[ ] Hệ thống vẫn hoạt động khi 1 node lỗi
[ ] Dữ liệu vẫn truy cập được qua replicas
[ ] Client tự động failover sang node khác

KHÔI PHỤC:
[ ] Node có thể join lại cluster
[ ] Dữ liệu được khôi phục tự động
[ ] Cluster quay lại trạng thái bình thường

PHÂN TÁN:
[ ] Consistent hashing phân bổ dữ liệu
[ ] Dữ liệu phân bổ tương đối đều
[ ] Request forwarding hoạt động

HIỆU NĂNG:
[ ] Xử lý nhiều clients đồng thời
[ ] Thread-safe (không có race condition)
[ ] Không crash khi load cao

╔══════════════════════════════════════════════════════════════════════════╗
║                           KẾT THÚC TESTING                               ║
╚══════════════════════════════════════════════════════════════════════════╝

Để dừng hệ thống:
- Nhấn Ctrl+C trong mỗi terminal chạy node
- Hoặc đóng các terminal windows

Xem log chi tiết trong file: node.log
""")