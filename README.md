# Distributed Key-Value Store

Hệ thống lưu trữ dạng key-value phân tán với khả năng chịu lỗi và sao lưu dữ liệu tự động.

## 📋 Mục lục

- [Tính năng](#tính-năng)
- [Kiến trúc](#kiến-trúc)
- [Cài đặt](#cài-đặt)
- [Sử dụng](#sử-dụng)
- [Testing](#testing)
- [Tài liệu kỹ thuật](#tài-liệu-kỹ-thuật)

## ✨ Tính năng

### Chức năng cơ bản
- **PUT(key, value)**: Lưu trữ cặp key-value
- **GET(key)**: Lấy giá trị của key
- **DELETE(key)**: Xóa key

### Tính năng nâng cao
- ✅ **Phân tán dữ liệu**: Sử dụng Consistent Hashing
- ✅ **Sao lưu tự động**: Replication factor = 2
- ✅ **Chịu lỗi**: Hoạt động khi có node bị hỏng
- ✅ **Tự phát hiện lỗi**: Heartbeat mechanism
- ✅ **Tự khôi phục**: Data recovery khi node restart
- ✅ **Chuyển tiếp yêu cầu**: Request forwarding tự động

## 🏗️ Kiến trúc

### Mô hình tổng thể

```
┌─────────────────────────────────────────┐
│           Client Layer                  │
│  (PUT, GET, DELETE operations)          │
└────────────────┬────────────────────────┘
                 │
    ┌────────────┼────────────┐
    ▼            ▼            ▼
┌────────┐  ┌────────┐  ┌────────┐
│ Node 1 │  │ Node 2 │  │ Node 3 │
│ :5001  │◄─┤ :5002  │◄─┤ :5003  │
└────┬───┘  └────┬───┘  └────┬───┘
     │           │           │
     └───────────┴───────────┘
        Heartbeat & Replication
```

### Các thành phần chính

1. **Node (node.py)**
   - Quản lý dữ liệu local
   - Xử lý requests từ client
   - Tham gia cluster
   - Gửi/nhận heartbeat
   - Sao lưu dữ liệu

2. **Client (client.py)**
   - Giao diện tương tác
   - Kết nối đến bất kỳ node nào
   - Retry logic khi node fail

3. **Consistent Hashing**
   - Phân vùng dữ liệu đều
   - Minimize data movement khi thêm/xóa node

## 🚀 Cài đặt

### Yêu cầu hệ thống
- Python 3.7+
- Không cần thư viện bên ngoài (chỉ dùng standard library)

### Cấu trúc thư mục

```
distributed-kv-store/
├── node.py              # Node implementation
├── client.py            # Client interface
├── start_cluster.py     # Cluster launcher
├── test_system.py       # Test suite
└── README.md            # Documentation
```

### Cài đặt

```bash
# Clone hoặc tải project
git clone <repository-url>
cd distributed-kv-store

# Không cần cài đặt thêm gì!
```

## 📖 Sử dụng

### 1. Khởi động Cluster

**Cách 1: Sử dụng script tự động**

```bash
# Start 3 nodes trên ports 5001-5003
python start_cluster.py

# Start 5 nodes
python start_cluster.py 5

# Start 3 nodes từ port 6000
python start_cluster.py 3 6000
```

**Cách 2: Khởi động thủ công**

Terminal 1 - Node 1:
```python
from node import Node
import threading

node1 = Node("node1", "localhost", 5001)
threading.Thread(target=node1.start).start()
```

Terminal 2 - Node 2:
```python
from node import Node
import threading

node2 = Node("node2", "localhost", 5002)
threading.Thread(target=node2.start).start()
node2.join_cluster("localhost", 5001)
```

Terminal 3 - Node 3:
```python
from node import Node
import threading

node3 = Node("node3", "localhost", 5003)
threading.Thread(target=node3.start).start()
node3.join_cluster("localhost", 5001)
```

### 2. Sử dụng Client

**Interactive mode:**

```bash
python client.py
```

Sau đó nhập commands:
```
> PUT name Alice
✓ PUT name = Alice

> GET name
✓ GET name = Alice

> DELETE name
✓ DELETE name

> QUIT
```

**Programmatic usage:**

```python
from client import KVStoreClient

# Kết nối đến cluster
client = KVStoreClient([
    ("localhost", 5001),
    ("localhost", 5002),
    ("localhost", 5003)
])

# Thực hiện operations
client.put("user:1", "Alice")
value = client.get("user:1")
client.delete("user:1")
```

### 3. Testing

```bash
# Chạy full test suite
python test_system.py
```

Test suite bao gồm:
- ✅ Basic operations (PUT, GET, DELETE)
- ✅ Cluster formation
- ✅ Data replication
- ✅ Fault tolerance
- ✅ Node recovery

## 🔧 Tài liệu kỹ thuật

### Giao thức truyền thông

**Format**: JSON qua TCP socket

**Message structure:**
```json
{
  "command": "PUT|GET|DELETE|JOIN|HEARTBEAT|REPLICATE",
  "key": "string",
  "value": "string",
  "node_id": "string",
  "host": "string",
  "port": 5001
}
```

**Response structure:**
```json
{
  "status": "success|error",
  "value": "string",
  "message": "string"
}
```

### Consistent Hashing

Mỗi key và node được hash thành một số nguyên:
```python
hash(key) = MD5(key) mod 2^128
hash(node_id) = MD5(node_id) mod 2^128
```

Dữ liệu được lưu trên node có hash nhỏ nhất >= hash(key) trên ring.

### Replication Strategy

**Primary-Backup model:**
- Mỗi key có 2 replicas (replication_factor = 2)
- Primary node: node đầu tiên responsible
- Backup node: node tiếp theo trên ring

**Write flow:**
1. Client gửi PUT đến bất kỳ node nào
2. Node kiểm tra trách nhiệm
3. Nếu không responsible → forward đến primary node
4. Primary node lưu local
5. Primary node replicate đến backup node

**Read flow:**
1. Client gửi GET đến bất kỳ node nào
2. Node kiểm tra trách nhiệm
3. Nếu có data → return ngay
4. Nếu không → forward đến responsible node

### Failure Detection

**Heartbeat mechanism:**
- Mỗi node gửi heartbeat mỗi 3 giây
- Timeout: 10 giây
- Nếu không nhận heartbeat trong 10s → node bị coi là failed

**Node failure handling:**
```
Node fail → Removed from peer list → Requests routed to replicas
```

### Data Recovery

Khi node restart:
1. Join cluster lại
2. Request full data snapshot từ peer
3. Filter data theo consistent hashing
4. Restore chỉ data mà node responsible for

### Scalability

**Thêm node mới:**
```python
new_node = Node("node4", "localhost", 5004)
threading.Thread(target=new_node.start).start()
new_node.join_cluster("localhost", 5001)
```

Consistent hashing đảm bảo:
- Chỉ ~1/N data cần di chuyển
- Minimize disruption

## 📊 Performance Characteristics

| Operation | Complexity | Notes |
|-----------|------------|-------|
| PUT | O(R) | R = replication factor |
| GET | O(1) | If local, O(1) network hop if forwarded |
| DELETE | O(R) | Same as PUT |
| Node failure detection | O(1) | Heartbeat based |
| Data recovery | O(D) | D = data size for node |

## 🔍 Debugging

**Enable verbose logging:**
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

**Check node status:**
```python
print(f"Peers: {node.peers}")
print(f"Data: {node.data}")
print(f"Heartbeats: {node.last_heartbeat}")
```

## ⚠️ Hạn chế hiện tại

1. **Không có persistence**: Dữ liệu chỉ trong memory
2. **Simple consistency model**: Eventual consistency
3. **No authentication**: Không có security layer
4. **Fixed replication factor**: Không thể thay đổi động
5. **No data compaction**: Không có garbage collection

## 🚀 Cải tiến đề xuất

### Ngắn hạn
- [ ] Thêm disk persistence (write-ahead log)
- [ ] Implement quorum-based consistency
- [ ] Add authentication & authorization
- [ ] Metrics và monitoring

### Dài hạn
- [ ] Dynamic replication factor
- [ ] Automatic data rebalancing
- [ ] Support for transactions
- [ ] Compression
- [ ] Multi-datacenter support

## 📝 License

MIT License - Free to use for educational purposes

## 👥 Contributors

Distributed Systems Course Project

---

**Lưu ý**: Đây là implementation đơn giản cho mục đích học tập. Không nên dùng cho production environment.