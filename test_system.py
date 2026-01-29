"""
Test Script for Distributed Key-Value Store
Tests basic operations, replication, and fault tolerance
"""

import time
import subprocess
import sys
from client import KVStoreClient


def print_header(title):
    """Print formatted header"""
    print("\n" + "=" * 60)
    print(f" {title}")
    print("=" * 60)


def test_basic_operations(client):
    """Test basic PUT, GET, DELETE operations"""
    print_header("Test 1: Basic Operations")
    
    print("\n1. Testing PUT operations...")
    assert client.put("name", "Alice", verbose=True)
    assert client.put("age", "25", verbose=True)
    assert client.put("city", "Hanoi", verbose=True)
    
    print("\n2. Testing GET operations...")
    assert client.get("name", verbose=True) == "Alice"
    assert client.get("age", verbose=True) == "25"
    assert client.get("city", verbose=True) == "Hanoi"
    
    print("\n3. Testing UPDATE operations...")
    assert client.put("age", "26", verbose=True)
    assert client.get("age", verbose=True) == "26"
    
    print("\n4. Testing DELETE operations...")
    assert client.delete("city", verbose=True)
    assert client.get("city", verbose=False) is None
    print("✓ GET city = None (as expected)")
    
    print("\n✓ All basic operations passed!")


def test_replication(client):
    """Test data replication across nodes"""
    print_header("Test 2: Data Replication")
    
    nodes = [
        ("localhost", 5001),
        ("localhost", 5002),
        ("localhost", 5003)
    ]
    
    print("\n1. Storing data through Node 1...")
    client1 = KVStoreClient([nodes[0]])
    client1.put("replicated_key", "replicated_value", verbose=True)
    
    time.sleep(2)  # Wait for replication
    
    print("\n2. Verifying replication on all nodes...")
    for i, node in enumerate(nodes):
        client_test = KVStoreClient([node])
        value = client_test.get("replicated_key", verbose=True)
        if value == "replicated_value":
            print(f"  ✓ Node {i+1}: Data replicated successfully")
        else:
            print(f"  ⚠ Node {i+1}: Replication may have failed")
    
    print("\n✓ Replication test completed!")


def test_consistent_hashing(client):
    """Test consistent hashing distribution"""
    print_header("Test 3: Consistent Hashing")
    
    print("\n1. Storing multiple keys...")
    keys = [f"key_{i}" for i in range(20)]
    
    for key in keys:
        client.put(key, f"value_{key}", verbose=False)
    
    print(f"✓ Stored {len(keys)} keys")
    
    print("\n2. Checking distribution across nodes...")
    nodes = [
        ("localhost", 5001),
        ("localhost", 5002),
        ("localhost", 5003)
    ]
    
    for i, node in enumerate(nodes):
        client_test = KVStoreClient([node])
        stats = client_test.get_node_stats(0)
        if stats:
            print(f"  Node {i+1}: {stats.get('data_count', 0)} keys")
    
    print("\n✓ Consistent hashing test completed!")


def test_failover():
    """Test failover when a node fails"""
    print_header("Test 4: Failover (Manual Test)")
    
    print("\nThis test requires manual intervention:")
    print("1. Data is stored through the client")
    print("2. You manually stop one node (Ctrl+C on that terminal)")
    print("3. The client should still be able to read the data")
    print()
    
    client = KVStoreClient([
        ("localhost", 5001),
        ("localhost", 5002),
        ("localhost", 5003)
    ])
    
    print("Storing test data...")
    client.put("failover_test", "critical_data", verbose=True)
    
    print("\n⚠ NOW: Stop Node 2 (port 5002) in its terminal")
    input("Press Enter after stopping Node 2...")
    
    time.sleep(2)
    
    print("\nAttempting to read data after node failure...")
    value = client.get("failover_test", verbose=True)
    
    if value == "critical_data":
        print("✓ Failover successful! Data still accessible")
    else:
        print("✗ Failover failed or data lost")
    
    print("\n⚠ You can restart Node 2 now")


def test_recovery():
    """Test node recovery after restart"""
    print_header("Test 5: Node Recovery (Manual Test)")
    
    print("\nThis test verifies that a restarted node recovers data:")
    print("1. Stop Node 3 (port 5003)")
    print("2. Store some data while Node 3 is down")
    print("3. Restart Node 3")
    print("4. Verify Node 3 has recovered the data")
    print()
    
    client = KVStoreClient([
        ("localhost", 5001),
        ("localhost", 5002)
    ])
    
    print("⚠ Stop Node 3 (port 5003) now")
    input("Press Enter after stopping Node 3...")
    
    print("\nStoring data while Node 3 is down...")
    client.put("recovery_test", "recovery_data", verbose=True)
    
    print("\n⚠ Now restart Node 3 with:")
    print("  python node.py 5003 127.0.0.1 5001")
    input("Press Enter after Node 3 has restarted...")
    
    time.sleep(5)  # Wait for recovery
    
    print("\nVerifying data on recovered Node 3...")
    client3 = KVStoreClient([("localhost", 5003)])
    value = client3.get("recovery_test", verbose=True)
    
    if value == "recovery_data":
        print("✓ Recovery successful! Node 3 recovered the data")
    else:
        print("⚠ Recovery incomplete or data not on this node")


def run_all_tests():
    """Run all automated tests"""
    print("=" * 60)
    print(" Distributed Key-Value Store - Test Suite")
    print("=" * 60)
    
    nodes = [
        ("localhost", 5001),
        ("localhost", 5002),
        ("localhost", 5003)
    ]
    
    client = KVStoreClient(nodes)
    
    # Wait for cluster to be ready
    print("\nWaiting for cluster to be ready...")
    time.sleep(2)
    
    try:
        # Automated tests
        test_basic_operations(client)
        time.sleep(1)
        
        test_replication(client)
        time.sleep(1)
        
        test_consistent_hashing(client)
        time.sleep(1)
        
        # Manual tests (optional)
        print("\n\nManual tests available:")
        print("  4. Failover test (requires stopping a node)")
        print("  5. Recovery test (requires restarting a node)")
        
        choice = input("\nRun manual tests? (y/n): ").strip().lower()
        if choice == 'y':
            test_failover()
            time.sleep(1)
            test_recovery()
        
        print_header("All Tests Completed!")
        
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
    except Exception as e:
        print(f"\n✗ Error during testing: {e}")


if __name__ == "__main__":
    print("\nMake sure all 3 nodes are running before starting tests:")
    print("  Terminal 1: python node.py 5001")
    print("  Terminal 2: python node.py 5002 127.0.0.1 5001")
    print("  Terminal 3: python node.py 5003 127.0.0.1 5001")
    print()
    
    choice = input("Are all nodes running? (y/n): ").strip().lower()
    if choice == 'y':
        run_all_tests()
    else:
        print("Please start all nodes first, then run this script again.")