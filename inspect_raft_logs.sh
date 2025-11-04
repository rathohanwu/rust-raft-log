#!/bin/bash

echo "🔍 RAFT TEST DATA INSPECTION TOOL"
echo "=================================="

if [ ! -d "./raft_test_data" ]; then
    echo "❌ No raft_test_data directory found. Run the test first!"
    exit 1
fi

echo ""
echo "📁 Directory Structure:"
find ./raft_test_data -type f | sort

echo ""
echo "📊 File Sizes:"
for node in node1 node2 node3; do
    if [ -d "./raft_test_data/$node" ]; then
        echo "  $node:"
        echo "    Raft State: $(ls -lh ./raft_test_data/$node/raft_state.meta | awk '{print $5}')"
        echo "    Log Segment: $(ls -lh ./raft_test_data/$node/logs/*.dat | awk '{print $5}')"
    fi
done

echo ""
echo "🔍 Log Entry Content Analysis:"
echo "Looking for our test entries in the binary logs..."

for node in node1 node2 node3; do
    if [ -f "./raft_test_data/$node/logs/log-segment-0000000001.dat" ]; then
        echo ""
        echo "  📋 $node log entries:"
        
        # Look for our test strings in the binary file
        if strings "./raft_test_data/$node/logs/log-segment-0000000001.dat" | grep -E "Entry [1-3]:" > /dev/null; then
            strings "./raft_test_data/$node/logs/log-segment-0000000001.dat" | grep -E "Entry [1-3]:" | while read line; do
                echo "    ✅ Found: '$line'"
            done
        else
            echo "    ⚠️ No readable test entries found"
        fi
    fi
done

echo ""
echo "🔍 Raft State Analysis:"
for node in node1 node2 node3; do
    if [ -f "./raft_test_data/$node/raft_state.meta" ]; then
        echo ""
        echo "  📋 $node state file (hex dump first 40 bytes):"
        hexdump -C "./raft_test_data/$node/raft_state.meta" | head -3
    fi
done

echo ""
echo "🧪 Log Consistency Check:"
echo "Comparing log file checksums to verify replication..."

md5_node1=$(md5 -q "./raft_test_data/node1/logs/log-segment-0000000001.dat" 2>/dev/null || echo "missing")
md5_node2=$(md5 -q "./raft_test_data/node2/logs/log-segment-0000000001.dat" 2>/dev/null || echo "missing")
md5_node3=$(md5 -q "./raft_test_data/node3/logs/log-segment-0000000001.dat" 2>/dev/null || echo "missing")

echo "  Node1 log MD5: $md5_node1"
echo "  Node2 log MD5: $md5_node2"
echo "  Node3 log MD5: $md5_node3"

if [ "$md5_node1" = "$md5_node2" ] && [ "$md5_node2" = "$md5_node3" ] && [ "$md5_node1" != "missing" ]; then
    echo "  ✅ All log files are identical - perfect replication!"
else
    echo "  ⚠️ Log files differ - this may be expected due to different node states"
fi

echo ""
echo "💡 Manual Inspection Commands:"
echo "  View log entries: strings ./raft_test_data/node1/logs/*.dat"
echo "  Hex dump logs: hexdump -C ./raft_test_data/node1/logs/*.dat | head -20"
echo "  Hex dump state: hexdump -C ./raft_test_data/node1/raft_state.meta"
echo "  Compare logs: diff <(hexdump -C ./raft_test_data/node1/logs/*.dat) <(hexdump -C ./raft_test_data/node2/logs/*.dat)"
echo ""
echo "🧹 Cleanup: rm -rf ./raft_test_data"
