#!/bin/bash

# Script to run a 3-node Raft cluster using the YAML configuration

echo "🚀 Starting 3-node Raft cluster..."

# Build the project first
echo "📦 Building the project..."
cargo build --release

if [ $? -ne 0 ]; then
    echo "❌ Build failed!"
    exit 1
fi

# Create log directories
mkdir -p raft_logs/node_1
mkdir -p raft_logs/node_2
mkdir -p raft_logs/node_3

echo "📁 Created log directories"

# Start each node in the background
echo "🌐 Starting node 1..."
./target/release/rust-raft-log --node-id 1 --config cluster_config.yaml &
NODE1_PID=$!

echo "🌐 Starting node 2..."
./target/release/rust-raft-log --node-id 2 --config cluster_config.yaml &
NODE2_PID=$!

echo "🌐 Starting node 3..."
./target/release/rust-raft-log --node-id 3 --config cluster_config.yaml &
NODE3_PID=$!

echo "✅ All nodes started!"
echo "Node 1 PID: $NODE1_PID"
echo "Node 2 PID: $NODE2_PID"
echo "Node 3 PID: $NODE3_PID"

# Function to cleanup on exit
cleanup() {
    echo "🛑 Shutting down cluster..."
    kill $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null
    wait $NODE1_PID $NODE2_PID $NODE3_PID 2>/dev/null
    echo "✅ Cluster shutdown complete"
}

# Set trap to cleanup on script exit
trap cleanup EXIT

# Wait for user input to shutdown
echo "Press Ctrl+C to shutdown the cluster..."
wait
