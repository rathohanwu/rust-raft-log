#!/bin/bash

# Test Log Rotation Script for Raft Cluster
# This script sends multiple test messages to trigger log rotation

set -e

# Configuration
CLUSTER_CONFIG="cluster_config.yaml"
CLIENT_BINARY="./target/debug/raft-client"
DEFAULT_COUNT=50
DEFAULT_NODE=""
DEFAULT_MESSAGE_PREFIX="this is rotation testing message"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -c, --count NUMBER        Number of messages to send (default: $DEFAULT_COUNT)"
    echo "  -n, --node NODE_ID        Target specific node (1, 2, or 3). If not specified, rotates through all nodes"
    echo "  -m, --message PREFIX      Message prefix (default: '$DEFAULT_MESSAGE_PREFIX')"
    echo "  -d, --delay SECONDS       Delay between messages in seconds (default: 0.1)"
    echo "  -s, --start-index NUMBER  Starting index for messages (default: 1)"
    echo "  -h, --help               Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                    # Send 50 messages rotating through all nodes"
    echo "  $0 -c 100                            # Send 100 messages"
    echo "  $0 -c 20 -n 2                       # Send 20 messages to node 2 only"
    echo "  $0 -c 30 -m \"custom load test message\" -d 0.05     # Send 30 messages with custom prefix and faster rate"
    echo "  $0 -c 10 -s 100                     # Send 10 messages starting from index 100"
}

# Parse command line arguments
COUNT=$DEFAULT_COUNT
NODE=$DEFAULT_NODE
MESSAGE_PREFIX=$DEFAULT_MESSAGE_PREFIX
DELAY=0.1
START_INDEX=1

while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--count)
            COUNT="$2"
            shift 2
            ;;
        -n|--node)
            NODE="$2"
            shift 2
            ;;
        -m|--message)
            MESSAGE_PREFIX="$2"
            shift 2
            ;;
        -d|--delay)
            DELAY="$2"
            shift 2
            ;;
        -s|--start-index)
            START_INDEX="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate inputs
if ! [[ "$COUNT" =~ ^[0-9]+$ ]] || [ "$COUNT" -le 0 ]; then
    print_error "Count must be a positive integer"
    exit 1
fi

if [ -n "$NODE" ] && ! [[ "$NODE" =~ ^[1-3]$ ]]; then
    print_error "Node must be 1, 2, or 3"
    exit 1
fi

if ! [[ "$START_INDEX" =~ ^[0-9]+$ ]] || [ "$START_INDEX" -le 0 ]; then
    print_error "Start index must be a positive integer"
    exit 1
fi

# Check if client binary exists
if [ ! -f "$CLIENT_BINARY" ]; then
    print_error "Client binary not found: $CLIENT_BINARY"
    print_info "Please build the project first: cargo build --release"
    exit 1
fi

# Check if cluster config exists
if [ ! -f "$CLUSTER_CONFIG" ]; then
    print_error "Cluster configuration not found: $CLUSTER_CONFIG"
    exit 1
fi

# Print configuration
print_info "🚀 Starting log rotation test..."
print_info "📊 Configuration:"
echo "   • Messages to send: $COUNT"
echo "   • Starting index: $START_INDEX"
echo "   • Message prefix: '$MESSAGE_PREFIX'"
echo "   • Delay between messages: ${DELAY}s"
if [ -n "$NODE" ]; then
    echo "   • Target node: $NODE"
else
    echo "   • Target nodes: Rotating through 1, 2, 3"
fi
echo ""

# Initialize counters
success_count=0
error_count=0
start_time=$(date +%s)

# Function to send a message
send_message() {
    local index=$1
    local target_node=$2
    local message="$MESSAGE_PREFIX $index"
    
    local cmd="$CLIENT_BINARY -c $CLUSTER_CONFIG -p \"$message\""
    if [ -n "$target_node" ]; then
        cmd="$cmd -n $target_node"
    fi
    
    if eval "$cmd" >/dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# Main loop
print_info "📤 Sending messages..."
echo ""

for ((i=0; i<COUNT; i++)); do
    current_index=$((START_INDEX + i))
    
    # Determine target node
    if [ -n "$NODE" ]; then
        target_node=$NODE
    else
        # Rotate through nodes 1, 2, 3
        target_node=$(((i % 3) + 1))
    fi
    
    # Send message
    if send_message "$current_index" "$target_node"; then
        success_count=$((success_count + 1))
        printf "\r${GREEN}✅ Sent: %d/%d (Success: %d, Errors: %d)${NC}" \
               "$((i + 1))" "$COUNT" "$success_count" "$error_count"
    else
        error_count=$((error_count + 1))
        printf "\r${RED}❌ Failed: %d/%d (Success: %d, Errors: %d)${NC}" \
               "$((i + 1))" "$COUNT" "$success_count" "$error_count"
    fi
    
    # Add delay if not the last message
    if [ $i -lt $((COUNT - 1)) ]; then
        sleep "$DELAY"
    fi
done

echo ""
echo ""

# Calculate statistics
end_time=$(date +%s)
duration=$((end_time - start_time))
messages_per_second=$(echo "scale=2; $success_count / $duration" | bc -l 2>/dev/null || echo "N/A")

# Print final results
print_info "📈 Test Results:"
echo "   • Total messages sent: $COUNT"
echo "   • Successful: $success_count"
echo "   • Failed: $error_count"
echo "   • Success rate: $(echo "scale=1; $success_count * 100 / $COUNT" | bc -l 2>/dev/null || echo "N/A")%"
echo "   • Duration: ${duration}s"
echo "   • Messages per second: $messages_per_second"
echo ""

if [ $error_count -eq 0 ]; then
    print_success "🎉 All messages sent successfully!"
else
    print_warning "⚠️  Some messages failed. Check cluster status."
fi

print_info "💡 To check log rotation, examine the log files in:"
echo "   • ./raft_logs/node_1/"
echo "   • ./raft_logs/node_2/"
echo "   • ./raft_logs/node_3/"
echo ""
print_info "🔍 Look for multiple .log files indicating successful rotation!"
