#!/bin/bash

# VPS Memory Management Script for 24GB RAM with 5 OpenClaw Containers
# Optimizes memory allocation: 2GB reserved, 4GB max per container

set -e

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔧 VPS Memory Management (24GB RAM, 5 Containers)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Function to display current system memory
show_system_memory() {
    echo "📊 Current System Memory:"
    free -h | grep -E "^(Mem|Swap)"
    echo ""
}

# Function to check Docker stats
show_docker_stats() {
    echo "🐳 Current Docker Container Stats:"
    docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" 2>/dev/null || echo "   (Docker not running or no containers)"
    echo ""
}

# Function to list OpenClaw containers
list_openclaw_containers() {
    echo "📦 OpenClaw Containers:"
    OPENCLAW_CONTAINERS=$(docker ps -a --filter "name=openclaw-user_" --format "{{.Names}}\t{{.Status}}\t{{.CreatedAt}}" 2>/dev/null | column -t || echo "")
    
    if [ -z "$OPENCLAW_CONTAINERS" ]; then
        echo "   No OpenClaw user containers found"
    else
        echo "$OPENCLAW_CONTAINERS" | nl -w2 -s'. '
    fi
    
    CONTAINER_COUNT=$(docker ps -a --filter "name=openclaw-user_" --format "{{.Names}}" 2>/dev/null | wc -l | tr -d ' ')
    echo ""
    echo "   Total: $CONTAINER_COUNT/5 OpenClaw containers"
    echo ""
}

# Function to update memory limits for all OpenClaw containers
update_openclaw_memory() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🔄 Updating Memory Limits for OpenClaw Containers"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    
    CONTAINERS=$(docker ps -a --filter "name=openclaw-user_" --format "{{.Names}}" 2>/dev/null)
    
    if [ -z "$CONTAINERS" ]; then
        echo "❌ No OpenClaw containers found to update"
        return 1
    fi
    
    echo "Target configuration:"
    echo "   • Memory Reservation: 2GB (soft limit)"
    echo "   • Memory Limit: 4GB (hard limit)"
    echo "   • Memory Swap: 4GB (no extra swap)"
    echo ""
    
    for CONTAINER in $CONTAINERS; do
        echo "🔧 Updating: $CONTAINER"
        
        if docker update \
            --memory=4g \
            --memory-reservation=2g \
            --memory-swap=4g \
            "$CONTAINER" 2>/dev/null; then
            echo "   ✅ Success"
        else
            echo "   ❌ Failed"
        fi
    done
    
    echo ""
    echo "✅ Memory limit update complete!"
    echo ""
}

# Function to show memory allocation plan
show_memory_plan() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📋 Memory Allocation Plan (24GB VPS)"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "Container Type              | Count | Reserved | Max   | Total Max"
    echo "----------------------------|-------|----------|-------|----------"
    echo "OpenClaw user instances     | 5     | 2GB      | 4GB   | 20GB"
    echo "Traefik (reverse proxy)     | 1     | -        | ~256MB| 0.3GB"
    echo "Forward-auth (auth service) | 1     | -        | ~128MB| 0.1GB"
    echo "Cloudflare (tunnel)         | 1     | -        | ~50MB | 0.1GB"
    echo "Portainer (management)      | 1     | -        | ~100MB| 0.1GB"
    echo "System + Docker overhead    | -     | -        | -     | 2.5GB"
    echo "----------------------------|-------|----------|-------|----------"
    echo "TOTAL                       | 9     | 10GB     | -     | ~23.1GB"
    echo ""
    echo "💡 Key Points:"
    echo "   • Reserved: 10GB (5 × 2GB) - guaranteed for OpenClaw instances"
    echo "   • Burst capacity: Each OpenClaw can use up to 4GB when available"
    echo "   • Safety margin: ~0.9GB free for spikes and system needs"
    echo "   • Status: ✅ FEASIBLE - Good configuration for 5 containers"
    echo ""
}

# Function to validate current setup
validate_setup() {
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "✅ Setup Validation"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    
    # Check total system memory
    TOTAL_MEM=$(free -g | awk '/^Mem:/{print $2}')
    echo "System Memory: ${TOTAL_MEM}GB"
    
    if [ "$TOTAL_MEM" -lt 23 ]; then
        echo "   ⚠️  WARNING: System shows ${TOTAL_MEM}GB, expected ~24GB"
    else
        echo "   ✅ Memory capacity confirmed"
    fi
    echo ""
    
    # Check container count
    CONTAINER_COUNT=$(docker ps -a --filter "name=openclaw-user_" --format "{{.Names}}" 2>/dev/null | wc -l | tr -d ' ')
    echo "OpenClaw Containers: $CONTAINER_COUNT/5"
    
    if [ "$CONTAINER_COUNT" -gt 5 ]; then
        echo "   ⚠️  WARNING: More than 5 containers detected!"
        echo "   Recommended: Keep at 5 or fewer for optimal performance"
    elif [ "$CONTAINER_COUNT" -eq 5 ]; then
        echo "   ✅ At capacity (5/5)"
    else
        echo "   ✅ Can add $((5 - CONTAINER_COUNT)) more container(s)"
    fi
    echo ""
    
    # Check if memory limits are set correctly
    echo "Checking memory limits..."
    INCORRECT=0
    for CONTAINER in $(docker ps --filter "name=openclaw-user_" --format "{{.Names}}" 2>/dev/null); do
        MEM_LIMIT=$(docker inspect "$CONTAINER" --format '{{.HostConfig.Memory}}' 2>/dev/null)
        MEM_RESERVATION=$(docker inspect "$CONTAINER" --format '{{.HostConfig.MemoryReservation}}' 2>/dev/null)
        
        # Convert to GB (bytes to GB)
        MEM_LIMIT_GB=$((MEM_LIMIT / 1024 / 1024 / 1024))
        MEM_RESERVATION_GB=$((MEM_RESERVATION / 1024 / 1024 / 1024))
        
        if [ "$MEM_LIMIT_GB" -ne 4 ] || [ "$MEM_RESERVATION_GB" -ne 2 ]; then
            echo "   ⚠️  $CONTAINER: ${MEM_RESERVATION_GB}GB reserved, ${MEM_LIMIT_GB}GB max (should be 2GB/4GB)"
            INCORRECT=$((INCORRECT + 1))
        fi
    done
    
    if [ "$INCORRECT" -gt 0 ]; then
        echo "   ⚠️  $INCORRECT container(s) need memory limit adjustment"
        echo "   Run: $0 update"
    else
        echo "   ✅ All containers have correct memory limits (2GB/4GB)"
    fi
    echo ""
}

# Main menu
case "${1:-menu}" in
    status|s)
        show_system_memory
        show_docker_stats
        list_openclaw_containers
        ;;
    
    plan|p)
        show_memory_plan
        ;;
    
    update|u)
        update_openclaw_memory
        show_docker_stats
        ;;
    
    validate|v)
        validate_setup
        ;;
    
    full|f)
        show_system_memory
        list_openclaw_containers
        show_memory_plan
        validate_setup
        show_docker_stats
        ;;
    
    menu|m|*)
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  status, s      Show system memory and container stats"
        echo "  plan, p        Display memory allocation plan"
        echo "  update, u      Update memory limits for all OpenClaw containers"
        echo "  validate, v    Validate current setup and configuration"
        echo "  full, f        Run all checks (full report)"
        echo "  menu, m        Show this menu"
        echo ""
        echo "Quick start:"
        echo "  $0 full        # Complete system analysis"
        echo "  $0 update      # Fix memory limits to 2GB/4GB"
        echo ""
        ;;
esac

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
