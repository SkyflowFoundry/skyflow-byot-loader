#!/bin/bash

# Lightweight EC2 monitoring script
# Minimal CPU overhead - samples every 5 seconds

echo "==================================================================="
echo "EC2 MONITOR - Press Ctrl+C to stop"
echo "==================================================================="
echo ""

# Get network interface (exclude lo)
NET_IF=$(ls /sys/class/net | grep -v lo | head -1)

# Initial network stats
read RX_BYTES_PREV < /sys/class/net/$NET_IF/statistics/rx_bytes
read TX_BYTES_PREV < /sys/class/net/$NET_IF/statistics/tx_bytes

# Initial CPU stats
read -a CPU_PREV < <(awk '/^cpu / {print $2,$3,$4,$5}' /proc/stat)

while true; do
    # Wait 5 seconds
    sleep 5

    # Current time
    TIME=$(date '+%H:%M:%S')

    # --- CPU Usage ---
    read -a CPU_CURR < <(awk '/^cpu / {print $2,$3,$4,$5}' /proc/stat)

    TOTAL_PREV=$((${CPU_PREV[0]} + ${CPU_PREV[1]} + ${CPU_PREV[2]} + ${CPU_PREV[3]}))
    TOTAL_CURR=$((${CPU_CURR[0]} + ${CPU_CURR[1]} + ${CPU_CURR[2]} + ${CPU_CURR[3]}))
    IDLE_PREV=${CPU_PREV[3]}
    IDLE_CURR=${CPU_CURR[3]}

    TOTAL_DELTA=$((TOTAL_CURR - TOTAL_PREV))
    IDLE_DELTA=$((IDLE_CURR - IDLE_PREV))

    if [ $TOTAL_DELTA -gt 0 ]; then
        CPU_USAGE=$((100 * (TOTAL_DELTA - IDLE_DELTA) / TOTAL_DELTA))
    else
        CPU_USAGE=0
    fi

    CPU_PREV=("${CPU_CURR[@]}")

    # --- Memory ---
    MEM_TOTAL=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)
    MEM_AVAIL=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo)

    MEM_TOTAL_GB=$(awk "BEGIN {printf \"%.1f\", $MEM_TOTAL/1024/1024}")
    MEM_AVAIL_GB=$(awk "BEGIN {printf \"%.1f\", $MEM_AVAIL/1024/1024}")
    MEM_USED_GB=$(awk "BEGIN {printf \"%.1f\", ($MEM_TOTAL-$MEM_AVAIL)/1024/1024}")
    MEM_USAGE=$(awk "BEGIN {printf \"%.0f\", ($MEM_TOTAL-$MEM_AVAIL)*100/$MEM_TOTAL}")

    # --- Network ---
    read RX_BYTES_CURR < /sys/class/net/$NET_IF/statistics/rx_bytes
    read TX_BYTES_CURR < /sys/class/net/$NET_IF/statistics/tx_bytes

    RX_MBPS=$(awk "BEGIN {printf \"%.1f\", ($RX_BYTES_CURR - $RX_BYTES_PREV) * 8 / 5 / 1000000}")
    TX_MBPS=$(awk "BEGIN {printf \"%.1f\", ($TX_BYTES_CURR - $TX_BYTES_PREV) * 8 / 5 / 1000000}")
    TOTAL_MBPS=$(awk "BEGIN {printf \"%.1f\", $RX_MBPS + $TX_MBPS}")

    RX_BYTES_PREV=$RX_BYTES_CURR
    TX_BYTES_PREV=$TX_BYTES_CURR

    # --- Skyflow Loader Process ---
    if pgrep -f "skyflow-loader" > /dev/null; then
        LOADER_PID=$(pgrep -f "skyflow-loader" | head -1)
        LOADER_CPU=$(ps -p $LOADER_PID -o %cpu= 2>/dev/null | xargs)
        LOADER_MEM=$(ps -p $LOADER_PID -o rss= 2>/dev/null)
        LOADER_MEM_MB=$(awk "BEGIN {printf \"%.0f\", $LOADER_MEM/1024}")
        LOADER_STATUS="Running (PID: $LOADER_PID, CPU: ${LOADER_CPU}%, Mem: ${LOADER_MEM_MB}M)"
    else
        LOADER_STATUS="Not running"
    fi

    # --- Display ---
    clear
    echo "==================================================================="
    echo "EC2 MONITOR - $TIME"
    echo "==================================================================="
    echo ""
    echo "SYSTEM:"
    printf "  CPU Usage:    %3s%% " "$CPU_USAGE"
    [ "$CPU_USAGE" -lt 70 ] 2>/dev/null && echo "[OK]" || echo "[HIGH]"

    printf "  Memory:       %s/%s GB (%s%%) " "$MEM_USED_GB" "$MEM_TOTAL_GB" "$MEM_USAGE"
    [ "$MEM_USAGE" -lt 80 ] 2>/dev/null && echo "[OK]" || echo "[HIGH]"

    printf "  Network:      %.1f Mbps (RX: %.1f TX: %.1f) " $TOTAL_MBPS $RX_MBPS $TX_MBPS
    [ $(awk "BEGIN {print ($TOTAL_MBPS < 10000) ? 1 : 0}") -eq 1 ] && echo "[OK]" || echo "[MAX]"

    echo ""
    echo "LOADER:"
    echo "  Status:       $LOADER_STATUS"
    echo ""
    echo "==================================================================="
    echo "Press Ctrl+C to stop monitoring"
done
