#!/bin/bash

# 设置运行时间限制：s
RUN_TIME=10

# 需要传递数据的管道
PIPE_NAME="/tmp/raft_input_pipe"

# 启动节点1
echo "Starting Node 1..."
timeout $RUN_TIME ./raftnode -id 1 -port ":9091" -cluster "127.0.0.1:9092,127.0.0.1:9093" -pipe "$PIPE_NAME" -isleader=true &

# 启动节点2
echo "Starting Node 2..."
timeout $RUN_TIME ./raftnode -id 2 -port ":9092" -cluster "127.0.0.1:9091,127.0.0.1:9093" -pipe "$PIPE_NAME" &

# 启动节点3
echo "Starting Node 3..."
timeout $RUN_TIME ./raftnode -id 3 -port ":9093" -cluster "127.0.0.1:9091,127.0.0.1:9092" -pipe "$PIPE_NAME"&

echo "All nodes started successfully!"
# 创建一个管道用于进程间通信
if [[ ! -p "$PIPE_NAME" ]]; then
    mkfifo "$PIPE_NAME"
fi

# 捕获终端输入并通过管道传递给三个节点
echo "Enter input to send to nodes:"
start_time=$(date +%s)
while true; do
    # 从终端读取用户输入
    read -r user_input

    current_time=$(date +%s)
    elapsed_time=$((current_time - start_time))

    # 如果运行时间大于限制时间，就退出
    if [ $elapsed_time -ge $RUN_TIME ]; then
        echo 'Timeout reached, normal exit now'
        break
    fi    

    # 如果输入为空，跳过
    if [[ -z "$user_input" ]]; then
        continue
    fi

    # 将用户输入发送到管道
    echo "$user_input" > "$PIPE_NAME"

    # 如果输入 "exit"，结束脚本
    if [[ "$user_input" == "exit" ]]; then
        break
    fi
done

# 删除管道
rm "$PIPE_NAME"

# 等待所有节点完成启动
wait
