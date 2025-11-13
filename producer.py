import socket, time, sys

# arguments: host, port, task_file
host = sys.argv[1]
port = int(sys.argv[2])
task_file = sys.argv[3]
task_count = 0

# setup socket connection to server
sc = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
sc.connect((host, port))
print(f"connected to server at {host}:{port}\n")
timer = 0.0

# read tasks from file
tasks = []
with open(task_file, 'r') as file:
    for line in file:
        timestamp, priority, task_id, duration = line.strip().split(' ')
        tasks.append((timestamp, priority, task_id, duration))
print(f"Loaded {len(tasks)} tasks from {task_file}\n")

print("== Starting task execution ==\n")
print(f"Total taks: {len(tasks)}\n")
print(f"Time span: {tasks[0][0]}s to {tasks[-1][0]}s\n")

# send tasks to server at the correct timestamp
while task_count < len(tasks):
    timestamp, priority, task_id, duration = tasks[task_count]
    if float(timestamp) <= timer:
        sc.sendall(f'{priority} {task_id} {duration}\n'.encode())
        print(f'{priority} {task_id} {duration}')
        task_count += 1
        continue

    time.sleep(1)       
    timer += 1.0

# close the socket connection after all tasks are sent
sc.send(b'[CLOSE]\n')
sc.close()
print("== Task creation complete ==")
print(f"Tasks created: {task_count}/{len(tasks)}")
print("Connection closed")