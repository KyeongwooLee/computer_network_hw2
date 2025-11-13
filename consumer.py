import socket, time, sys
task_count = 0
# arguments: host, port, worker_name
host = sys.argv[1]
port = int(sys.argv[2])
worker_name = sys.argv[3]

# setup socket connection to server
sc = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
sc.connect((host, port))
print(f"{worker_name} connected to server at {host}:{port}")
print(f"{worker_name} started\n") 
# continuously pull and process tasks from server
try:
  while True:
    sc.sendall(b'[REQUEST]\n') 
    print("[REQUEST]")
    response = sc.recv(1024).decode().strip() .split()

    # task does not exist
    if response[0] == "NOTASK":
      print("NOTASK\n")
      time.sleep(1)
      continue
    
    # process received task
    else:
      task_id, duration = response
      time.sleep(float(duration))
      print(f'[Complete] {task_id}\n')
      task_count += 1
except KeyboardInterrupt:
  sc.sendall(b'[CLOSE]\n') 
  sc.close()
  
  print("== Task processing complete ==\n")
  print(f"Total tasks completed: {task_count}\n")
  print("Connection closed")