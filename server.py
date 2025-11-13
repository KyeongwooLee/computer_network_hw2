import socket, sys, time
from threading import Thread, Lock
import logging
import heapq

# Global state
is_running = True
producer_listener = None
consumer_listener = None
heap = []  # (priority, task_id, duration) as received; protected by heap_lock
heap_lock = Lock()
counter = 0

# create listener socket
def create_listener(host, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen(5)
        # Short timeout so accept() unblocks and shutdown can proceed on Ctrl+C
        s.settimeout(1.0)
        logging.info(f"[SERVER] listening on {host}:{port}")
        return s

# setup sockets: producer and consumer
def setup_sockets():
    """
    Initialize producer and consumer listener sockets.

    Host/ports are read from sys.argv if not provided explicitly.
    """
    global producer_listener, consumer_listener
    host = sys.argv[1]
    # CLI as: python server.py <host> <producer_port> <consumer_port>
    consumer_port = int(sys.argv[2])
    producer_port = int(sys.argv[3])

    producer_listener = create_listener(host, producer_port)
    consumer_listener = create_listener(host, consumer_port)
    
def producer_worker(producer, proAddr):
    global heap, is_running
    logging.info(f"[PRODUCER] Producer connected from {proAddr}")
    buffer = []
    while True:
        # wait for request from consumer
        buffer = producer.recv(1024).decode().strip().split('\n')

        if buffer[0] == '[CLOSE]':
            producer.close()
            logging.info("[PRODUCER] disconnected")
            break
        try:
            # Ensure concurrent safety when multiple threads access the heap
            for task in buffer:
                with heap_lock:
                    task = task.split()
                    heapq.heappush(heap, task)
                logging.info(f"[CREATE] {task[0]} {task[1]} {task[2]}")
        except Exception:
            pass
        

def consumer_worker(consumer, conAddr):
    global heap, counter, is_running
    logging.info(f"[CONSUMER] consumer{counter} connected from {conAddr}")

    while True:
        # wait for request from consumer
        request = consumer.recv(1024).decode().strip()

        if request == '[REQUEST]':
            # Atomically check and pop from heap to avoid races
            with heap_lock:
                is_empty = (len(heap) == 0)
                if not is_empty:
                    _, task_id, duration = heapq.heappop(heap)
            if is_empty:
                consumer.send(b"NOTASK")
                continue
            else:
                logging.info(f"[ASSIGN] {task_id} - consumer{counter}")
                consumer.sendall(f"{task_id} {duration}\n".encode())

        elif request == '[CLOSE]':
            consumer.close()
            logging.info(f"[CONSUMER] consumer{counter} is disconnected - ({counter} workers online)")
            counter -= 1
            break

def shutdown():
    global is_running, producer_listener, consumer_listener
    is_running = False
    for s in (producer_listener, consumer_listener):
        if s is not None:
            try:
                s.close()
            except Exception:
                pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    setup_sockets()
    logging.info("[SERVER] server is running....")

    try:
        while is_running:
            # Accept consumer connections if any
            try:
                consumer, conAddr = consumer_listener.accept()
                if consumer is not None:
                    consumer_thread = Thread(target=consumer_worker, args=(consumer, conAddr), daemon=True)
                    counter += 1
                    logging.info(f"[SERVER] {counter} online")
                    consumer_thread.start()
            except socket.timeout:
                pass
            except OSError:
                # Listener likely closed during shutdown
                break

            # Accept producer connections if any
            try:
                producer, proAddr = producer_listener.accept()
                if producer is not None:
                    producer_thread = Thread(target=producer_worker, args=(producer, proAddr), daemon=True)
                    logging.info("[PRODUCER] New producer online")
                    producer_thread.start()
            except socket.timeout:
                pass
            except OSError:
                break

            # Small sleep to avoid tight loop
            time.sleep(0.05)
    except KeyboardInterrupt:
        shutdown()
        logging.info("[SERVER] shutting down...")