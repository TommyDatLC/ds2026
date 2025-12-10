from mpi4py import MPI
import os
import sys

# MPI message tags
TAG_INFO = 1
TAG_PAYLOAD = 2
BUFFER_SIZE = 4096  # 4KB

def sender(comm, target, path):
    """Rank 0: gửi file đi."""
    if not os.path.isfile(path):
        print(f"[Sender] Error: File '{path}' not found.")
        comm.send(None, dest=target, tag=TAG_INFO)
        return
    
    filesize = os.path.getsize(path)
    print(f"[Sender] Sending '{path}' ({filesize} bytes) to Rank {target}...")

    # Gửi metadata
    info = {
        'name': os.path.basename(path),
        'size': filesize
    }
    comm.send(info, dest=target, tag=TAG_INFO)

    # Gửi từng chunk
    total_sent = 0
    with open(path, 'rb') as fp:
        while True:
            block = fp.read(BUFFER_SIZE)
            if not block:
                break
            comm.send(block, dest=target, tag=TAG_PAYLOAD)
            total_sent += len(block)

    # Gửi tín hiệu EOF
    comm.send(b'', dest=target, tag=TAG_PAYLOAD)
    print(f"[Sender] Done. Sent {total_sent} bytes.")

def receiver(comm, source):
    """Rank 1: nhận file và lưu lại."""
    print(f"[Receiver] Waiting for incoming data from Rank {source}...")

    # Nhận metadata
    info = comm.recv(source=source, tag=TAG_INFO)
    if info is None:
        print("[Receiver] Sender aborted.")
        return

    name = "recv_" + info['name']
    expected = info['size']
    print(f"[Receiver] Incoming '{name}' ({expected} bytes).")

    total_recv = 0
    with open(name, 'wb') as fp:
        while True:
            block = comm.recv(source=source, tag=TAG_PAYLOAD)
            if not block:
                break
            fp.write(block)
            total_recv += len(block)

    print(f"[Receiver] Saved '{name}'. Received {total_recv} bytes.")

if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if size < 2:
        if rank == 0:
            print("Requires at least 2 MPI ranks.")
            print("Usage: mpiexec -n 2 python mpi_transfer.py <file>")
        sys.exit(1)

    if rank == 0:
        if len(sys.argv) < 2:
            print("Usage: mpiexec -n 2 python mpi_transfer.py <file>")
            comm.send(None, dest=1, tag=TAG_INFO)
            sys.exit(1)
        sender(comm, target=1, path=sys.argv[1])

    elif rank == 1:
        receiver(comm, source=0)

    else:
        print(f"[Rank {rank}] Idle.")
