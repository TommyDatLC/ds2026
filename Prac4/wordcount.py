import threading
import sys
from collections import defaultdict

WORKER_COUNT = 3

shared_counts = {}
unique_total = 0

mutex = threading.Lock()

def count_words(params):
    worker_index = params['index']
    text_block = params['text']

    local_map = defaultdict(int)
    tokens = text_block.split()

    for token in tokens:
        local_map[token] += 1

    print(f"[Worker {worker_index}] Found {len(local_map)} unique words.")

    global shared_counts, unique_total

    mutex.acquire()
    try:
        for token, freq in local_map.items():
            if token in shared_counts:
                shared_counts[token] += freq
            else:
                shared_counts[token] = freq
                unique_total += 1
    finally:
        mutex.release()

def main():
    try:
        with open("Prac4/input.txt", "r") as f:
            data_text = f.read()
    except FileNotFoundError:
        print("Cannot open file input.txt", file=sys.stderr)
        return 1
    except Exception as err:
        print(f"Error reading file: {err}", file=sys.stderr)
        return 1

    worker_threads = []
    word_list = data_text.split()

    chunk_size = len(word_list) // WORKER_COUNT

    print("--- RUNNING MAPREDUCE WITH MULTITHREADING ---")

    for i in range(WORKER_COUNT):
        start_pos = i * chunk_size

        if i == WORKER_COUNT - 1:
            part_words = word_list[start_pos:]
        else:
            part_words = word_list[start_pos:start_pos + chunk_size]

        part_text = ' '.join(part_words)

        params = {'index': i + 1, 'text': part_text}

        t = threading.Thread(target=count_words, args=(params,))
        worker_threads.append(t)
        t.start()

    for t in worker_threads:
        t.join()

    print("\n--- FINAL RESULTS ---")

    for token, freq in sorted(shared_counts.items()):
        print(f"{token}: {freq}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
