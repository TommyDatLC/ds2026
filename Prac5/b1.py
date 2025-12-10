from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import threading

# Lock để tránh in chồng chéo khi nhiều thread
print_lock = threading.Lock()

# =======================
# MAPPER
# =======================
def map_line(line):
    file_path = line.strip()
    if not file_path:
        return None

    return ("longest_path", (len(file_path), file_path))

# =======================
# REDUCER
# =======================
def reduce_max(key, values):
    # values: list[(length, path)]
    return max(values)

# =======================
# MAP TASK (1 THREAD / FILE)
# =======================
def process_file(file_name):
    """
    Mỗi thread xử lý 1 file input
    """
    mapped_results = []

    try:
        with open(file_name, 'r', encoding='utf-8') as f:
            for line_number, line in enumerate(f, 1):
                # gọi hàm mapper cho mỗi dòng
                mapped = map_line(line)
                if mapped:
                    # lưu key, value vào kết quả thread
                    mapped_results.append(mapped)

                    key, (path_length, file_path) = mapped
                    with print_lock:
                        # in thông tin đường dẫn và độ dài
                        print(f"[{file_name}] Line {line_number:3d} | Length={path_length:3d} | {file_path}")

    except FileNotFoundError:
        with print_lock:
            print(f"❌ Không tìm thấy file: {file_name}")

    return mapped_results

# =======================
# MAPREDUCE (MULTITHREADING)
# =======================
def run_mapreduce(input_file_list):
    all_mapped = []

    # -------- MAP PHASE (parallel) --------
    with ThreadPoolExecutor(max_workers=min(8, len(input_file_list))) as executor:
        futures = [executor.submit(process_file, f) for f in input_file_list]

        # lấy kết quả khi các thread hoàn thành
        for future in as_completed(futures):
            all_mapped.extend(future.result())

    if not all_mapped:
        print("❌ Không có dữ liệu hợp lệ")
        return

    # -------- SHUFFLE PHASE --------
    grouped_by_key = defaultdict(list)

    print(all_mapped)
    for key, value in all_mapped:
        grouped_by_key[key].append(value)

    # -------- REDUCE PHASE --------
    for key, values in grouped_by_key.items():
        # tìm longest path
        max_length, longest_path = reduce_max(key, values)

        print(f"Độ dài lớn nhất: {max_length} ký tự")
        print("Đường dẫn dài nhất:")
        print(f"  {longest_path}")

# =======================
# MAIN
# =======================
def main():
    if len(sys.argv) < 2:
        print("❌ Cần ít nhất 1 file input")
        sys.exit(1)

    input_file_list = sys.argv[1:]
    run_mapreduce(input_file_list)

if __name__ == "__main__":
    main()
