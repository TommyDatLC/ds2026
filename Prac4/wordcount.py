"""
====================================================================================
CHƯƠNG TRÌNH ĐẾM TỪ SỬ DỤNG MAPREDUCE VỚI ĐA LUỒNG (MULTITHREADING)
====================================================================================
Mục đích: Đếm số lần xuất hiện của mỗi từ trong file văn bản bằng kỹ thuật
         MapReduce với nhiều luồng (thread) chạy song song.

Các bước:
    1. Đọc file input.txt
    2. Chia văn bản thành nhiều phần nhỏ
    3. Mỗi luồng xử lý một phần và đếm từ
    4. Gộp kết quả từ tất cả các luồng
    5. In kết quả cuối cùng
====================================================================================
"""

import threading  # Thư viện để tạo và quản lý các luồng (thread)
import sys  # Thư viện hệ thống, dùng để thoát chương trình với mã lỗi
from collections import defaultdict  # Dictionary đặc biệt, tự động tạo giá trị mặc định cho key mới

# ==================== CẤU HÌNH ====================
NUM_MAPPERS = 3  # Số luồng xử lý song song (có thể điều chỉnh: 2, 4, 5...)

# ==================== DỮ LIỆU DÙNG CHUNG ====================
# Dictionary lưu kết quả: {"từ": số_lần_xuất_hiện}
global_results = {}
global_count = 0

# Lock (khóa) bảo vệ dữ liệu dùng chung
# Khi một luồng đang ghi dữ liệu, các luồng khác phải đợi
lock = threading.Lock()


# ==================== HÀM MAPPER ====================
def mapper(args):
    """
    Mỗi luồng chạy hàm này để xử lý một phần văn bản.
    
    Tham số:
        args['id']: Số thứ tự của luồng
        args['chunk']: Phần văn bản cần xử lý
    """
    thread_id = args['id']
    text_chunk = args['chunk']
    
    # BƯỚC 1: Đếm từ cục bộ (mỗi luồng đếm riêng)
    local_counts = defaultdict(int)  # Tự động khởi tạo giá trị 0 cho từ mới
    
    # Tách văn bản thành danh sách các từ (phân cách bởi khoảng trắng)
    words = text_chunk.split()
    
    # Duyệt qua từng từ và tăng số đếm lên 1
    for word in words:
        local_counts[word] += 1  # Nếu từ chưa tồn tại, defaultdict tự tạo với giá trị 0
    
    print(f"[Mapper {thread_id}] Found {len(local_counts)} unique words.")
    
    # BƯỚC 2: Gộp kết quả vào bộ nhớ dùng chung (CRITICAL SECTION)
    # Khai báo sử dụng biến toàn cục
    global global_results, global_count
    
    # KHÓA (LOCK): Đảm bảo chỉ 1 luồng được truy cập vào phần này tại một thời điểm
    # Ngăn chặn race condition (xung đột dữ liệu khi nhiều luồng ghi đồng thời)
    lock.acquire()  # Khóa - chỉ cho 1 luồng vào tại một thời điểm
    try:
        # Gộp kết quả từ local_counts vào global_results
        for word, count in local_counts.items():
            if word in global_results:
                # Từ đã tồn tại: cộng thêm số lần xuất hiện
                global_results[word] += count
            else:
                # Từ mới: thêm vào dictionary
                global_results[word] = count
                global_count += 1  # Tăng số lượng từ duy nhất
    finally:
        # finally đảm bảo luôn được thực thi, kể cả khi có lỗi
        lock.release()  # Giải phóng khóa cho luồng khác


def main():
    # BƯỚC 1: Đọc file input
    try:
        with open("Prac4/input.txt", "r") as fp:
            full_text = fp.read()
    except FileNotFoundError:
        print("Cannot open file input.txt", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error reading file: {e}", file=sys.stderr)
        return 1
    
    # BƯỚC 2: Chia văn bản và tạo các luồng
    threads = []  # Danh sách chứa các thread object
    words = full_text.split()  # Tách toàn bộ văn bản thành list các từ
    
    # Tính số từ mỗi luồng sẽ xử lý (chia đều)
    # Ví dụ: 100 từ / 3 luồng = 33 từ/luồng (lấy phần nguyên)
    words_per_chunk = len(words) // NUM_MAPPERS  # Chia lấy phần nguyên
    
    print("--- STARTING MAPREDUCE WITH PYTHON MULTITHREADING ---")
    
    # Tạo NUM_MAPPERS luồng, mỗi luồng xử lý một phần văn bản
    for i in range(NUM_MAPPERS):
        # Xác định vị trí bắt đầu của chunk này
        start_idx = i * words_per_chunk
        
        # Luồng cuối cùng nhận tất cả từ còn lại (kể cả phần dư)
        # Ví dụ: 100 từ / 3 luồng → luồng 1,2 nhận 33 từ, luồng 3 nhận 34 từ
        if i == NUM_MAPPERS - 1:
            chunk_words = words[start_idx:]  # Lấy từ start_idx đến hết
        else:
            chunk_words = words[start_idx:start_idx + words_per_chunk]
        
        # Ghép list từ thành chuỗi văn bản
        chunk = ' '.join(chunk_words)
        
        # Chuẩn bị tham số cho hàm mapper
        args = {'id': i + 1, 'chunk': chunk}
        
        # Tạo thread mới: target=hàm sẽ chạy, args=tham số truyền vào
        thread = threading.Thread(target=mapper, args=(args,))
        threads.append(thread)  # Lưu thread vào list để quản lý
        thread.start()  # Bắt đầu chạy thread (gọi hàm mapper)
    
    # BƯỚC 3: Đợi tất cả luồng hoàn thành (ĐỒNG BỘ HÓA)
    for thread in threads:
        # join(): Chặn chương trình chính đến khi thread này hoàn thành
        # Đảm bảo tất cả thread xử lý xong trước khi in kết quả
        thread.join()
    
    # BƯỚC 4: In kết quả
    print("\n--- FINAL RESULTS ---")
    # sorted(): Sắp xếp theo thứ tự bảng chữ cái (alphabet)
    # items(): Trả về cặp (key, value) từ dictionary
    for word, count in sorted(global_results.items()):
        print(f"{word}: {count}")
    
    return 0


# Điểm bắt đầu chương trình
# __name__ == "__main__": Chỉ chạy khi file này được thực thi trực tiếp
# (không chạy khi file này được import vào file khác)
if __name__ == "__main__":
    sys.exit(main())  # Gọi hàm main() và thoát với mã trả về (0=thành công, 1=lỗi)

