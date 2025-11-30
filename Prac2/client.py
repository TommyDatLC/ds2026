import grpc
import os
import file_transfer_pb2
import file_transfer_pb2_grpc

# Hàm này sinh ra các gói tin (generator)
def generate_requests(file_path):
    # 1. Xử lý tên file
    # Lấy ra: "boc_phot.mp4" - Gộp cả tên và đuôi [cite: 449]
    filename_only = os.path.basename(file_path)

    # 2. Gửi gói tin đầu tiên: CHỈ CHỨA THÔNG TIN FILE
    print(f"Client: Đang gửi thông tin file '{filename_only}'...")
    yield file_transfer_pb2.UploadRequest(
        info=file_transfer_pb2.FileInfo(filename=filename_only)
    )

    # 3. Gửi các gói tin tiếp theo: CHỨA DỮ LIỆU FILE (CHUNKS)
    print("Client: Đang xả hàng...")
    with open(file_path, 'rb') as f:
        while True:
            # Cắt mỗi miếng 1MB (1024*1024 bytes)
            chunk = f.read(1024 * 1024)
            if not chunk:
                break # Hết phim
            
            # Gói vào UploadRequest và bắn đi
            yield file_transfer_pb2.UploadRequest(chunk_data=chunk)

def run():
    # Kết nối tới cái động bàn tơ của Server
    #[cite_start]# [cite: 413] Create stub functions
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = file_transfer_pb2_grpc.FileTransferStub(channel)
        
        # Nhập tên file cần gửi (nhớ là file phải tồn tại nhé các thần đồng)
        file_to_send = input("Nhap file de gui vao day:")
        
        # Kiểm tra xem file có tồn tại không đã, không lại lỗi sấp mặt
        if not os.path.exists(file_to_send):
            print("Client: Ủa file đâu? Nhập sai đường dẫn rồi cha nội!")
            return

        try:
            # Gọi hàm Upload và truyền vào cái máy bắn đá (generator)
            response = stub.Upload(generate_requests(file_to_send))
            
            # In ra phán quyết cuối cùng của Server
            print(f"Server phản hồi: {response.message}")
            
        except grpc.RpcError as e:
            print(f"Toang rồi ông giáo ạ! Lỗi RPC: {e}")

if __name__ == '__main__':
    run()