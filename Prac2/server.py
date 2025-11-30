import grpc
from concurrent import futures
import file_transfer_pb2
import file_transfer_pb2_grpc

# Class này kế thừa từ bộ khung xương (Skeleton) mà gRPC tạo ra
class FileTransferServicer(file_transfer_pb2_grpc.FileTransferServicer):

    def Upload(self, request_iterator, context):
        # request_iterator chính là cái vòi nước đang xả dữ liệu từ Client sang
        
        file_object = None
        final_name = "unknown.bin" # Tên mặc định phòng khi Client bị ngáo

        print("Server: Đang mở cổng kết nối... Chờ hàng về.")

        for request in request_iterator:
            # KIỂM TRA: Gói này là Tên file hay là Dữ liệu?
            if request.HasField("info"):
                # Đây là Metadata (Tên file)
                final_name = request.info.filename
                print(f"Server: Phát hiện mục tiêu! Đang chuẩn bị nhận file '{final_name}'")
                
                # Mở file với chế độ 'wb' (write binary) - Ghi đè không thương tiếc
                file_object = open(final_name, 'wb')
            
            elif request.HasField("chunk_data"):
                # Đây là Thịt (Dữ liệu)
                if file_object:
                    file_object.write(request.chunk_data)
                else:
                    # Nếu Client ném dữ liệu trước khi ném tên -> Chửi ngay
                    print("Server: Ê! Gửi cái tên file trước đi bạn ơi!")
                    return file_transfer_pb2.UploadStatus(
                        success=False, 
                        message="Lỗi quy trình: Không thấy tên file đâu cả!"
                    )

        # Sau khi vòng lặp kết thúc (Client ngừng gửi)
        if file_object:
            file_object.close()
            print(f"Server: Đã nuốt trọn file '{final_name}'.")

        # Trả lời Client một câu cho nó yên tâm
        return file_transfer_pb2.UploadStatus(
            success=True,
            message=f"Upload thành công file {final_name}. Server đã nhận đủ!"
        )

def serve():
    # Khởi tạo Server với 10 luồng xử lý - Mạnh hơn cả dàn PC đào coin
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    file_transfer_pb2_grpc.add_FileTransferServicer_to_server(
        FileTransferServicer(), server
    )
    
    # Mở port 50051
    server.add_insecure_port('[::]:50051')
    print("Server đang rình rập tại port 50051...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()