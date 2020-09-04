mix_3x

gRPC
Формирование библиотек на основании описания протокола Api.proto
    python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. Api.proto
    результат:
      Api_pb2_grpc.py
      Api_pb2.py