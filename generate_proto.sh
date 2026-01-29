#!/bin/bash

# Script to generate Python code from .proto file

echo "Generating Python code from calculator.proto..."

python -m grpc_tools.protoc \
    --python_out=. \
    --grpc_python_out=. \
    --proto_path=. \
    calculator.proto

echo "Code generation complete!"
echo "Generated files: calculator_pb2.py and calculator_pb2_grpc.py"

