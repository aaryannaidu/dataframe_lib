#include <arrow/api.h>
#include <iostream>

int main() {
    arrow::Int64Builder builder;
    builder.Append(1);
    builder.Append(2);
    builder.Append(3);
    
    std::shared_ptr<arrow::Array> array;
    arrow::Status status = builder.Finish(&array);
    
    if (!status.ok()) {
        std::cerr << "Arrow Error: " << status.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "Successfully created an Arrow array!" << std::endl;
    std::cout << "Array length: " << array->length() << std::endl;
    std::cout << "Array content: " << array->ToString() << std::endl;
    
    return 0;
}
