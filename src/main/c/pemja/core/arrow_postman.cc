// Copyright 2022 Alibaba Group Holding Limited.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "Pemja.h"

#include "java_class/JavaClass.h"
#include "python_class/PythonClass.h"

#include <cstdint>
#include <iostream>
#include <vector>

#include <arrow/api.h>

using arrow::Int32Builder;
using arrow::Int64Builder;
using arrow::DoubleBuilder;
using arrow::StringBuilder;

struct row_data {
    int32_t col1;
    int64_t col2;
    double col3;
    std::string col4;
};//行结构

#define EXIT_ON_FAILURE(expr)                      \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      return EXIT_FAILURE;                         \
    }                                              \
  } while (0);

arrow::Status CreateTable(const std::vector<struct row_data>& rows,std::shared_ptr<arrow::Table>* table) {

    //使用arrow::jemalloc::MemoryPool::default_pool()构建器更有效，因为这可以适当增加底层内存区域的大小.
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    Int32Builder col1_builder(pool);
    Int64Builder col2_builder(pool);
    DoubleBuilder col3_builder(pool);
    StringBuilder col4_builder(pool);

    //现在我们可以循环我们现有的数据，并将其插入到构建器中。这里的' Append '调用可能会失败(例如，我们无法分配足够的额外内存)。因此我们需要检查它们的返回值。
    for (const row_data& row : rows) {
        ARROW_RETURN_NOT_OK(col1_builder.Append(row.col1));
        ARROW_RETURN_NOT_OK(col2_builder.Append(row.col2));
        ARROW_RETURN_NOT_OK(col3_builder.Append(row.col3));
        ARROW_RETURN_NOT_OK(col4_builder.Append(row.col4));
    }

    //添加空值,末尾值的元素为空
    ARROW_RETURN_NOT_OK(col1_builder.AppendNull());
    ARROW_RETURN_NOT_OK(col2_builder.AppendNull());
    ARROW_RETURN_NOT_OK(col3_builder.AppendNull());
    ARROW_RETURN_NOT_OK(col4_builder.AppendNull());

    std::shared_ptr<arrow::Array> col1_array;
    ARROW_RETURN_NOT_OK(col1_builder.Finish(&col1_array));
    std::shared_ptr<arrow::Array> col2_array;
    ARROW_RETURN_NOT_OK(col2_builder.Finish(&col2_array));
    std::shared_ptr<arrow::Array> col3_array;
    ARROW_RETURN_NOT_OK(col3_builder.Finish(&col3_array));
    std::shared_ptr<arrow::Array> col4_array;
    ARROW_RETURN_NOT_OK(col4_builder.Finish(&col4_array));

    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
            arrow::field("col1", arrow::int32()), arrow::field("col2", arrow::int64()), arrow::field("col3", arrow::float64()),
            arrow::field("col4", arrow::utf8())};

    auto schema = std::make_shared<arrow::Schema>(schema_vector);

    //最终的' table '变量是我们可以传递给其他可以使用Apache Arrow内存结构的函数的变量。这个对象拥有所有引用数据的所有权，
    //因此一旦我们离开构建表及其底层数组的函数的作用域，就不必关心未定义的引用。
    *table = arrow::Table::Make(schema, {col1_array, col2_array, col3_array,col4_array});

    return arrow::Status::OK();
}

// #include <jni.h>
// #include <arrow/array.h>
// #include <arrow/builder.h>
// #include <arrow/memory_pool.h>
// #include <iostream>
// #include <pybind11/pybind11.h>
// #include <arrow/python/pyarrow.h>
//
// namespace py = pybind11;
//
// extern "C" PyObject*
// JcpPy_passArrow(JNIEnv* env, jobject jobj, jlong dataAddress, jlong validityAddress, jint length) {
//     using arrow::Int32Builder;
//     using arrow::Buffer;
//     using arrow::Status;
//
//     // 使用传入的地址和长度创建Arrow Buffer
//     std::shared_ptr<Buffer> data = std::make_shared<Buffer>(reinterpret_cast<uint8_t*>(dataAddress), length * sizeof(int32_t));
//     std::shared_ptr<Buffer> validity = std::make_shared<Buffer>(reinterpret_cast<uint8_t*>(validityAddress), (length + 7) / 8);
//
//     // 使用ArrayBuilder创建Array
//     Int32Builder builder;
//     Status st = builder.AppendValues(reinterpret_cast<const int32_t*>(data->data()), length, validity->data());
//
//     if (!st.ok()) {
//         // 错误处理
//         std::cerr << "Arrow array building failed: " << st.ToString() << std::endl;
//         return nullptr;
//
//     }
//
//     // 完成Array的构建
//     std::shared_ptr<arrow::Array> array;
//     st = builder.Finish(&array);
//     if (!st.ok()) {
//         // 错误处理
//         std::cerr << "Arrow array finishing failed: " << st.ToString() << std::endl;
//         return nullptr;
//     }
//
//     // 使用arrow::Array进行数据处理...
//     // 例如，打印第一个元素（仅作为示例，实际应用中应更全面地处理数据）
//     auto intArray = std::static_pointer_cast<arrow::Int32Array>(array);
//     if (intArray->length() > 0 && !intArray->IsNull(0)) {
//         std::cout << "The first element is: " << intArray->Value(0) << std::endl;
//     }
//     auto pyarrow_lib_module = py::module_::import("pyarrow").attr("lib");
//     auto pyarrow_array_class = pyarrow_lib_module.attr("Array");
//
//     // 使用 Arrow 的 Python C API 转换
//     auto result = arrow::py::wrap_array(array);
//     return result;
// }
//
// jobject
// JcpPy_recArrow(JNIEnv*, PyObject*)
// {
//
//
//}