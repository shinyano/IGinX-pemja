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
#ifndef _Included_arrow_postman
#define _Included_arrow_postman

#ifdef __cplusplus
extern "C" {
#endif

/* convert java arrow array to python */
JcpAPI_FUNC(PyObject*) JcpPy_passArrow(JNIEnv*, jobject, jlong, jlong, jint);

/* convert python arrow array to java */
JcpAPI_FUNC(jobject) JcpPy_recArrow(JNIEnv*, PyObject*);

#ifdef __cplusplus
}
#endif

#endif