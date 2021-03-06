/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

/* DO NOT EDIT THIS FILE - it is machine generated */

#ifndef __eu_stratosphere_nephele_io_compression_library_lzma_LzmaDecompressor__
#define __eu_stratosphere_nephele_io_compression_library_lzma_LzmaDecompressor__

#include <jni.h>

#ifdef __cplusplus
extern "C"
{
#endif

JNIEXPORT void JNICALL Java_eu_stratosphere_nephele_io_compression_library_lzma_LzmaDecompressor_initIDs (JNIEnv *env, jclass);
JNIEXPORT void JNICALL Java_eu_stratosphere_nephele_io_compression_library_lzma_LzmaDecompressor_init (JNIEnv *env, jclass, jint);
JNIEXPORT void JNICALL Java_eu_stratosphere_nephele_io_compression_library_lzma_LzmaDecompressor_finish (JNIEnv *env, jobject, jint);
JNIEXPORT void JNICALL Java_eu_stratosphere_nephele_io_compression_library_lzma_LzmaDecompressor_finishAll (JNIEnv *env, jclass);
JNIEXPORT jint JNICALL Java_eu_stratosphere_nephele_io_compression_library_lzma_LzmaDecompressor_decompressBytesDirect (JNIEnv *env, jobject, jint);
JNIEXPORT jint JNICALL Java_eu_stratosphere_nephele_io_compression_library_lzma_LzmaDecompressor_getAmountOfConsumedInput (JNIEnv *env, jobject, jint);
JNIEXPORT jint JNICALL Java_eu_stratosphere_nephele_io_compression_library_lzma_LzmaDecompressor_getAmountOfConsumedOutput (JNIEnv *env, jobject, jint);
#undef eu_stratosphere_nephele_io_compression_library_lzma_LzmaDecompressor_SIZE_LENGTH
#define eu_stratosphere_nephele_io_compression_library_lzma_LzmaDecompressor_SIZE_LENGTH 13L
#undef eu_stratosphere_nephele_io_compression_library_lzma_LzmaDecompressor_UNCOMPRESSED_BLOCKSIZE_LENGTH
#define eu_stratosphere_nephele_io_compression_library_lzma_LzmaDecompressor_UNCOMPRESSED_BLOCKSIZE_LENGTH 4L
#undef eu_stratosphere_nephele_io_compression_library_lzma_LzmaDecompressor_LZMA_PROPS_SIZE
#define eu_stratosphere_nephele_io_compression_library_lzma_LzmaDecompressor_LZMA_PROPS_SIZE 5L

#ifdef __cplusplus
}
#endif

#endif /* __eu_stratosphere_nephele_io_compression_library_lzma_LzmaDecompressor__ */
