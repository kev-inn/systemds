/*
 * Copyright 2019 Graz University of Technology
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tugraz.sysds.test.functions.data;

import org.junit.Assert;
import org.junit.Test;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.data.TensorBlock;
import org.tugraz.sysds.runtime.io.TensorReaderBinaryBlock;
import org.tugraz.sysds.runtime.io.TensorWriterBinaryBlock;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.test.TestUtils;

import java.util.Arrays;


public class TensorBinaryBlockTest {
	@Test
	public void testReadWriteBinaryBlockBasicTensorFP32() {
		testReadWriteBinaryBlockBasicTensor(ValueType.FP32);
	}

	@Test
	public void testReadWriteBinaryBlockBasicTensorFP64() {
		testReadWriteBinaryBlockBasicTensor(ValueType.FP64);
	}

	@Test
	public void testReadWriteBinaryBlockBasicTensorINT32() {
		testReadWriteBinaryBlockBasicTensor(ValueType.INT32);
	}

	@Test
	public void testReadWriteBinaryBlockBasicTensorINT64() {
		testReadWriteBinaryBlockBasicTensor(ValueType.INT64);
	}

	@Test
	public void testReadWriteBinaryBlockBasicTensorBoolean() {
		testReadWriteBinaryBlockBasicTensor(ValueType.BOOLEAN);
	}

	@Test
	public void testReadWriteBinaryBlockBasicTensorString() {
		testReadWriteBinaryBlockBasicTensor(ValueType.STRING);
	}

	private void testReadWriteBinaryBlockBasicTensor(ValueType vt) {
		TensorBlock tb1 = createBasicTensor(vt, 70, 30, 0.7);
		TensorBlock tb2 = writeAndReadBasicTensorBinaryBlock(tb1);
		compareTensorBlocks(tb1, tb2);
	}

	@Test
	public void testReadWriteBinaryBlockDataTensorFP32() {
		testReadWriteBinaryBlockDataTensor(ValueType.FP32);
	}

	@Test
	public void testReadWriteBinaryBlockDataTensorFP64() {
		testReadWriteBinaryBlockDataTensor(ValueType.FP64);
	}

	@Test
	public void testReadWriteBinaryBlockDataTensorINT32() {
		testReadWriteBinaryBlockDataTensor(ValueType.INT32);
	}

	@Test
	public void testReadWriteBinaryBlockDataTensorINT64() {
		testReadWriteBinaryBlockDataTensor(ValueType.INT64);
	}

	@Test
	public void testReadWriteBinaryBlockDataTensorBoolean() {
		testReadWriteBinaryBlockDataTensor(ValueType.BOOLEAN);
	}

	@Test
	public void testReadWriteBinaryBlockDataTensorString() {
		testReadWriteBinaryBlockDataTensor(ValueType.STRING);
	}

	private void testReadWriteBinaryBlockDataTensor(ValueType vt) {
		TensorBlock tb1 = createDataTensor(vt, 70, 30, 0.7);
		TensorBlock tb2 = writeAndReadDataTensorBinaryBlock(tb1);
		compareTensorBlocks(tb1, tb2);
	}

	private TensorBlock writeAndReadBasicTensorBinaryBlock(TensorBlock tb1) {
		try {
			long[] dims = Arrays.stream(tb1.getDims()).mapToLong(i -> i).toArray();
			TensorWriterBinaryBlock writer = new TensorWriterBinaryBlock();
			writer.writeTensorToHDFS(tb1, "a", dims, new int[]{1024, 1024});
			TensorReaderBinaryBlock reader = new TensorReaderBinaryBlock();
			return reader.readTensorFromHDFS("a", dims, new int[]{1024, 1024}, new ValueType[]{tb1.getValueType()});
		}
		catch (Exception ex) {
			throw new DMLRuntimeException(ex);
		}
	}

	private TensorBlock writeAndReadDataTensorBinaryBlock(TensorBlock tb1) {
		try {
			long[] dims = Arrays.stream(tb1.getDims()).mapToLong(i -> i).toArray();
			TensorWriterBinaryBlock writer = new TensorWriterBinaryBlock();
			writer.writeTensorToHDFS(tb1, "a", dims, new int[]{1024, 1024});
			TensorReaderBinaryBlock reader = new TensorReaderBinaryBlock();
			return reader.readTensorFromHDFS("a", dims, new int[]{1024, 1024}, tb1.getSchema());
		}
		catch (Exception ex) {
			throw new DMLRuntimeException(ex);
		}
	}

	private TensorBlock createBasicTensor(ValueType vt, int rows, int cols, double sparsity) {
		return DataConverter.convertToTensorBlock(TestUtils.round(
				MatrixBlock.randOperations(rows, cols, sparsity, 0, 10, "uniform", 7)), vt, true);
	}

	private TensorBlock createDataTensor(ValueType vt, int rows, int cols, double sparsity) {
		return DataConverter.convertToTensorBlock(TestUtils.round(
				MatrixBlock.randOperations(rows, cols, sparsity, 0, 10, "uniform", 7)), vt, false);
	}

	private void compareTensorBlocks(TensorBlock tb1, TensorBlock tb2) {
		Assert.assertEquals(tb1.getValueType(), tb2.getValueType());
		Assert.assertArrayEquals(tb1.getSchema(), tb2.getSchema());
		Assert.assertEquals(tb1.getNumRows(), tb2.getNumRows());
		Assert.assertEquals(tb1.getNumColumns(), tb2.getNumColumns());
		for (int i = 0; i < tb1.getNumRows(); i++)
			for (int j = 0; j < tb1.getNumColumns(); j++)
				Assert.assertEquals(Double.valueOf(tb1.get(i, j)),
						Double.valueOf(tb2.get(i, j)));
	}
}