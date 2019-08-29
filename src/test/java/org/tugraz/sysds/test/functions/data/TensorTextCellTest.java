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
import org.tugraz.sysds.runtime.io.TensorReaderTextCell;
import org.tugraz.sysds.runtime.io.TensorWriterTextCell;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.test.TestUtils;

import java.util.Arrays;


public class TensorTextCellTest {
	@Test
	public void testReadWriteTextCellBasicTensorFP32() {
		testReadWriteTextCellBasicTensor(ValueType.FP32);
	}

	@Test
	public void testReadWriteTextCellBasicTensorFP64() {
		testReadWriteTextCellBasicTensor(ValueType.FP64);
	}

	@Test
	public void testReadWriteTextCellBasicTensorINT32() {
		testReadWriteTextCellBasicTensor(ValueType.INT32);
	}

	@Test
	public void testReadWriteTextCellBasicTensorINT64() {
		testReadWriteTextCellBasicTensor(ValueType.INT64);
	}

	@Test
	public void testReadWriteTextCellBasicTensorBoolean() {
		testReadWriteTextCellBasicTensor(ValueType.BOOLEAN);
	}

	@Test
	public void testReadWriteTextCellBasicTensorString() {
		testReadWriteTextCellBasicTensor(ValueType.STRING);
	}

	private void testReadWriteTextCellBasicTensor(ValueType vt) {
		TensorBlock tb1 = createBasicTensor(vt, 70, 30, 0.7);
		TensorBlock tb2 = writeAndReadBasicTensorTextCell(tb1);
		compareTensorBlocks(tb1, tb2);
	}

	@Test
	public void testReadWriteTextCellDataTensorFP32() {
		testReadWriteTextCellDataTensor(ValueType.FP32);
	}

	@Test
	public void testReadWriteTextCellDataTensorFP64() {
		testReadWriteTextCellDataTensor(ValueType.FP64);
	}

	@Test
	public void testReadWriteTextCellDataTensorINT32() {
		testReadWriteTextCellDataTensor(ValueType.INT32);
	}

	@Test
	public void testReadWriteTextCellDataTensorINT64() {
		testReadWriteTextCellDataTensor(ValueType.INT64);
	}

	@Test
	public void testReadWriteTextCellDataTensorBoolean() {
		testReadWriteTextCellDataTensor(ValueType.BOOLEAN);
	}

	@Test
	public void testReadWriteTextCellDataTensorString() {
		testReadWriteTextCellDataTensor(ValueType.STRING);
	}

	private void testReadWriteTextCellDataTensor(ValueType vt) {
		TensorBlock tb1 = createDataTensor(vt, 70, 30, 0.7);
		TensorBlock tb2 = writeAndReadDataTensorTextCell(tb1);
		compareTensorBlocks(tb1, tb2);
	}

	private TensorBlock writeAndReadBasicTensorTextCell(TensorBlock tb1) {
		try {
			long[] dims = new long[tb1.getNumDims()];
			for (int i = 0; i < dims.length; i++)
				dims[i] = tb1.getDim(i);
			TensorWriterTextCell writer = new TensorWriterTextCell();
			writer.writeTensorToHDFS(tb1, "a", dims, new int[]{1024, 1024});
			TensorReaderTextCell reader = new TensorReaderTextCell();
			return reader.readTensorFromHDFS("a", dims, new int[]{1024, 1024}, new ValueType[]{tb1.getValueType()});
		}
		catch (Exception ex) {
			throw new DMLRuntimeException(ex);
		}
	}

	private TensorBlock writeAndReadDataTensorTextCell(TensorBlock tb1) {
		try {
			long[] dims = Arrays.stream(tb1.getDims()).mapToLong(i -> i).toArray();
			TensorWriterTextCell writer = new TensorWriterTextCell();
			writer.writeTensorToHDFS(tb1, "a", dims, new int[]{1024, 1024});
			TensorReaderTextCell reader = new TensorReaderTextCell();
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
