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
 */

package org.tugraz.sysds.test.component.tensor;

import java.io.DataInput;
import java.io.DataOutput;

import org.junit.Assert;
import org.junit.Test;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.caching.CacheDataInput;
import org.tugraz.sysds.runtime.controlprogram.caching.CacheDataOutput;
import org.tugraz.sysds.runtime.data.TensorBlock;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.test.TestUtils;


public class TensorSerializationTest 
{
	@Test
	public void testSerializeBasicTensorFP32() {
		testSerializeBasicTensor(ValueType.FP32);
	}
	
	@Test
	public void testSerializeBasicTensorFP64() {
		testSerializeBasicTensor(ValueType.FP64);
	}
	
	@Test
	public void testSerializeBasicTensorINT32() {
		testSerializeBasicTensor(ValueType.INT32);
	}
	
	@Test
	public void testSerializeBasicTensorINT64() {
		testSerializeBasicTensor(ValueType.INT64);
	}
	
	@Test
	public void testSerializeBasicTensorBoolean() {
		testSerializeBasicTensor(ValueType.BOOLEAN);
	}

	private void testSerializeBasicTensor(ValueType vt) {
		TensorBlock tb1 = createBasicTensor(vt, 70, 30, 0.7);
		TensorBlock tb2 = serializeAndDeserializeTensorBlock(tb1);
		compareTensorBlocks(tb1, tb2);
	}

	@Test
	public void testSerializeDataTensorFP32() {
		testSerializeDataTensor(ValueType.FP32);
	}

	@Test
	public void testSerializeDataTensorFP64() {
		testSerializeDataTensor(ValueType.FP64);
	}

	@Test
	public void testSerializeDataTensorINT32() {
		testSerializeDataTensor(ValueType.INT32);
	}

	@Test
	public void testSerializeDataTensorINT64() {
		testSerializeDataTensor(ValueType.INT64);
	}

	@Test
	public void testSerializeDataTensorBoolean() {
		testSerializeDataTensor(ValueType.BOOLEAN);
	}

	private void testSerializeDataTensor(ValueType vt) {
		TensorBlock tb1 = createDataTensor(vt, 70, 30, 0.7);
		TensorBlock tb2 = serializeAndDeserializeTensorBlock(tb1);
		compareTensorBlocks(tb1, tb2);
	}

	private TensorBlock serializeAndDeserializeTensorBlock(TensorBlock tb1) {
		try {
			//serialize and deserialize tensor block
			byte[] bdata = new byte[(int)tb1.getExactSerializedSize()];
			DataOutput dout = new CacheDataOutput(bdata);
			tb1.write(dout); //tb1 serialized into bdata
			DataInput din = new CacheDataInput(bdata);
			TensorBlock tb2 = new TensorBlock();
			tb2.readFields(din); //bdata deserialized into tb2
			return tb2;
		}
		catch(Exception ex) {
			throw new DMLRuntimeException(ex);
		}
	}

	private TensorBlock createBasicTensor(ValueType vt, int rows, int cols, double sparsity) {
		return DataConverter.convertToTensorBlock(TestUtils.round(
				MatrixBlock.randOperations(rows, cols, sparsity, 0, 1, "uniform", 7)), vt, true);
	}

	private TensorBlock createDataTensor(ValueType vt, int rows, int cols, double sparsity) {
		return DataConverter.convertToTensorBlock(TestUtils.round(
				MatrixBlock.randOperations(rows, cols, sparsity, 0, 1, "uniform", 7)), vt, false);
	}

	private void compareTensorBlocks(TensorBlock tb1, TensorBlock tb2) {
		Assert.assertEquals(tb1.getValueType(), tb2.getValueType());
		Assert.assertArrayEquals(tb1.getSchema(), tb2.getSchema());
		Assert.assertEquals(tb1.getNumRows(), tb2.getNumRows());
		Assert.assertEquals(tb1.getNumColumns(), tb2.getNumColumns());
		for(int i=0; i<tb1.getNumRows(); i++)
			for(int j=0; j<tb1.getNumColumns(); j++)
				Assert.assertEquals(Double.valueOf(tb1.get(i, j)),
						Double.valueOf(tb2.get(i, j)));
	}
}
