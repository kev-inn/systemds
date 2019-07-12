/*
 * Copyright 2018 Graz University of Technology
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

import org.junit.Assert;
import org.junit.Test;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.data.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;


public class DenseBlockConstIndexingTest 
{
	@Test
	public void testIndexDenseBlock2FP32Const() throws Exception {
		DenseBlock db = getDenseBlock2(ValueType.FP32);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				Assert.assertEquals(7.3, db.get(i, j), 1e-5);
	}
	
	@Test
	public void testIndexDenseBlock2FP64Const() throws Exception {
		DenseBlock db = getDenseBlock2(ValueType.FP64);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				Assert.assertEquals(7.3, db.get(i, j), 0);
	}
	
	@Test
	public void testIndexDenseBlock2BoolConst() throws Exception {
		DenseBlock db = getDenseBlock2(ValueType.BOOLEAN);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				Assert.assertEquals(1, db.get(i, j), 0);
	}
	
	@Test
	public void testIndexDenseBlock2Int32Const() throws Exception {
		DenseBlock db = getDenseBlock2(ValueType.INT32);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				Assert.assertEquals(7, db.get(i, j), 0);
	}
	
	@Test
	public void testIndexDenseBlock2Int64Const() throws Exception {
		DenseBlock db = getDenseBlock2(ValueType.INT64);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				Assert.assertEquals(7, db.get(i, j), 0);
	}

	@Test
	public void testIndexDenseBlock2StringConst() throws Exception {
		DenseBlock db = getDenseBlock2(ValueType.STRING);
		db.set(new int[]{1,3}, "hello");
		Assert.assertEquals("hello", db.getString(new int[]{1,3}));
	}

	@Test
	public void testIndexDenseBlockLarge2FP32Const() throws Exception {
		DenseBlock db = getDenseBlockLarge2(ValueType.FP32);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				Assert.assertEquals(7.3, db.get(i, j), 1e-5);
	}

	@Test
	public void testIndexDenseBlockLarge2FP64Const() throws Exception {
		DenseBlock db = getDenseBlockLarge2(ValueType.FP64);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				Assert.assertEquals(7.3, db.get(i, j), 0);
	}

	@Test
	public void testIndexDenseBlockLarge2BoolConst() throws Exception {
		DenseBlock db = getDenseBlockLarge2(ValueType.BOOLEAN);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				Assert.assertEquals(1, db.get(i, j), 0);
	}

	@Test
	public void testIndexDenseBlockLarge2Int32Const() throws Exception {
		DenseBlock db = getDenseBlockLarge2(ValueType.INT32);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				Assert.assertEquals(7, db.get(i, j), 0);
	}

	@Test
	public void testIndexDenseBlockLarge2Int64Const() throws Exception {
		DenseBlock db = getDenseBlockLarge2(ValueType.INT64);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				Assert.assertEquals(7, db.get(i, j), 0);
	}

	@Test
	public void testIndexDenseBlockLarge2StringConst() throws Exception {
		DenseBlock db = getDenseBlockLarge2(ValueType.STRING);
		db.set(new int[]{1,3}, "hello");
		Assert.assertEquals("hello", db.getString(new int[]{1,3}));
	}

	@Test
	public void testIndexDenseBlock3FP32Const() throws Exception {
		DenseBlock db = getDenseBlock3(ValueType.FP32);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				for(int k=0; k<7; k++)
					Assert.assertEquals(7.3, db.get(new int[]{i,j,k}), 1e-5);
	}
	
	@Test
	public void testIndexDenseBlock3FP64Const() throws Exception {
		DenseBlock db = getDenseBlock3(ValueType.FP64);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				for(int k=0; k<7; k++)
					Assert.assertEquals(7.3, db.get(new int[]{i,j,k}), 0);
	}
	
	@Test
	public void testIndexDenseBlock3BoolConst() throws Exception {
		DenseBlock db = getDenseBlock3(ValueType.BOOLEAN);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				for(int k=0; k<7; k++)
					Assert.assertEquals(1, db.get(new int[]{i,j,k}), 0);
	}
	
	@Test
	public void testIndexDenseBlock3Int32Const() throws Exception {
		DenseBlock db = getDenseBlock3(ValueType.INT32);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				for(int k=0; k<7; k++)
					Assert.assertEquals(7, db.get(new int[]{i,j,k}), 0);
	}
	
	@Test
	public void testIndexDenseBlock3Int64Const() throws Exception {
		DenseBlock db = getDenseBlock3(ValueType.INT64);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				for(int k=0; k<7; k++)
					Assert.assertEquals(7, db.get(new int[]{i,j,k}), 0);
	}

	@Test
	public void testIndexDenseBlock3StringConst() throws Exception {
		DenseBlock db = getDenseBlock3(ValueType.STRING);
		db.set(new int[]{0,4,2}, "hello");
		Assert.assertEquals("hello", db.getString(new int[]{0,4,2}));
	}

	@Test
	public void testIndexDenseBlockLarge3FP32Const() throws Exception {
		DenseBlock db = getDenseBlockLarge3(ValueType.FP32);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				for(int k=0; k<7; k++)
					Assert.assertEquals(7.3, db.get(new int[]{i,j,k}), 1e-5 );
	}

	@Test
	public void testIndexDenseBlockLarge3FP64Const() throws Exception {
		DenseBlock db = getDenseBlockLarge3(ValueType.FP64);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				for(int k=0; k<7; k++)
					Assert.assertEquals(7.3, db.get(new int[]{i,j,k}), 0);
	}

	@Test
	public void testIndexDenseBlockLarge3BoolConst() throws Exception {
		DenseBlock db = getDenseBlockLarge3(ValueType.BOOLEAN);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				for(int k=0; k<7; k++)
					Assert.assertEquals(1, db.get(new int[]{i,j,k}), 0);
	}

	@Test
	public void testIndexDenseBlockLarge3Int32Const() throws Exception {
		DenseBlock db = getDenseBlockLarge3(ValueType.INT32);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				for(int k=0; k<7; k++)
					Assert.assertEquals(7, db.get(new int[]{i,j,k}), 0);
	}

	@Test
	public void testIndexDenseBlockLarge3Int64Const() throws Exception {
		DenseBlock db = getDenseBlockLarge3(ValueType.INT64);
		db.set(7.3);
		for(int i=0; i<db.numRows(); i++)
			for(int j=0; j<5; j++)
				for(int k=0; k<7; k++)
					Assert.assertEquals(7, db.get(new int[]{i,j,k}), 0);
	}

	@Test
	public void testIndexDenseBlockLarge3StringConst() throws Exception {
		DenseBlock db = getDenseBlockLarge3(ValueType.STRING);
		db.set(new int[]{0,4,2}, "hello");
		Assert.assertEquals("hello", db.getString(new int[]{0,4,2}));
	}

	private DenseBlock getDenseBlock2(ValueType vt) {
		return DenseBlockFactory.createDenseBlock(vt, new int[] {3,5});
	}
	
	private DenseBlock getDenseBlock3(ValueType vt) {
		return DenseBlockFactory.createDenseBlock(vt, new int[] {3,5,7});
	}

	private DenseBlock getDenseBlockLarge2(ValueType vt) {
		int[] dims = {3,5};
		switch (vt) {
			case FP32: return new DenseBlockLFP32(dims);
			case FP64: return new DenseBlockLFP64(dims);
			case BOOLEAN: return new DenseBlockLBool(dims);
			case INT32: return new DenseBlockLInt32(dims);
			case INT64: return new DenseBlockLInt64(dims);
			case STRING: return new DenseBlockLString(dims);
			default: throw new NotImplementedException();
		}
	}

	private DenseBlock getDenseBlockLarge3(ValueType vt) {
		int[] dims = {3,5,7};
		switch (vt) {
			case FP32: return new DenseBlockLFP32(dims);
			case FP64: return new DenseBlockLFP64(dims);
			case BOOLEAN: return new DenseBlockLBool(dims);
			case INT32: return new DenseBlockLInt32(dims);
			case INT64: return new DenseBlockLInt64(dims);
			case STRING: return new DenseBlockLString(dims);
			default: throw new NotImplementedException();
		}
	}
}
