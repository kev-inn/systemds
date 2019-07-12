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

import java.util.Arrays;


public class DenseBlockSetDenseBlockTest
{
	@Test
	public void testDenseBlock2FP32SetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlock2(ValueType.FP32);
		DenseBlock dbSet = getDenseBlock2(ValueType.FP32);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlock2FP64SetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlock2(ValueType.FP64);
		DenseBlock dbSet = getDenseBlock2(ValueType.FP64);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlock2BoolSetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlock2(ValueType.BOOLEAN);
		DenseBlock dbSet = getDenseBlock2(ValueType.BOOLEAN);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlock2Int32SetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlock2(ValueType.INT32);
		DenseBlock dbSet = getDenseBlock2(ValueType.INT32);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlock2Int64SetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlock2(ValueType.INT64);
		DenseBlock dbSet = getDenseBlock2(ValueType.INT64);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlock2StringSetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlock2(ValueType.STRING);
		DenseBlock dbSet = getDenseBlock2(ValueType.STRING);
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 5; j++) {
				dbSet.set(new int[]{i,j}, "test");
			}
		}
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlockLarge2FP32SetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlockLarge2(ValueType.FP32);
		DenseBlock dbSet = getDenseBlockLarge2(ValueType.FP32);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlockLarge2FP64SetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlockLarge2(ValueType.FP64);
		DenseBlock dbSet = getDenseBlockLarge2(ValueType.FP64);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlockLarge2BoolSetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlockLarge2(ValueType.BOOLEAN);
		DenseBlock dbSet = getDenseBlockLarge2(ValueType.BOOLEAN);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlockLarge2Int32SetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlockLarge2(ValueType.INT32);
		DenseBlock dbSet = getDenseBlockLarge2(ValueType.INT32);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlockLarge2Int64SetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlockLarge2(ValueType.INT64);
		DenseBlock dbSet = getDenseBlockLarge2(ValueType.INT64);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlockLarge2StringSetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlockLarge2(ValueType.STRING);
		DenseBlock dbSet = getDenseBlockLarge2(ValueType.STRING);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlock3FP32SetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlock3(ValueType.FP32);
		DenseBlock dbSet = getDenseBlock3(ValueType.FP32);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlock3FP64SetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlock3(ValueType.FP64);
		DenseBlock dbSet = getDenseBlock3(ValueType.FP64);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlock3BoolSetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlock3(ValueType.BOOLEAN);
		DenseBlock dbSet = getDenseBlock3(ValueType.BOOLEAN);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlock3Int32SetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlock3(ValueType.INT32);
		DenseBlock dbSet = getDenseBlock3(ValueType.INT32);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlock3Int64SetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlock3(ValueType.INT64);
		DenseBlock dbSet = getDenseBlock3(ValueType.INT64);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlock3StringSetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlock3(ValueType.STRING);
		DenseBlock dbSet = getDenseBlock3(ValueType.STRING);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlockLarge3FP32SetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlockLarge3(ValueType.FP32);
		DenseBlock dbSet = getDenseBlockLarge3(ValueType.FP32);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlockLarge3FP64SetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlockLarge3(ValueType.FP64);
		DenseBlock dbSet = getDenseBlockLarge3(ValueType.FP64);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlockLarge3BoolSetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlockLarge3(ValueType.BOOLEAN);
		DenseBlock dbSet = getDenseBlockLarge3(ValueType.BOOLEAN);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlockLarge3Int32SetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlockLarge3(ValueType.INT32);
		DenseBlock dbSet = getDenseBlockLarge3(ValueType.INT32);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlockLarge3Int64SetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlockLarge3(ValueType.INT64);
		DenseBlock dbSet = getDenseBlockLarge3(ValueType.INT64);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
	}

	@Test
	public void testDenseBlockLarge3StringSetDenseBlock() throws Exception {
		DenseBlock db = getDenseBlockLarge3(ValueType.STRING);
		DenseBlock dbSet = getDenseBlockLarge3(ValueType.STRING);
		dbSet.set(1);
		db.set(dbSet);
		compareDenseBlocks(db, dbSet);
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

	private void compareDenseBlocks(DenseBlock left, DenseBlock right) {
	    Assert.assertEquals(left.numDims(), right.numDims());
		for (long i = 0; i < left.size(); i++) {
			int[] index = new int[left.numDims()];
			for (int ix = 0; ix < left.numDims() - 1; ix++) {
			    Assert.assertEquals(left.getDim(ix), right.getDim(ix));
				index[ix] = (int)((i % left.getDim(ix)) / right.getDim(ix + 1));
			}
			Assert.assertEquals(left.getDim(left.numDims() - 1), right.getDim(left.numDims() - 1));
			index[left.numDims() - 1] = (int)(i % left.getDim(left.numDims() - 1));
			if (left instanceof DenseBlockString || left instanceof DenseBlockLString) {
				Assert.assertEquals(left.getString(index), right.getString(index));
			} else {
				Assert.assertEquals(left.get(index), right.get(index), 0);
			}
		}
	}
}
