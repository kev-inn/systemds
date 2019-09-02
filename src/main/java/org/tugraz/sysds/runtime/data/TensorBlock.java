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

package org.tugraz.sysds.runtime.data;

import org.apache.commons.lang.NotImplementedException;
import org.tugraz.sysds.common.Types.BlockType;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.caching.CacheBlock;
import org.tugraz.sysds.runtime.io.IOUtilFunctions;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.UtilFunctions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

public class TensorBlock implements CacheBlock, Externalizable {
	private static final long serialVersionUID = -8768054067319422277L;
	
	private enum SERIALIZED_TYPES {
		EMPTY, BASIC, DATA
	}
	
	public static final int[] DEFAULT_DIMS = new int[]{0, 0};
	public static final ValueType DEFAULT_VTYPE = ValueType.FP64;

	private int[] _dims;
	private boolean _basic = true;

	private DataTensorBlock _dataTensor = null;
	private BasicTensorBlock _basicTensor = null;

	public TensorBlock() {
		this(DEFAULT_DIMS, true);
	}

	public TensorBlock(int[] dims, boolean basic) {
		_dims = dims;
		_basic = basic;
	}

	public TensorBlock(ValueType vt, int[] dims) {
		this(dims, true);
		_basicTensor = new BasicTensorBlock(vt, dims, false);
	}

	public TensorBlock(ValueType[] schema, int[] dims) {
		this(dims, false);
		_dataTensor = new DataTensorBlock(schema, dims);
	}

	public TensorBlock(double value) {
		_dims = new int[]{1, 1};
		_basicTensor = new BasicTensorBlock(value);
	}

	public TensorBlock(BasicTensorBlock basicTensor) {
		this(basicTensor._dims, true);
		_basicTensor = basicTensor;
	}

	public TensorBlock(DataTensorBlock dataTensor) {
		this(dataTensor._dims, false);
		_dataTensor = dataTensor;
	}

	public TensorBlock(TensorBlock that) {
		copy(that);
	}

	public void reset() {
		if (_basic) {
			if (_basicTensor == null)
				_basicTensor = new BasicTensorBlock(DEFAULT_VTYPE, _dims);
			_basicTensor.reset();
		}
		else {
			if (_dataTensor == null)
				_dataTensor = new DataTensorBlock(DEFAULT_VTYPE, _dims);
			_dataTensor.reset();
		}
	}

	public void reset(int[] dims) {
		_dims = dims;
		if (_basic) {
			if (_basicTensor == null)
				_basicTensor = new BasicTensorBlock(DEFAULT_VTYPE, _dims);
			_basicTensor.reset(dims);
		}
		else {
			if (_dataTensor == null)
				_dataTensor = new DataTensorBlock(DEFAULT_VTYPE, _dims);
			_dataTensor.reset(dims);
		}
	}

	public boolean isBasic() {
		return _basic;
	}

	public boolean isAllocated() {
		if (_basic)
			return _basicTensor != null && _basicTensor.isAllocated();
		else
			return _dataTensor != null && _dataTensor.isAllocated();
	}

	public TensorBlock allocateBlock() {
		if (_basic) {
			if (_basicTensor == null)
				_basicTensor = new BasicTensorBlock(DEFAULT_VTYPE, _dims);
			_basicTensor.allocateBlock();
		}
		else {
			if (_dataTensor == null)
				_dataTensor = new DataTensorBlock(DEFAULT_VTYPE, _dims);
			_dataTensor.allocateBlock();
		}
		return this;
	}

	public BasicTensorBlock getBasicTensor() {
		return _basicTensor;
	}

	public DataTensorBlock getDataTensor() {
		return _dataTensor;
	}

	public ValueType getValueType() {
		if (_basic)
			return _basicTensor == null ? DEFAULT_VTYPE : _basicTensor.getValueType();
		else
			return null;
	}

	public ValueType[] getSchema() {
		if (_basic)
			return null;
		else {
			if (_dataTensor == null) {
				//TODO perf, do not fill, instead save schema
				ValueType[] schema = new ValueType[getDim(1)];
				Arrays.fill(schema, DEFAULT_VTYPE);
				return schema;
			}
			else
				return _dataTensor.getSchema();
		}
	}

	public int getNumDims() {
		return _dims.length;
	}

	public int getNumRows() {
		return getDim(0);
	}

	public int getNumColumns() {
		return getDim(1);
	}

	@Override
	public long getInMemorySize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isShallowSerialize() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isShallowSerialize(boolean inclConvert) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void toShallowSerializeBlock() {
		// TODO Auto-generated method stub
	}

	@Override
	public void compactEmptyBlock() {
		// TODO Auto-generated method stub
	}

	@Override
	public CacheBlock slice(int rl, int ru, int cl, int cu, CacheBlock block) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void merge(CacheBlock that, boolean appendOnly) {
		// TODO Auto-generated method stub
	}

	public int getDim(int i) {
		return _dims[i];
	}

	public int[] getDims() {
		return _dims;
	}

	public long[] getLongDims() {
		return Arrays.stream(_dims).mapToLong(i -> i).toArray();
	}

	/**
	 * Calculates the next index array. Note that if the given index array was the last element, the next index will
	 * be the first one.
	 *
	 * @param dims the dims array for which we have to decide the next index
	 * @param ix the index array which will be incremented to the next index array
	 */
	public static void getNextIndexes(int[] dims, int[] ix) {
		int i = ix.length - 1;
		ix[i]++;
		//calculating next index
		if (ix[i] == dims[i]) {
			while (ix[i] == dims[i]) {
				ix[i] = 0;
				i--;
				if (i < 0) {
					//we are finished
					break;
				}
				ix[i]++;
			}
		}
	}

	/**
	 * Calculates the next index array. Note that if the given index array was the last element, the next index will
	 * be the first one.
	 *
	 * @param ix the index array which will be incremented to the next index array
	 */
	public void getNextIndexes(int[] ix) {
		getNextIndexes(_dims, ix);
	}

	public boolean isVector() {
		return getNumDims() <= 2
				&& (getDim(0) == 1 || getDim(1) == 1);
	}

	public boolean isMatrix() {
		return getNumDims() == 2
				&& (getDim(0) > 1 && getDim(1) > 1);
	}

	public long getLength() {
		return UtilFunctions.prod(_dims);
	}

	public boolean isEmpty() {
		return isEmpty(false);
	}

	public boolean isEmpty(boolean safe) {
		if (_basic)
			return _basicTensor == null || _basicTensor.isEmpty(safe);
		else
			return _dataTensor == null || _dataTensor.isEmpty(safe);
	}

	public long getNonZeros() {
		if (!isAllocated())
			return 0;
		if (_basic)
			return _basicTensor.getNonZeros();
		else
			return _dataTensor.getNonZeros();
	}

	public Object get(int[] ix) {
		if (_basic && _basicTensor != null)
			return _basicTensor.get(ix);
		else if (_dataTensor != null)
			return _dataTensor.get(ix);
		return 0.0;
	}

	public double get(int r, int c) {
		if (_basic && _basicTensor != null)
			return _basicTensor.get(r, c);
		else if (_dataTensor != null)
			return _dataTensor.get(r, c);
		return 0.0;
	}

	public void set(Object v) {
		if (_basic) {
			if (_basicTensor == null)
				_basicTensor = new BasicTensorBlock(DEFAULT_VTYPE, _dims, false);
			_basicTensor.set(v);
		}
		else {
			if (_dataTensor == null)
				_dataTensor = new DataTensorBlock(getSchema(), _dims);
			_dataTensor.set(v);
		}
	}

	public void set(MatrixBlock other) {
		if (_basic)
			_basicTensor.set(other);
		else
			throw new DMLRuntimeException("TensorBlock.set(MatrixBlock) is not yet implemented for heterogeneous tensors");
	}

	public void set(int[] ix, Object v) {
		if (_basic) {
			if (_basicTensor == null)
				_basicTensor = new BasicTensorBlock(DEFAULT_VTYPE, _dims, false);
			_basicTensor.set(ix, v);
		}
		else {
			if (_dataTensor == null)
				_dataTensor = new DataTensorBlock(getSchema(), _dims);
			_dataTensor.set(ix, v);
		}
	}

	public void set(int r, int c, double v) {
		if (_basic) {
			if (_basicTensor == null)
				_basicTensor = new BasicTensorBlock(DEFAULT_VTYPE, _dims, false);
			_basicTensor.set(r, c, v);
		}
		else {
			if (_dataTensor == null)
				_dataTensor = new DataTensorBlock(getSchema(), _dims);
			_dataTensor.set(r, c, v);
		}
	}

	/**
	 * Slice the current block and write into the outBlock. The offsets determines where the slice starts,
	 * the length of the blocks is given by the outBlock dimensions.
	 *
	 * @param offsets  offsets where the slice starts
	 * @param outBlock sliced result block
	 * @return the sliced result block
	 */
	public TensorBlock slice(int[] offsets, TensorBlock outBlock) {
		// TODO perf
		int[] srcIx = offsets.clone();
		int[] destIx = new int[offsets.length];
		for (int l = 0; l < outBlock.getLength(); l++) {
			outBlock.set(destIx, get(srcIx));
			int i = outBlock.getNumDims() - 1;
			destIx[i]++;
			srcIx[i]++;
			//calculating next index
			while (destIx[i] == outBlock.getDim(i)) {
				destIx[i] = 0;
				srcIx[i] = offsets[i];
				i--;
				if (i < 0) {
					//we are finished
					return outBlock;
				}
				destIx[i]++;
				srcIx[i]++;
			}
		}
		return outBlock;
	}

	public TensorBlock copy(TensorBlock src) {
		_dims = src._dims.clone();
		_basic = src._basic;
		if (_basic) {
			_dataTensor = null;
			_basicTensor = src._basicTensor == null ? null : new BasicTensorBlock(src._basicTensor);
		}
		else {
			_basicTensor = null;
			_dataTensor = src._dataTensor == null ? null : new DataTensorBlock(src._dataTensor);
		}
		return this;
	}

	public TensorBlock copy(int[] lower, int[] upper, TensorBlock src) {
		if (_basic) {
			if (src._basic) {
				_basicTensor.copy(lower, upper, src._basicTensor);
			}
			else {
				throw new DMLRuntimeException("Copying `DataTensor` into `BasicTensor` is not a safe operation.");
			}
		}
		else {
			if (src._basic) {
				// TODO perf
				_dataTensor.copy(lower, upper, new DataTensorBlock(src._basicTensor));
			}
			else {
				_dataTensor.copy(lower, upper, src._dataTensor);
			}
		}
		return this;
	}

	// `getExactSerializedSize()`, `write(DataOutput)` and `readFields(DataInput)` have to match in their serialized
	// form definition
	@Override
	public long getExactSerializedSize() {
		// header size (_basic, _dims.length + _dims[*], type)
		long size = 1 + 4 * (1 + _dims.length) + 1;
		if (isAllocated()) {
			if (_basic) {
				size += 1 + getExactBlockDataSerializedSize(_basicTensor);
			}
			else {
				size += _dataTensor._schema.length;
				for (BasicTensorBlock bt : _dataTensor._colsdata) {
					if (bt != null)
						size += getExactBlockDataSerializedSize(bt);
				}
			}
		}
		return size;
	}

	public long getExactBlockDataSerializedSize(BasicTensorBlock bt) {
		// nnz, BlockType
		long size = 8 + 1;
		if (!bt.isSparse()) {
			switch (bt._vt) {
				case INT32:
				case FP32:
					size += 4 * getLength(); break;
				case INT64:
				case FP64:
					size += 8 * getLength(); break;
				case BOOLEAN:
					//TODO perf bits instead of bytes
					size += getLength(); break;
					//size += Math.ceil((double)getLength() / 64); break;
				case STRING:
					int[] ix = new int[bt._dims.length];
					for (int i = 0; i < bt.getLength(); i++) {
						String s = (String) bt.get(ix);
						size += IOUtilFunctions.getUTFSize(s == null ? "" : s);
						getNextIndexes(bt.getDims(), ix);
					}
					break;
				case UNKNOWN:
					throw new NotImplementedException();
			}
		}
		else {
			throw new NotImplementedException();
		}
		return size;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//step 1: write header (_basic, dims length, dims)
		out.writeBoolean(_basic);
		out.writeInt(_dims.length);
		for (int dim : _dims)
			out.writeInt(dim);

		//step 2: write block type
		//step 3: if tensor allocated write its data
		if (!isAllocated())
			out.writeByte(SERIALIZED_TYPES.EMPTY.ordinal());
		else if (_basic) {
			out.writeByte(SERIALIZED_TYPES.BASIC.ordinal());
			out.writeByte(_basicTensor.getValueType().ordinal());
			writeBlockData(out, _basicTensor);
		}
		else {
			out.writeByte(SERIALIZED_TYPES.DATA.ordinal());
			//write schema and colIndexes
			for (int i = 0; i < getDim(1); i++)
				out.writeByte(_dataTensor._schema[i].ordinal());
			for (BasicTensorBlock bt : _dataTensor._colsdata) {
				//present flag
				if (bt != null)
					writeBlockData(out, bt);
			}
		}
	}

	public void writeBlockData(DataOutput out, BasicTensorBlock bt) throws IOException {
		out.writeLong(bt.getNonZeros()); // nnz
		if (bt.isEmpty(false)) {
			//empty blocks do not need to materialize row information
			out.writeByte(BlockType.EMPTY_BLOCK.ordinal());
		}
		else if (!bt.isSparse()) {
			out.writeByte(BlockType.DENSE_BLOCK.ordinal());
			DenseBlock a = bt.getDenseBlock();
			int odims = (int) UtilFunctions.prod(bt._dims, 1);
			int[] ix = new int[bt._dims.length];
			for (int i = 0; i < bt._dims[0]; i++) {
				ix[0] = i;
				for (int j = 0; j < odims; j++) {
					ix[ix.length - 1] = j;
					switch (bt._vt) {
						case FP32: out.writeFloat((float) a.get(i, j)); break;
						case FP64: out.writeDouble(a.get(i, j)); break;
						case INT32: out.writeInt((int) a.getLong(ix)); break;
						case INT64: out.writeLong(a.getLong(ix)); break;
						case BOOLEAN: out.writeBoolean(a.get(i, j) != 0); break;
						case STRING:
							String s = a.getString(ix);
							out.writeUTF(s == null ? "" : s);
							break;
					}
				}
			}
		}
		else {
			throw new NotImplementedException();
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		//step 1: read header (_basic, dims length, dims)
		_basic = in.readBoolean();
		_dims = new int[in.readInt()];
		for (int i = 0; i < _dims.length; i++)
			_dims[i] = in.readInt();

		//step 2: read block type
		//step 3: if tensor allocated read its data
		switch (SERIALIZED_TYPES.values()[in.readByte()]) {
			case EMPTY:
				break;
			case BASIC:
				_basicTensor = new BasicTensorBlock(ValueType.values()[in.readByte()], _dims);
				readBlockData(in, _basicTensor);
				break;
			case DATA:
				//read schema and colIndexes
				ValueType[] schema = new ValueType[getDim(1)];
				for (int i = 0; i < getDim(1); i++)
					schema[i] = ValueType.values()[in.readByte()];
				_dataTensor = new DataTensorBlock(schema, _dims);
				for (int i = 0; i < _dataTensor._colsdata.length; i++) {
					//present flag
					if (_dataTensor._colsdata[i] != null)
						readBlockData(in, _dataTensor._colsdata[i]);
				}
				break;
		}
	}

	protected void readBlockData(DataInput in, BasicTensorBlock bt) throws IOException {
		bt._nnz = in.readLong();
		switch (BlockType.values()[in.readByte()]) {
			case EMPTY_BLOCK:
				reset(bt._dims);
				return;
			case DENSE_BLOCK: {
				bt.allocateDenseBlock(false);
				DenseBlock a = bt.getDenseBlock();
				int odims = (int) UtilFunctions.prod(bt._dims, 1);
				int[] ix = new int[bt._dims.length];
				for (int i = 0; i < bt._dims[0]; i++) {
					ix[0] = i;
					for (int j = 0; j < odims; j++) {
						ix[ix.length - 1] = j;
						switch (bt._vt) {
							case FP32:
								a.set(i, j, in.readFloat());
								break;
							case FP64:
								a.set(i, j, in.readDouble());
								break;
							case INT32:
								a.set(ix, in.readInt());
								break;
							case INT64:
								a.set(ix, in.readLong());
								break;
							case BOOLEAN:
								a.set(i, j, in.readByte());
								break;
							case STRING:
								// FIXME readUTF is not supported for CacheDataInput
								a.set(ix, in.readUTF());
								break;
						}
					}
				}
				break;
			}
			case SPARSE_BLOCK:
			case ULTRA_SPARSE_BLOCK:
				throw new NotImplementedException();
		}
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		write(out);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException {
		readFields(in);
	}
}
