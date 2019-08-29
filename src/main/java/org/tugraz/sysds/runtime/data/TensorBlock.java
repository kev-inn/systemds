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

public class TensorBlock implements CacheBlock, Externalizable {
	private static final long serialVersionUID = -8768054067319422277L;

	public static final int[] DEFAULT_DIMS = new int[]{0, 0};
	public static final ValueType DEFAULT_VTYPE = ValueType.FP64;

	private int[] _dims;
	private boolean _heterogeneous;

	private DataTensorBlock _dataTensor = null;
	private BasicTensorBlock _basicTensor = null;

	public TensorBlock() {
		this(DEFAULT_DIMS, false);
	}

	public TensorBlock(int[] dims, boolean heterogeneous) {
		_dims = dims;
		_heterogeneous = heterogeneous;
	}

	public TensorBlock(int[] dims, ValueType vt) {
		this(dims, false);
		_basicTensor = new BasicTensorBlock(vt, dims, false);
	}

	public TensorBlock(int[] dims, ValueType[] schema) {
		this(dims, true);
		_dataTensor = new DataTensorBlock(schema, dims);
	}

	public TensorBlock(double value) {
		_dims = new int[]{1, 1};
		_heterogeneous = false;
		_basicTensor = new BasicTensorBlock(value);
	}

	public TensorBlock(BasicTensorBlock basicTensor) {
		this(basicTensor._dims, false);
		_basicTensor = basicTensor;
	}

	public TensorBlock(DataTensorBlock dataTensor) {
		this(dataTensor._dims, true);
		_dataTensor = dataTensor;
	}

	public TensorBlock(TensorBlock that) {
		copy(that);
	}

	public void reset() {
		if (_heterogeneous) {
			if (_dataTensor == null)
				_dataTensor = new DataTensorBlock(DEFAULT_VTYPE, _dims);
			_dataTensor.reset();
		}
		else {
			if (_basicTensor == null)
				_basicTensor = new BasicTensorBlock(DEFAULT_VTYPE, _dims);
			_basicTensor.reset();
		}
	}

	public void reset(int[] dims) {
		_dims = dims;
		if (_heterogeneous) {
			if (_dataTensor == null)
				_dataTensor = new DataTensorBlock(DEFAULT_VTYPE, _dims);
			_dataTensor.reset(dims);
		}
		else {
			if (_basicTensor == null)
				_basicTensor = new BasicTensorBlock(DEFAULT_VTYPE, _dims);
			_basicTensor.reset(dims);
		}
	}

	public boolean isHeterogeneous() {
		return _heterogeneous;
	}

	public boolean isAllocated() {
		if (_heterogeneous)
			return _dataTensor != null && _dataTensor.isAllocated();
		else
			return _basicTensor != null && _basicTensor.isAllocated();
	}

	public TensorBlock allocateBlock() {
		if (_heterogeneous) {
			if (_dataTensor == null)
				_dataTensor = new DataTensorBlock(DEFAULT_VTYPE, _dims);
			_dataTensor.allocateBlock();
		}
		else {
			if (_basicTensor == null)
				_basicTensor = new BasicTensorBlock(DEFAULT_VTYPE, _dims);
			_basicTensor.allocateBlock();
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
		if (!_heterogeneous)
			return _basicTensor == null ? DEFAULT_VTYPE : _basicTensor.getValueType();
		else
			return null;
	}

	public ValueType[] getSchema() {
		if (_heterogeneous) {
			//TODO should we return array with col DEFAULT_VTYPE elements instead
			return _dataTensor == null ? new ValueType[0] : _dataTensor.getSchema();
		}
		else
			return null;
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
		if (_heterogeneous)
			return _dataTensor == null || _dataTensor.isEmpty(safe);
		else
			return _basicTensor == null || _basicTensor.isEmpty(safe);
	}

	public long getNonZeros() {
		if (!isAllocated())
			return 0;
		if (_heterogeneous)
			return _dataTensor.getNonZeros();
		else
			return _basicTensor.getNonZeros();
	}

	public Object get(int[] ix) {
		if (_heterogeneous)
			return _dataTensor.get(ix);
		else
			return _basicTensor.get(ix);
	}

	public double get(int r, int c) {
		if (_heterogeneous)
			return _dataTensor.get(r, c);
		else
			return _basicTensor.get(r, c);
	}

	public void set(Object v) {
		if (_heterogeneous)
			_dataTensor.set(v);
		else
			_basicTensor.set(v);
	}

	public void set(MatrixBlock other) {
		if (_heterogeneous)
			throw new DMLRuntimeException("TensorBlock.set(MatrixBlock) is not yet implemented for heterogeneous tensors");
		else
			_basicTensor.set(other);
	}

	public void set(int[] ix, Object v) {
		if (_heterogeneous)
			_dataTensor.set(ix, v);
		else
			_basicTensor.set(ix, v);
	}

	public void set(int r, int c, double v) {
		if (_heterogeneous)
			_dataTensor.set(r, c, v);
		else
			_basicTensor.set(r, c, v);
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
		_heterogeneous = src._heterogeneous;
		if (_heterogeneous) {
			_basicTensor = null;
			_dataTensor = src._dataTensor == null ? null : new DataTensorBlock(src._dataTensor);
		}
		else {
			_dataTensor = null;
			_basicTensor = src._basicTensor == null ? null : new BasicTensorBlock(src._basicTensor);
		}
		return this;
	}

	public TensorBlock copy(int[] lower, int[] upper, TensorBlock src) {
		if (_heterogeneous) {
			if (src._heterogeneous) {
				_dataTensor.copy(lower, upper, src._dataTensor);
			}
			else {
				// TODO perf
				_dataTensor.copy(lower, upper, new DataTensorBlock(src._basicTensor));
			}
		}
		else {
			if (src._heterogeneous) {
				throw new DMLRuntimeException("Copying `DataTensor` into `BasicTensor` is not a safe operation.");
			}
			else {
				_basicTensor.copy(lower, upper, src._basicTensor);
			}
		}
		return this;
	}

	// `getExactSerializedSize()`, `write(DataOutput)` and `readFields(DataInput)` have to match in their serialized
	// form definition
	@Override
	public long getExactSerializedSize() {
		// header size (_isDataTensor, _dims.length + _dims[*], isAllocated)
		long size = 1 + 4 * (1 + _dims.length) + 1;
		if (isAllocated()) {
			if (_heterogeneous) {
				size += _dataTensor._schema.length;
				for (BasicTensorBlock bt : _dataTensor._colsdata) {
					if (bt != null)
						size += getExactBlockDataSerializedSize(bt);
				}
			}
			else {
				size += 1 + getExactBlockDataSerializedSize(_basicTensor);
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
		//step 1: write header (isDataTensor, dims length, dims)
		out.writeBoolean(_heterogeneous);
		out.writeInt(_dims.length);
		for (int dim : _dims)
			out.writeInt(dim);

		//step 2: write flag if tensor is allocated or not
		if (!isAllocated())
			out.writeBoolean(false);
		else {
			//step 3: if tensor allocated write its data
			out.writeBoolean(true);
			if (_heterogeneous) {
				//write schema and colIndexes
				for (int i = 0; i < getDim(1); i++)
					out.writeByte(_dataTensor._schema[i].ordinal());
				for (BasicTensorBlock bt : _dataTensor._colsdata) {
					//present flag
					if (bt != null)
						writeBlockData(out, bt);
				}
			}
			else {
				out.writeByte(_basicTensor.getValueType().ordinal());
				writeBlockData(out, _basicTensor);
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
						case FP32:
							out.writeFloat((float) a.get(i, j));
							break;
						case FP64:
							out.writeDouble(a.get(i, j));
							break;
						case INT32:
							out.writeInt((int) a.getLong(ix));
							break;
						case INT64:
							out.writeLong(a.getLong(ix));
							break;
						case BOOLEAN:
							out.writeBoolean(a.get(i, j) != 0);
							break;
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
		//step 1: read header (isDataTensor, dims length, dims)
		_heterogeneous = in.readBoolean();
		_dims = new int[in.readInt()];
		for (int i = 0; i < _dims.length; i++)
			_dims[i] = in.readInt();

		//step 2: read flag if tensor is allocated or not
		if (in.readBoolean()) {
			//step 3: if tensor allocated read its data
			if (_heterogeneous) {
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
			}
			else {
				_basicTensor = new BasicTensorBlock(ValueType.values()[in.readByte()], _dims);
				readBlockData(in, _basicTensor);
			}
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
