/*
 * Modifications Copyright 2019 Graz University of Technology
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.tugraz.sysds.runtime.data;

import org.tugraz.sysds.runtime.util.UtilFunctions;

import java.util.Arrays;

public class DenseBlockLFP64 extends DenseBlockLDRB
{
	private static final long serialVersionUID = -1723319832162080273L;

	private double[][] _blocks;

	public DenseBlockLFP64(int[] dims) {
		super(dims);
		reset(_rlen, _odims, 0);
	}

	@Override
	protected void createBlocks(int numBlocks) {
		_blocks = new double[numBlocks][];
	}

	@Override
	protected void createBlock(int bix, int length) {
		_blocks[bix] = new double[length];
	}

	@Override
	protected void setInternal(int bix, int ix, double v) {
		_blocks[bix][ix] = v;
	}

	@Override
	public boolean isNumeric() {
		return true;
	}

	@Override
	public boolean isContiguous() {
		return _blocks.length == 1;
	}

	@Override
	public int numBlocks() {
		return _blocks.length;
	}

	@Override
	public long capacity() {
		return (_blocks!=null) ? (long)(_blocks.length - 1) * _blocks[0].length + _blocks[_blocks.length - 1].length : -1;
	}

	@Override
	protected long computeNnz(int bix, int start, int length) {
		return UtilFunctions.computeNnz(_blocks[bix], start, length);
	}

	@Override
	public double[] values(int r) {
		return valuesAt(index(r));
	}
	
	@Override
	public double[] valuesAt(int bix) {
		return _blocks[bix];
	}

	@Override
	public void incr(int r, int c) {
		incr(r, c, 1);
	}

	@Override
	public void incr(int r, int c, double delta) {
		_blocks[index(r)][pos(r, c)] += delta;
	}

	@Override
	protected void fillBlock(int bix, int fromIndex, int toIndex, double v) {
		Arrays.fill(_blocks[bix], fromIndex,toIndex, v);
	}

	@Override
	public DenseBlock set(int r, int c, double v) {
		_blocks[index(r)][pos(r, c)] = v;
		return this;
	}

	@Override
	public DenseBlock set(int[] ix, double v) {
		_blocks[index(ix[0])][pos(ix)] = v;
		return this;
	}

	@Override
	public DenseBlock set(int[] ix, String v) {
		_blocks[index(ix[0])][pos(ix)] = Double.parseDouble(v);
		return this;
	}

	@Override
	public double get(int r, int c) {
		return _blocks[index(r)][pos(r, c)];
	}

	@Override
	public double get(int[] ix) {
		return _blocks[index(ix[0])][pos(ix)];
	}

	@Override
	public String getString(int[] ix) {
		return String.valueOf(_blocks[index(ix[0])][pos(ix)]);
	}
}
