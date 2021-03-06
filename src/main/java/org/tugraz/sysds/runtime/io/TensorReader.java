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

package org.tugraz.sysds.runtime.io;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.data.TensorBlock;
import org.tugraz.sysds.runtime.util.HDFSTool;

import java.io.EOFException;
import java.io.IOException;

public abstract class TensorReader {
	public abstract TensorBlock readTensorFromHDFS(String fname, long[] dims, int blen, ValueType[] schema)
			throws IOException, DMLRuntimeException;

	protected static void checkValidInputFile(FileSystem fs, Path path)
			throws IOException {
		//check non-existing file
		if (!fs.exists(path))
			throw new IOException("File " + path.toString() + " does not exist on HDFS/LFS.");

		//check for empty file
		if (HDFSTool.isFileEmpty(fs, path))
			throw new EOFException("Empty input file " + path.toString() + ".");
	}
}
