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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.tugraz.sysds.conf.ConfigurationManager;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.data.BasicTensor;
import org.tugraz.sysds.runtime.data.TensorBlock;
import org.tugraz.sysds.runtime.data.TensorIndexes;

import java.io.IOException;
import java.util.Arrays;

public class TensorReaderBinaryBlock extends TensorReader {
	@Override
	public TensorBlock readTensorFromHDFS(String fname, long[] dims,
			int[] blen) throws IOException, DMLRuntimeException {
		//prepare file access
		JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());
		Path path = new Path(fname);
		FileSystem fs = IOUtilFunctions.getFileSystem(path, job);

		//check existence and non-empty file
		checkValidInputFile(fs, path);

		//core read
		return readBinaryBlockTensorFromHDFS(path, job, fs, dims, blen);
	}

	private TensorBlock readBinaryBlockTensorFromHDFS(Path path, JobConf job, FileSystem fs, long[] dims,
			int[] blen) throws IOException {
		// TODO DataTensor
		BasicTensor ret = null;
		TensorIndexes key = new TensorIndexes();
		BasicTensor value = new BasicTensor();
		int[] idims = Arrays.stream(dims).mapToInt(i -> (int) i).toArray();

		for (Path lpath : IOUtilFunctions.getSequenceFilePaths(fs, path)) {
			SequenceFile.Reader reader = new SequenceFile.Reader(job, SequenceFile.Reader.file(lpath));

			try {
				while (reader.next(key, value)) {
					if (ret == null) {
						ret = new BasicTensor(value.getValueType(), idims, false);
						ret.allocateBlock();
					}
					if (value.isEmpty(false))
						continue;

					int[] lower = new int[blen.length];
					int[] upper = new int[lower.length];
					for (int i = 0; i < blen.length; i++) {
						lower[i] = (int) (key.getIndex(i) - 1) * blen[i];
						upper[i] = lower[i] + value.getDim(i) - 1;
					}
					upper[upper.length - 1]++;
					for (int i = upper.length - 1; i > 0; i--) {
						if (upper[i] == dims[i]) {
							upper[i] = 0;
							upper[i - 1]++;
						}
						else
							break;
					}

					ret.copy(lower, upper, value);
				}
			}
			finally {
				IOUtilFunctions.closeSilently(reader);
			}
		}
		return ret;
	}
}
