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
import org.tugraz.sysds.runtime.data.BasicTensor;
import org.tugraz.sysds.runtime.data.TensorBlock;
import org.tugraz.sysds.runtime.data.TensorIndexes;
import org.tugraz.sysds.runtime.matrix.mapred.MRJobConfiguration;
import org.tugraz.sysds.runtime.util.HDFSTool;

import java.io.IOException;

public class TensorWriterBinaryBlock extends TensorWriter {
	//TODO replication

	@Override
	public void writeTensorToHDFS(TensorBlock src, String fname, int[] dims, int[] blen) throws IOException {
		//prepare file access
		JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());
		Path path = new Path(fname);
		FileSystem fs = IOUtilFunctions.getFileSystem(path, job);

		//if the file already exists on HDFS, remove it.
		HDFSTool.deleteFileIfExistOnHDFS(fname);

		//set up preferred custom serialization framework for binary block format
		if (MRJobConfiguration.USE_BINARYBLOCK_SERIALIZATION)
			MRJobConfiguration.addBinaryBlockSerializationFramework(job);

		//core write sequential
		writeBinaryBlockMatrixToHDFS(path, job, fs, src, dims, blen);

		IOUtilFunctions.deleteCrcFilesFromLocalFileSystem(fs, path);
	}

	private void writeBinaryBlockMatrixToHDFS(Path path, JobConf job, FileSystem fs, TensorBlock src, int[] dims,
			int[] blen) throws IOException {
		//TODO DataTensor
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, job, path, TensorIndexes.class, BasicTensor.class);

		try {
			// bound check
			for (int i = 0; i < dims.length; i++) {
				if (src.getDim(i) > dims[i])
					throw new IOException("TensorBlock dimension " + i + " range [1:" + src.getDim(i) +
							"] out of range [1:" + dims[i] + "].");
			}
			long numBlocks = 1;
			for (int i = 0; i < blen.length; i++) {
				numBlocks *= Math.max((long) Math.ceil((double) dims[i] / blen[i]), 1);
			}

			BasicTensor bt = (BasicTensor) src;
			for (int i = 0; i < numBlocks; i++) {
				int[] offsets = new int[dims.length];
				long blockIndex = i;
				long[] tix = new long[blen.length];
				int[] blockDims = new int[dims.length];
				for (int j = blen.length - 1; j >= 0; j--) {
					tix[j] = 1 + (blockIndex % blen[j]);
					blockIndex /= blen[j];
					offsets[j] = ((int) tix[j] - 1) * blen[j];
					blockDims[j] = (tix[j] * blen[j] < src.getDim(j)) ? blen[j] : src.getDim(j) - offsets[j];
				}
				TensorIndexes indx = new TensorIndexes(tix);
				BasicTensor block = new BasicTensor(bt.getValueType(), blockDims, false);

				//copy submatrix to block
				bt.slice(offsets, block);

				writer.append(indx, block);
			}
		}
		finally {
			IOUtilFunctions.closeSilently(writer);
		}
	}
}
