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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.conf.ConfigurationManager;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.data.BasicTensor;
import org.tugraz.sysds.runtime.data.DataTensor;
import org.tugraz.sysds.runtime.data.TensorBlock;
import org.tugraz.sysds.runtime.util.FastStringTokenizer;

import java.io.IOException;

public class TensorReaderTextCell extends TensorReader {

	@Override
	public TensorBlock readTensorFromHDFS(String fname, int[] dims,
			int[] blen) throws IOException, DMLRuntimeException {
		//prepare file access
		JobConf job = new JobConf(ConfigurationManager.getCachedJobConf());
		Path path = new Path(fname);
		FileSystem fs = IOUtilFunctions.getFileSystem(path, job);

		//check existence and non-empty file
		checkValidInputFile(fs, path);

		//allocate output matrix block
		return readTextCellTensorFromHDFS(path, job, dims);
	}

	protected TensorBlock readTextCellTensorFromHDFS(Path path, JobConf job, int[] dims) throws IOException {
		FileInputFormat.addInputPath(job, path);
		TextInputFormat informat = new TextInputFormat();
		informat.configure(job);
		InputSplit[] splits = informat.getSplits(job, 1);

		LongWritable key = new LongWritable();
		Text value = new Text();
		TensorBlock ret = null;

		try {
			FastStringTokenizer st = new FastStringTokenizer(' ');

			for (InputSplit split : splits) {
				RecordReader<LongWritable, Text> reader = informat.getRecordReader(split, job, Reporter.NULL);
				try {
					reader.next(key, value);
					st.reset(value.toString());
					String tensorId = st.nextToken();
					if (tensorId.equals(TensorWriter.BASIC_TENSOR_IDENTIFIER)) {
						Types.ValueType vt = Types.ValueType.valueOf(st.nextToken());
						ret = new BasicTensor(vt, dims, false);
					}
					else if (tensorId.equals(TensorWriter.DATA_TENSOR_IDENTIFIER)) {
						int cols = dims[1];
						Types.ValueType[] schema = new Types.ValueType[cols];
						for (int i = 0; i < cols; i++)
							schema[i] = Types.ValueType.valueOf(st.nextToken());
						ret = new DataTensor(schema, dims);
					}

					int[] ix = new int[dims.length];
					while (reader.next(key, value)) {
						st.reset(value.toString());
						for (int i = 0; i < ix.length; i++) {
							ix[i] = st.nextInt() - 1;
						}
						ret.set(ix, st.nextToken());
					}
				} finally {
					IOUtilFunctions.closeSilently(reader);
				}
			}
		}
		catch (Exception ex) {
			throw new IOException("Unable to read tensor in text cell format.", ex);
		}
		return ret;
	}
}
