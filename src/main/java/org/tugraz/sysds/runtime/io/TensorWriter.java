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

import org.tugraz.sysds.runtime.data.TensorBlock;

import java.io.IOException;

public abstract class TensorWriter {
	protected static final String BASIC_TENSOR_IDENTIFIER = "BASIC";
	protected static final String DATA_TENSOR_IDENTIFIER = "DATA";

	public abstract void writeTensorToHDFS(TensorBlock src, String fname, int[] dims, int[] blen) throws IOException;
}
