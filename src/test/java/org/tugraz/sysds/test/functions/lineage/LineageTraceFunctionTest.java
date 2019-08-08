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
 */

package org.tugraz.sysds.test.functions.lineage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;
import org.tugraz.sysds.runtime.controlprogram.caching.MatrixObject;
import org.tugraz.sysds.runtime.instructions.cp.Data;
import org.tugraz.sysds.runtime.lineage.Lineage;
import org.tugraz.sysds.runtime.lineage.LineageItem;
import org.tugraz.sysds.runtime.lineage.LineageItemUtils;
import org.tugraz.sysds.runtime.lineage.LineageParser;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestConfiguration;
import org.tugraz.sysds.test.TestUtils;

public class LineageTraceFunctionTest extends AutomatedTestBase
{
	protected static final String TEST_DIR = "functions/lineage/";
	protected static final String TEST_NAME1 = "LineageTraceFun1"; //rand - matrix result
	protected static final String TEST_NAME2 = "LineageTraceFun2"; //rand - matrix result
	
	protected String TEST_CLASS_DIR = TEST_DIR + LineageTraceFunctionTest.class.getSimpleName() + "/";
	
	protected static final int numRecords = 50;
	protected static final int numFeatures = 10;
	
	public LineageTraceFunctionTest() {
		
	}
	
	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration( TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1, new String[] {"R"}) );
		addTestConfiguration( TEST_NAME2, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME2, new String[] {"R"}) );
	}
	
	@Test
	public void testLineageTraceFunction1() {
		testLineageTraceFunction(TEST_NAME1);
	}
	
	@Test
	public void testLineageTraceFunction2() {
		testLineageTraceFunction(TEST_NAME2);
	}
	
	private void testLineageTraceFunction(String testname) {
		System.out.println("------------ BEGIN " + testname + "------------");
		
		getAndLoadTestConfiguration(testname);
		List<String> proArgs = new ArrayList<String>();
		
		proArgs.add("-explain");
		proArgs.add("-lineage");
		proArgs.add("-args");
		proArgs.add(input("X"));
		proArgs.add(output("R"));
		proArgs.add(String.valueOf(numRecords));
		proArgs.add(String.valueOf(numFeatures));
		programArgs = proArgs.toArray(new String[proArgs.size()]);
		fullDMLScriptName = getScript();
		
		//run the test
		Lineage.resetInternalState();
		runTest(true, EXCEPTION_NOT_EXPECTED, null, -1);
		
		//get lineage and generate program
		String Rtrace = readDMLLineageFromHDFS("R");
		LineageItem R = LineageParser.parseLineageTrace(Rtrace);
		Data ret = LineageItemUtils.computeByLineage(R);
		
		HashMap<CellIndex, Double> dmlfile = readDMLMatrixFromHDFS("R");
		MatrixBlock tmp = ((MatrixObject)ret).acquireReadAndRelease();
		TestUtils.compareMatrices(dmlfile, tmp, 1e-6);
	}
}
