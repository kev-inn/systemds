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

package org.tugraz.sysds.runtime.instructions;

import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.instructions.fed.FEDInstruction;
import org.tugraz.sysds.runtime.instructions.fed.FEDInstruction.FEDType;
import org.tugraz.sysds.runtime.instructions.fed.ReadFEDInstruction;

import java.util.HashMap;

public class FEDInstructionParser extends InstructionParser
{
	public static final HashMap<String, FEDType> String2FEDInstructionType;
	static {
		String2FEDInstructionType = new HashMap<>();
		String2FEDInstructionType.put("fedread", FEDType.Read);
	}

	public static FEDInstruction parseSingleInstruction (String str ) {
		if ( str == null || str.isEmpty() )
			return null;
		FEDType fedtype = InstructionUtils.getFEDType(str);
		if ( fedtype == null )
			throw new DMLRuntimeException("Unable derive fedtype for instruction: " + str);
		FEDInstruction cpinst = parseSingleInstruction(fedtype, str);
		if ( cpinst == null )
			throw new DMLRuntimeException("Unable to parse instruction: " + str);
		return cpinst;
	}
	
	public static FEDInstruction parseSingleInstruction ( FEDType fedtype, String str ) {
		if ( str == null || str.isEmpty() )
			return null;
		switch(fedtype) {
			case Read:
				return ReadFEDInstruction.parseInstruction(str);
			default:
				throw new DMLRuntimeException("Invalid FEDERATED Instruction Type: " + fedtype );
		}
	}
}
