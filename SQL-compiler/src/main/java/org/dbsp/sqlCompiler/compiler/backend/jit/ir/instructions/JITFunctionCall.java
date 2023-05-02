/*
 * Copyright 2023 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqlCompiler.compiler.backend.jit.ir.instructions;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.JITNode;
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITType;
import org.dbsp.util.Linq;

import java.util.List;

public class JITFunctionCall extends JITInstruction {
    public final String functionName;
    public final List<JITInstructionReference> arguments;
    public final List<JITType> argumentTypes;
    public final JITType returnType;

    public JITFunctionCall(long id, String functionName, List<JITInstructionReference> arguments,
                           List<JITType> argumentTypes, JITType returnType) {
        super(id, "Call");
        this.functionName = functionName;
        this.arguments = arguments;
        this.argumentTypes = argumentTypes;
        this.returnType = returnType;
    }

    @Override
    public String toString() {
        List<String> args = Linq.map(this.arguments, a -> Long.toString(a.getId()));
        return this.getId() + " " + this.functionName + "(" + String.join(", ", args) + ")";
    }

    @Override
    public BaseJsonNode instructionAsJson() {
        // { "Call": {
        //    "function": "some.func",
        //    "args": [100, 200],
        //    "arg_types": [{ "Row": 10 }, { "Scalar": "U32" }],
        //    "ret_ty": "I32"
        // }
        ObjectNode result = JITNode.jsonFactory().createObjectNode();
        result.put("function", this.functionName);
        ArrayNode args = result.putArray("args");
        ArrayNode argTypes = result.putArray("arg_types");
        int index = 0;
        for (JITInstructionReference arg: this.arguments) {
            ObjectNode argTypeNode = argTypes.addObject();
            JITType argType = this.argumentTypes.get(index);
            args.add(arg.getId());
            if (argType.isScalarType()) {
                argTypeNode.put("Scalar", argType.toString());
            } else {
                argTypeNode.put("Row", argType.toString());
            }
            index++;
        }
        result.put("ret_ty", this.returnType.toString());
        return result;
    }
}
