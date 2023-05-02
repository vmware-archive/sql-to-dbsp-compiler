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
import org.dbsp.sqlCompiler.compiler.backend.jit.ir.types.JITRowType;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPTupleExpression;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPZSetLiteral;

import java.util.HashMap;
import java.util.Map;

public class JITZSetLiteral extends JITNode {
    public final Map<JITTupleLiteral, Long> elements;
    public final JITRowType rowType;

    public JITZSetLiteral(DBSPZSetLiteral zset, JITRowType type) {
        this.elements = new HashMap<>(zset.size());
        this.rowType = type;
        for (Map.Entry<DBSPExpression, Long> element : zset.data.entrySet()) {
            long weight = element.getValue();
            DBSPTupleExpression elementValue = element.getKey().to(DBSPTupleExpression.class);
            JITTupleLiteral row = new JITTupleLiteral(elementValue);
            this.elements.put(row, weight);
        }
    }

    @Override
    public BaseJsonNode asJson() {
        ObjectNode result = jsonFactory().createObjectNode();
        ObjectNode valueLayout = result.putObject("layout");
        valueLayout.put("Set", this.rowType.getId());
        ArrayNode set = result.putArray("Set");
        for (Map.Entry<JITTupleLiteral, Long> element : this.elements.entrySet()) {
            long weight = element.getValue();
            ArrayNode array = set.addArray();
            ObjectNode rows = array.addObject();
            ArrayNode rowsValue = rows.putArray("rows");
            rowsValue.add(element.getKey().asJson());
            array.add(weight);
        }
        return result;
    }
}