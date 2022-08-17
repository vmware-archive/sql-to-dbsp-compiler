package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.dbsp.TypeCompiler;
import org.dbsp.sqlCompiler.dbsp.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.DBSPApplyOperator;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.DBSPSourceOperator;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPTypeInteger;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPType;
import org.dbsp.sqlCompiler.dbsp.rust.type.DBSPTypeZSet;
import org.dbsp.sqlCompiler.dbsp.rust.expression.*;
import org.junit.Assert;
import org.junit.Test;

public class IRTests {
    @Test
    public void irTest() {
        DBSPCircuit circuit = new DBSPCircuit(null, "test_scalar", "handbuilt");
        DBSPType type = DBSPTypeInteger.signed32;
        DBSPOperator input = new DBSPSourceOperator(null, type, "i");
        circuit.addOperator(input);
        String varName = "x";
        DBSPOperator op = new DBSPApplyOperator(null,
                new DBSPClosureExpression(null, new DBSPBinaryExpression(null, type, "+",
                        new DBSPVariableReference(varName, type),
                        new DBSPLiteral(1)), varName
                ), TypeCompiler.makeZSet(type), input);
        circuit.addOperator(op);
        DBSPOperator output = new DBSPSinkOperator(null, type, "o", op);
        circuit.addOperator(output);
        String str = circuit.toString();
        Assert.assertNotNull(str);
        System.out.println(str);
    }

    @Test
    public void setTest() {
        DBSPCircuit circuit = new DBSPCircuit(null, "test_zset", "handbuilt");
        DBSPType type = new DBSPTypeZSet(null, DBSPTypeInteger.signed32, DBSPTypeInteger.signed64);
        DBSPOperator input = new DBSPSourceOperator(null, type, "i");
        circuit.addOperator(input);
        String x = "x";
        DBSPExpression function = new DBSPClosureExpression(null, new DBSPApplyExpression("add_by_ref", type,
                new DBSPVariableReference(x, type),
                new DBSPRefExpression(new DBSPVariableReference(x, type))), x
        );
        DBSPOperator op = new DBSPApplyOperator(null, function, TypeCompiler.makeZSet(type), input);
        circuit.addOperator(op);
        DBSPOperator output = new DBSPSinkOperator(null, type, "o", op);
        circuit.addOperator(output);
        String str = circuit.toString();
        Assert.assertNotNull(str);
        System.out.println(str);
    }
}
