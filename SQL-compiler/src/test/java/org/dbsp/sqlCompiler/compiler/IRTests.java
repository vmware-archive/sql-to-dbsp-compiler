package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.dbsp.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.dbsp.circuit.expression.*;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.DBSPApplyOperator;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.DBSPSourceOperator;
import org.dbsp.sqlCompiler.dbsp.circuit.type.DBSPTypeInteger;
import org.dbsp.sqlCompiler.dbsp.circuit.type.DBSPType;
import org.dbsp.sqlCompiler.dbsp.circuit.type.DBSPTypeZSet;
import org.junit.Assert;
import org.junit.Test;

public class IRTests {
    @Test
    public void irTest() {
        DBSPCircuit circuit = new DBSPCircuit(null, "test_scalar");
        DBSPType type = DBSPTypeInteger.signed32;
        DBSPOperator input = new DBSPSourceOperator(null, type, "i");
        circuit.addOperator(input);
        String varName = "x";
        DBSPOperator op = new DBSPApplyOperator(null,
                new DBSPClosureExpression(null, varName, type,
                        new DBSPBinaryExpression(null, type, "+",
                                new DBSPVariableReference(varName, type),
                                new DBSPLiteral(1))), type, input);
        circuit.addOperator(op);
        DBSPOperator output = new DBSPSinkOperator(null, type, "o", op);
        circuit.addOperator(output);
        String str = circuit.toString();
        Assert.assertNotNull(str);
        System.out.println(str);
    }

    @Test
    public void setTest() {
        DBSPCircuit circuit = new DBSPCircuit(null, "test_zset");
        DBSPType type = new DBSPTypeZSet(null, DBSPTypeInteger.signed32, DBSPTypeInteger.signed64);
        DBSPOperator input = new DBSPSourceOperator(null, type, "i");
        circuit.addOperator(input);
        String x = "x";
        DBSPExpression function = new DBSPClosureExpression(null, x, type,
                new DBSPApplyExpression("add_by_ref", type,
                        new DBSPVariableReference(x, type),
                        new DBSPDerefExpression(new DBSPVariableReference(x, type))));
        DBSPOperator op = new DBSPApplyOperator(null, function, type, input);
        circuit.addOperator(op);
        DBSPOperator output = new DBSPSinkOperator(null, type, "o", op);
        circuit.addOperator(output);
        String str = circuit.toString();
        Assert.assertNotNull(str);
        System.out.println(str);
    }
}
