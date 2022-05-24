package org.dbsp.sqlCompiler.compiler;

import org.dbsp.sqlCompiler.dbsp.circuit.DBSPCircuit;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.DBSPOperator;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.DBSPSinkOperator;
import org.dbsp.sqlCompiler.dbsp.circuit.operator.DBSPSourceOperator;
import org.dbsp.sqlCompiler.dbsp.circuit.type.DBSPTypeInteger;
import org.dbsp.sqlCompiler.dbsp.circuit.type.DBSPType;
import org.dbsp.sqlCompiler.dbsp.circuit.type.DBSPZSetType;
import org.junit.Assert;
import org.junit.Test;

public class IRTests {
    @Test
    public void irTest() {
        DBSPCircuit circuit = new DBSPCircuit(null, "test_scalar");
        DBSPType type = DBSPTypeInteger.signed32;
        DBSPOperator input = new DBSPSourceOperator(null, type, "i");
        circuit.addOperator(input);
        DBSPOperator op = new DBSPOperator(null, "apply", "|x| x + 1", type, "op");
        op.addInput(input);
        circuit.addOperator(op);
        DBSPOperator output = new DBSPSinkOperator(null, type, "o");
        output.addInput(op);
        circuit.addOperator(output);
        String str = circuit.toString();
        Assert.assertNotNull(str);
        System.out.println(str);
    }

    @Test
    public void setTest() {
        DBSPCircuit circuit = new DBSPCircuit(null, "test_zset");
        DBSPType type = new DBSPZSetType(null, DBSPTypeInteger.signed32, DBSPTypeInteger.signed64);
        DBSPOperator input = new DBSPSourceOperator(null, type, "i");
        circuit.addOperator(input);
        DBSPOperator op = new DBSPOperator(null, "apply", "|x| x.add_by_ref(&x)", type, "op");
        op.addInput(input);
        circuit.addOperator(op);
        DBSPOperator output = new DBSPSinkOperator(null, type, "o");
        output.addInput(op);
        circuit.addOperator(output);
        String str = circuit.toString();
        Assert.assertNotNull(str);
        System.out.println(str);
    }
}
