package org.dbsp.sqlCompiler.compiler.backend;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.dbsp.sqlCompiler.ir.DBSPParameter;
import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.sqlCompiler.ir.expression.*;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPBoolLiteral;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI32Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPI64Literal;
import org.dbsp.sqlCompiler.ir.expression.literal.DBSPLiteral;
import org.dbsp.sqlCompiler.ir.pattern.DBSPIdentifierPattern;
import org.dbsp.sqlCompiler.ir.statement.DBSPLetStatement;
import org.dbsp.sqlCompiler.ir.statement.DBSPStatement;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeTuple;
import org.dbsp.util.Unimplemented;
import org.dbsp.util.Utilities;

import javax.annotation.Nullable;
import java.util.*;

/**
 * Generate code for the JIT compiler using a JSON representation.

 */
public class ToJitInnerVisitor extends InnerVisitor {
    public static boolean isLegalId(int id) {
        return id >= 0;
    }

    /**
     * In the JIT representation tuples can have nullable fields,
     * but scalar variables cannot.  So we represent each scalar
     * variable that has a nullable type as a pair of expressions,
     * one carrying the value, and a second one, Boolean, carrying the nullness.
     */
    static class ExpressionIds {
        /**
         * Expression id.
         */
        public final int id;
        /**
         * Id of the expression carrying the nullability - if any.
         * Otherwise -1.
         */
        public final int isNullId;

        private ExpressionIds(int id, int isNullId) {
            this.id = id;
            this.isNullId = isNullId;
        }

        static ExpressionIds withNull(int id) {
            return new ExpressionIds(id, id + 1);
        }

        static ExpressionIds noNull(int id) {
            return new ExpressionIds(id, -1);
        }

        boolean hasNull() {
            return isLegalId(this.isNullId);
        }
    }

    static class ExpressionRepresentation {
        /**
         * The json instruction that computes the value of the expression.
         */
        public final ObjectNode instruction;
        /**
         * This field is also stored inside the instruction JSON node.
         */
        int instructionId;
        /**
         * The json instruction that computes the value of the null field - if needed.
         * null otherwise.
         */
        public final @Nullable ObjectNode isNullInstruction;

        public ExpressionRepresentation(ObjectNode instruction, int id, @Nullable ObjectNode isNullInstruction) {
            this.instruction = instruction;
            this.instructionId = id;
            this.isNullInstruction = isNullInstruction;
        }

        public ObjectNode getNullObject() {
            return Objects.requireNonNull(this.isNullInstruction);
        }
    }

    /**
     * Contexts keep track of variables defined.
     * Variable names can be redefined, as in Rust, and a context
     * will always return the one in the current scope.
     */
    class Context {
        /**
         * Maps variable names to expression ids.
         */
        final Map<String, ExpressionIds> variables;

        public Context() {
            this.variables = new HashMap<>();
        }

        @Nullable
        ExpressionIds lookup(String varName) {
            if (this.variables.containsKey(varName))
                return this.variables.get(varName);
            return null;
        }

        /**
         * Add a new variable to the current context.
         * @param varName   Variable name.
         * @param needsNull True if the variable is a scalar nullable variable.
         *                  Then we allocate an extra expression to hold its
         *                  nullability.
         */
        ExpressionIds add(String varName, boolean needsNull) {
            if (this.variables.containsKey(varName)) {
                throw new RuntimeException("Duplicate declaration " + varName);
            }
            int id = ToJitInnerVisitor.this.nextExpressionId();
            ExpressionIds ids;
            if (needsNull) {
                ToJitInnerVisitor.this.nextExpressionId();
                ids = ExpressionIds.withNull(id);
            } else {
                ids = ExpressionIds.noNull(id);
            }
            this.variables.put(varName, ids);
            return ids;
        }
    }

    /**
     * Name of a fictitious parameter that is added to closures that return structs.
     * The JIT only supports returning scalar values, so a closure that returns a
     * tuple actually receives an additional parameter that is "output" (equivalent to a "mut").
     */
    private static final String RETURN_PARAMETER_NAME = "$retval";
    /**
     * Used to allocate ids for expressions and blocks.
     */
    private int nextExpressionId = 1;
    private int nextBlockId = 1;
    final ObjectNode blocks;
    /**
     * Map expression to id.
     */
    final Map<DBSPExpression, Integer> expressionId;
    /**
     * A context for each block expression.
     */
    public final List<Context> declarations;
    /**
     * JSON object representing the body of the current block expression.
     */
    @Nullable
    public ArrayNode currentBlockBody;
    /**
     * JSON object representing the current block.
     */
    @Nullable
    public ObjectNode currentBlock;
    /**
     * The type catalog shared with the ToJitVisitor.
     */
    public final ToJitVisitor.TypeCatalog catalog;
    /**
     * The names of the variable currently being assigned.
     * This is a stack, because we can have nested blocks:
     * let var1 = { let v2 = 1 + 2; v2 + 1 }
     */
    final List<String> variableAssigned;

    public ToJitInnerVisitor(ObjectNode blocks, ToJitVisitor.TypeCatalog catalog) {
        super(true);
        this.blocks = blocks;
        this.catalog = catalog;
        this.expressionId = new HashMap<>();
        this.declarations = new ArrayList<>();
        this.currentBlock = null;
        this.currentBlockBody = null;
        this.variableAssigned = new ArrayList<>();
    }

    int nextBlockId() {
        int result = this.nextBlockId;
        this.nextBlockId++;
        return result;
    }

    int nextExpressionId() {
        int result = this.nextExpressionId;
        this.nextExpressionId++;
        return result;
    }

    ExpressionIds resolve(String varName) {
        // Look in the contexts in backwards order
        for (int i = 0; i < this.declarations.size(); i++) {
            int index = this.declarations.size() - 1 - i;
            Context current = this.declarations.get(index);
            ExpressionIds ids = current.lookup(varName);
            if (ids != null)
                return ids;
        }
        throw new RuntimeException("Could not resolve " + varName);
    }

    int map(DBSPExpression expression) {
        return Utilities.putNew(this.expressionId, expression, this.nextExpressionId());
    }

    int getExpressionId(DBSPExpression expression) {
        return Utilities.getExists(this.expressionId, expression);
    }

    ExpressionIds accept(DBSPExpression expression) {
        expression.accept(this);
        int id = this.getExpressionId(expression);
        if (needsNull(expression.getNonVoidType()))
            return ExpressionIds.withNull(id);
        return ExpressionIds.noNull(id);
    }

    static final DBSPBoolLiteral constantFalse = new DBSPBoolLiteral(false);

    ExpressionRepresentation insertInstruction(DBSPExpression expression) {
        int id = this.map(expression);
        ArrayNode instruction = Objects.requireNonNull(this.currentBlockBody).addArray();
        instruction.add(id);
        ObjectNode isNull = null;
        if (needsNull(expression.getNonVoidType())) {
            ArrayNode isNullInstr = this.currentBlockBody.addArray();
            id = this.nextExpressionId();
            isNullInstr.add(id);
            isNull = isNullInstr.addObject();
        }
        return new ExpressionRepresentation(instruction.addObject(), id, isNull);
    }

    static String baseTypeName(DBSPExpression expression) {
        return ToJitVisitor.baseTypeName(expression.getNonVoidType());
    }

    void newContext() {
        this.declarations.add(new Context());
    }

    Context getCurrentContext() {
        return this.declarations.get(this.declarations.size() - 1);
    }

    void popContext() {
        Utilities.removeLast(this.declarations);
    }

    ExpressionIds declare(String var, boolean needsNull) {
        return this.getCurrentContext().add(var, needsNull);
    }

    /**
     * True if this type needs to store a "is_null" value in a separate place.
     * Tuples don't.  Only scalar nullable types do.
     */
    static boolean needsNull(DBSPType type) {
        if (type.is(DBSPTypeTuple.class))
            return false;
        return type.mayBeNull;
    }

    /////////////////////////// Code generation

    boolean createIntegerLiteral(DBSPLiteral expression, String type, @Nullable Long value) {
        ExpressionRepresentation ev = this.insertInstruction(expression);
        if (expression.isNull) {
            Objects.requireNonNull(ev.isNullInstruction);
            ObjectNode n = ev.isNullInstruction.putObject("Constant");
            n.put("is_null", true);
        } else {
            Objects.requireNonNull(value);
            ObjectNode constant = ev.instruction.putObject("Constant");
            constant.put(type, value);
            if (ev.isNullInstruction != null) {
                ObjectNode n = ev.isNullInstruction.putObject("Constant");
                n.put("is_null", false);
            }
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPI32Literal expression) {
        return createIntegerLiteral(expression, "I32",
                expression.value == null ? null : Long.valueOf(expression.value));
    }

    @Override
    public boolean preorder(DBSPI64Literal expression) {
        return createIntegerLiteral(expression, "I64", expression.value);
    }

    @Override
    public boolean preorder(DBSPBoolLiteral expression) {
        ExpressionRepresentation ev = this.insertInstruction(expression);
        if (expression.isNull) {
            Objects.requireNonNull(ev.isNullInstruction);
            ObjectNode n = ev.isNullInstruction.putObject("Constant");
            n.put("is_null", true);
        } else {
            Objects.requireNonNull(expression.value);
            ObjectNode constant = ev.instruction.putObject("Constant");
            constant.put("Bool", expression.value);
            if (ev.isNullInstruction != null) {
                ObjectNode n = ev.isNullInstruction.putObject("Constant");
                n.put("is_null", false);
            }
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPCastExpression expression) {
        // "Cast": {
        //    "value": 2,
        //    "from": "I32",
        //    "to": "I64"
        //  }
        ExpressionIds sourceId = this.accept(expression.source);
        ExpressionRepresentation ev = this.insertInstruction(expression);
        ObjectNode cast = ev.instruction.putObject("Cast");
        cast.put("value", sourceId.id);
        cast.put("from", baseTypeName(expression.source));
        cast.put("to", baseTypeName(expression));

        if (sourceId.hasNull()) {
            ObjectNode isNull = ev.getNullObject().putObject("CopyVal");
            isNull.put("value", sourceId.isNullId);
            isNull.put("value_ty", "Bool");
        }
        return false;
    }

    static final Map<String, String> opNames = new HashMap<>();

    static {
        // https://github.com/vmware/database-stream-processor/blob/dataflow-jit/crates/dataflow-jit/src/ir/expr.rs, the BinaryOpKind enum
        opNames.put("+", "Add");
        opNames.put("-", "Sub");
        opNames.put("*", "Mul");
        opNames.put("/", "Div");
        opNames.put("==", "Eq");
        opNames.put("!=", "Neq");
        opNames.put("<", "LessThan");
        opNames.put(">", "GreaterThan");
        opNames.put("<=", "LessThanOrEqual");
        opNames.put(">=", "GreaterThanOrEqlual");
        opNames.put("&", "And");
        opNames.put("&&", "And");
        opNames.put("|", "Or");
        opNames.put("||", "Or");
        opNames.put("^", "Xor");
    }

    @Override
    public boolean preorder(DBSPBinaryExpression expression) {
        // "BinOp": {
        //   "lhs": 4,
        //   "rhs": 5,
        //   "operand_ty": "I64",
        //   "kind": "GreaterThan"
        // }
        ExpressionIds leftId = this.accept(expression.left);
        ExpressionIds rightId = this.accept(expression.right);
        ExpressionIds cf = this.accept(constantFalse);
        ExpressionRepresentation ev = this.insertInstruction(expression);
        ObjectNode binOp = ev.instruction.putObject("BinOp");
        binOp.put("lhs", leftId.id);
        binOp.put("rhs", rightId.id);
        binOp.put("kind", Utilities.getExists(opNames, expression.operation));
        binOp.put("operand_ty", baseTypeName(expression.left));
        if (ev.isNullInstruction != null) {
            switch (expression.operation) {
                case "&&":
                    // TODO
                    break;
                case "||":
                    // TODO
                    break;
                default: {
                    // The result is null if either operand is null.
                    int leftNullId;
                    if (leftId.hasNull())
                        leftNullId = leftId.isNullId;
                    else
                        // Not nullable: use false.
                        leftNullId = cf.id;
                    int rightNullId;
                    if (rightId.hasNull())
                        rightNullId = rightId.isNullId;
                    else
                        rightNullId = cf.id;
                    binOp = ev.isNullInstruction.putObject("BinOp");
                    binOp.put("lhs", leftNullId);
                    binOp.put("rhs", rightNullId);
                    binOp.put("kind", Utilities.getExists(opNames, "||"));
                    binOp.put("operand_ty", "Bool");
                }
            }
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPUnaryExpression expression) {
        // "UnOp": {
        //   "lhs": 4,
        //   "operand_ty": "I64",
        //   "kind": "Minus"
        // }
        ExpressionIds leftId = this.accept(expression.source);
        ExpressionRepresentation er = this.insertInstruction(expression);
        ObjectNode binOp = er.instruction.putObject("UnOp");
        String kind;
        switch (expression.operation) {
            case "-":
                kind = "Neg";
                break;
            case "!":
                kind = "Not";
                break;
            default:
                throw new Unimplemented(expression);
        }
        binOp.put("value", leftId.id);
        binOp.put("kind", kind);
        binOp.put("value_ty", baseTypeName(expression.source));
        if (leftId.hasNull()) {
            int leftNullId = leftId.isNullId;
            binOp = er.getNullObject().putObject("CopyVal");
            binOp.put("value", leftNullId);
            binOp.put("value_ty", "Bool");
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPVariablePath expression) {
        ExpressionIds ids = this.resolve(expression.variable);
        this.expressionId.put(expression, ids.id);
        // may already be there, but this may be a new variable with the same name,
        // and then we overwrite with the new definition.
        return false;
    }

    @Override
    public boolean preorder(DBSPClosureExpression expression) {
        for (DBSPParameter param: expression.parameters) {
            DBSPIdentifierPattern identifier = param.pattern.to(DBSPIdentifierPattern.class);
            this.declare(identifier.identifier, needsNull(param.type));
        }
        DBSPType ret = expression.getResultType();
        if (!ToJitVisitor.isScalarType(ret)) {
            this.declare(RETURN_PARAMETER_NAME, ret.mayBeNull);
            this.variableAssigned.add(RETURN_PARAMETER_NAME);
        }
        expression.body.accept(this);
        if (!ToJitVisitor.isScalarType(ret)) {
            String removed = Utilities.removeLast(this.variableAssigned);
            if (!removed.equals(RETURN_PARAMETER_NAME))
                throw new RuntimeException("Unexpected variable removed " + removed);
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPLetStatement statement) {
        if (!statement.type.is(DBSPTypeTuple.class))
            throw new Unimplemented("Variables with non-tuple type not yet supported", statement);
        ExpressionIds ids = this.declare(statement.variable, needsNull(statement.type));
        ArrayNode instruction = Objects.requireNonNull(this.currentBlockBody).addArray();
        instruction.add(ids.id);
        ObjectNode uninit = instruction.addObject();
        ObjectNode storeNode = uninit.putObject("UninitRow");
        int typeId = this.catalog.getTypeId(statement.type);
        storeNode.put("layout", typeId);
        this.variableAssigned.add(statement.variable);
        if (statement.initializer != null)
            statement.initializer.accept(this);
        Utilities.removeLast(this.variableAssigned);
        return false;
    }

    @Override
    public boolean preorder(DBSPFieldExpression expression) {
        // "Load": {
        //   "source": 1,
        //   "source_layout": 2,
        //   "column": 1,
        //   "column_type": "I32"
        // }
        ExpressionRepresentation er = this.insertInstruction(expression);
        ObjectNode load = er.instruction.putObject("Load");
        ExpressionIds sourceId = this.accept(expression.expression);
        load.put("source", sourceId.id);
        int typeId = this.catalog.getTypeId(expression.expression.getNonVoidType());
        load.put("source_layout", typeId);
        load.put("column", expression.fieldNo);
        load.put("column_type", baseTypeName(expression));
        if (er.isNullInstruction != null) {
            ObjectNode isNull = er.isNullInstruction.putObject("IsNull");
            isNull.put("target", sourceId.id);
            isNull.put("target_layout", typeId);
            isNull.put("column", expression.fieldNo);
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPIfExpression expression) {
        // "Branch": {
        //    "cond": {
        //      "Expr": 3
        //    },
        //    "truthy": 3,
        //    "falsy": 2
        // }
        // TODO: handle nulls
        ExpressionIds condId = this.accept(expression.condition);
        ExpressionRepresentation er = this.insertInstruction(expression);
        ObjectNode branch = er.instruction.putObject("Branch");
        ObjectNode branchExpr = branch.putObject("cond");
        branchExpr.put("Expr", condId.id);
        ExpressionIds posId = this.accept(expression.positive);
        ExpressionIds negId = this.accept(expression.negative);
        branch.put("truthy", posId.id);
        branch.put("falsy", negId.id);
        return false;
    }

    @Override
    public boolean preorder(DBSPTupleExpression expression) {
        // Compile this as an assignment to the currently assigned variable
        String variableAssigned = this.variableAssigned.get(this.variableAssigned.size() - 1);
        ExpressionIds retValId = this.resolve(variableAssigned);
        int tupleTypeId = this.catalog.getTypeId(expression.getNonVoidType());
        int index = 0;
        for (DBSPExpression field: expression.fields) {
            // Generates 1 or 2 instructions for each field (depending on nullability)
            // "Store": {
            //   "target": 2,
            //   "target_layout": 3,
            //   "column": 0,
            //   "value": {
            //     "Expr": 3
            //   },
            //   "value_type": "I32"
            // }
            ExpressionIds fieldId = this.accept(field);
            ArrayNode instruction = Objects.requireNonNull(this.currentBlockBody).addArray();
            instruction.add(this.nextExpressionId());
            ObjectNode store = instruction.addObject();
            ObjectNode storeNode = store.putObject("Store");
            storeNode.put("target", retValId.id);
            storeNode.put("target_layout", tupleTypeId);
            storeNode.put("column", index);
            ObjectNode value = storeNode.putObject("value");
            value.put("Expr", fieldId.id);
            storeNode.put("value_type", ToJitVisitor.baseTypeName(field.getNonVoidType()));
            if (fieldId.hasNull()) {
                // "SetNull": {
                //  "target": 2,
                //  "target_layout": 3,
                //  "column": 0,
                //  "is_null": {
                //    "Expr": 4 }}
                instruction = Objects.requireNonNull(this.currentBlockBody).addArray();
                instruction.add(this.nextExpressionId());
                store = instruction.addObject();
                storeNode = store.putObject("SetNull");
                storeNode.put("target", retValId.id);
                storeNode.put("target_layout", tupleTypeId);
                storeNode.put("column", index);
                ObjectNode isNull = storeNode.putObject("is_null");
                isNull.put("Expr", fieldId.isNullId);
            }
            index++;
        }
        return false;
    }

    @Override
    public boolean preorder(DBSPBlockExpression expression) {
        this.newContext();
        ObjectNode saveBlock = this.currentBlock;
        ArrayNode saveBody = this.currentBlockBody;
        int blockId = this.nextBlockId();
        this.currentBlock = this.blocks.putObject(Integer.toString(blockId));
        this.currentBlock.put("id", blockId);
        this.currentBlockBody = this.currentBlock.putArray("body");
        for (DBSPStatement stat: expression.contents)
            stat.accept(this);

        // TODO: handle nullability
        ExpressionIds resultId = null;
        if (expression.lastExpression != null) {
            if (ToJitVisitor.isScalarType(expression.lastExpression.getType())) {
                resultId = this.accept(expression.lastExpression);
            } else {
                // This will store the result in the return value
                // and this block will return Unit.
                expression.lastExpression.accept(this);
            }
        }
        ObjectNode terminator = this.currentBlock.putObject("terminator");
        ObjectNode ret = terminator.putObject("Return");
        ObjectNode retValue = ret.putObject("value");
        if (resultId != null) {
            retValue.put("Expr", resultId.id);
        } else {
            retValue.put("Imm", "Unit");
        }
        this.popContext();
        this.currentBlock = saveBlock;
        this.currentBlockBody = saveBody;
        return false;
    }

    /**
     * Convert the body of a closure expression to a JSON representation
     * @param expression  Expression to generate code for.
     * @param blocks      Insert here the blocks of the body of the closure.
     * @param catalog     The catalog of Tuple types.
     */
    static void convertClosure(DBSPClosureExpression expression,
                               ObjectNode blocks,
                               ToJitVisitor.TypeCatalog catalog) {
        ToJitInnerVisitor visitor = new ToJitInnerVisitor(blocks, catalog);
        visitor.newContext();
        expression.accept(visitor);
        visitor.popContext();
    }
}
