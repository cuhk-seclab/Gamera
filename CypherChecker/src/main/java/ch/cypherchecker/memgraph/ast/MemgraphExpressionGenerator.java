package ch.cypherchecker.memgraph.ast;

import ch.cypherchecker.common.schema.Entity;
import ch.cypherchecker.cypher.ast.*;
import ch.cypherchecker.memgraph.MemgraphBugs;
import ch.cypherchecker.memgraph.ast.*;
import ch.cypherchecker.memgraph.schema.MemgraphType;
import ch.cypherchecker.util.IgnoreMeException;
import ch.cypherchecker.util.Randomization;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// TODO: Try to extract common base class from Neo4JExpressionGenerator and RedisExpressionGenerator
public class MemgraphExpressionGenerator {

    private static final int MAX_DEPTH = 3;

    private final Map<String, Entity<MemgraphType>> variables;

    public MemgraphExpressionGenerator(Map<String, Entity<MemgraphType>> variables) {
        this.variables = variables;
    }

    public MemgraphExpressionGenerator() {
        this.variables = new HashMap<>();
    }

    private enum IntegerConstantFormat {
        NORMAL_INTEGER,
        HEX_INTEGER,
        OCTAL_INTEGER
    }

    public static CypherExpression generateConstant(MemgraphType type) {
        if (Randomization.smallBiasProbability()) {
            return new CypherConstant.NullConstant();
        }

        switch (type) {
            case INTEGER:
                return switch (Randomization.fromOptions(MemgraphExpressionGenerator.IntegerConstantFormat.values())) {
                    case NORMAL_INTEGER -> new CypherConstant.IntegerConstant(Randomization.getInteger());
                    case HEX_INTEGER -> new CypherConstant.IntegerHexConstant(Randomization.getInteger());
                    case OCTAL_INTEGER -> new CypherConstant.IntegerOctalConstant(Randomization.getInteger());
                };
            case BOOLEAN:
                return new CypherConstant.BooleanConstant(Randomization.getBoolean());
            case FLOAT:
                return new CypherConstant.FloatConstant(Randomization.getFloat());
            case STRING:
                return new CypherConstant.StringConstant(Randomization.getString());
            case DURATION:
                Map<String, Long> datePart;
                Map<String, Long> timePart;

                do {
                    datePart = new LinkedHashMap<>();

                    for (String current : new String[]{"Y", "M", "W", "D"}) {
                        if (Randomization.getBoolean()) {
                            if (MemgraphBugs.bug1) {
                                datePart.put(current, Randomization.getPositiveInt());
                            } else {
                                datePart.put(current, Randomization.getPositiveInteger());
                            }
                        }
                    }

                    timePart = new LinkedHashMap<>();

                    for (String current : new String[]{"H", "M", "S"}) {
                        if (Randomization.getBoolean()) {
                            if (MemgraphBugs.bug1) {
                                timePart.put(current, Randomization.getPositiveInt());
                            } else {
                                timePart.put(current, Randomization.getPositiveInteger());
                            }
                        }
                    }
                } while (datePart.isEmpty() && timePart.isEmpty());

                return new MemgraphDurationConstant(datePart, timePart);
            case LOCAL_TIME:
                int hours = Randomization.nextInt(0, 24);
                String separator;
                Integer minutes = null;
                Integer seconds = null;
                String nanoSecondSeparator = null;
                Integer nanoSeconds = null;

                if (Randomization.getBoolean()) {
                    separator = ":";
                } else {
                    separator = "";
                }

                if (Randomization.getBoolean()) {
                    minutes = Randomization.nextInt(0, 59);

                    if (Randomization.getBoolean()) {
                        seconds = Randomization.nextInt(0, 59);

                        if (Randomization.getBoolean()) {
                            if (Randomization.getBoolean()) {
                                nanoSecondSeparator = ".";
                            } else {
                                nanoSecondSeparator = ",";
                            }

                            nanoSeconds = Randomization.nextInt(0, 1000000000);
                        }
                    }
                }

                return new MemgraphLocalTimeConstant(hours,
                        separator,
                        minutes,
                        seconds,
                        nanoSecondSeparator,
                        nanoSeconds);
            case DATE:
                return new MemgraphDateConstant(Randomization.getBoolean(),
                        Randomization.nextInt(0, 1000),
                        Randomization.nextInt(1, 13),
                        Randomization.nextInt(1, 32));
            default:
                throw new AssertionError(type);
        }
    }

    /*
     * BINARY_LOGICAL_OPERATOR: AND, OR, XOR
     * NOT: NOT
     * POSTFIX_OPERATOR: IS NULL, IS NOT NULL
     * BINARY_COMPARISON: =, <>, <, <=, >, >=
     * BINARY_STRING_OPERATOR: STARTS WITH, ENDS WITH, CONTAINS
     * REGEX:
     * FUNCTION: toBoolean, toBooleanOrNull, abs, sign, toInteger, toIntegerOrNull, duration.between, duration.inMonths,
     *           duration.inDays, duration.inSeconds, left, right, lTrim, rTrim, trim, toLower, toUpper,
     *           reverse, replace, substring, toString, toStringOrNull, ceil, floor, toFloat, toFloatOrNull,
     *           point.distance, isEmpty, size, round, e, exp, log, log10, sqrt, isNaN
     */
    private enum BooleanExpression {
        BINARY_LOGICAL_OPERATOR, NOT,
        POSTFIX_OPERATOR, BINARY_COMPARISON,
        BINARY_STRING_OPERATOR, REGEX, FUNCTION
    }

    // TODO: Support IN_OPERATION
    private CypherExpression generateBooleanExpression(int depth) {
        MemgraphExpressionGenerator.BooleanExpression option = Randomization.fromOptions(MemgraphExpressionGenerator.BooleanExpression.values());

        switch (option) {
            case BINARY_LOGICAL_OPERATOR:
                CypherExpression first = generateExpression(depth + 1, MemgraphType.BOOLEAN);
                int nr = Randomization.smallNumber() + 1;

                for (int i = 0; i < nr; i++) {
                    first = new CypherBinaryLogicalOperation(first,
                            generateExpression(depth + 1, MemgraphType.BOOLEAN),
                            CypherBinaryLogicalOperation.BinaryLogicalOperator.getRandom());
                }

                return first;
            case NOT:
                return new CypherPrefixOperation(generateExpression(depth + 1, MemgraphType.BOOLEAN),
                        CypherPrefixOperation.PrefixOperator.NOT);
            case POSTFIX_OPERATOR:
                return new CypherPostfixOperation(generateExpression(depth + 1),
                        CypherPostfixOperation.PostfixOperator.getRandom());
            case BINARY_COMPARISON:
                return generateComparison(depth, Randomization.fromOptions(MemgraphType.INTEGER,
                        MemgraphType.FLOAT, MemgraphType.STRING, MemgraphType.BOOLEAN, MemgraphType.DATE,
                        MemgraphType.LOCAL_TIME));
            case BINARY_STRING_OPERATOR:
                return new CypherBinaryStringOperation(generateExpression(depth + 1, MemgraphType.STRING),
                        generateExpression(depth + 1, MemgraphType.STRING),
                        CypherBinaryStringOperation.BinaryStringOperation.getRandom());
            case REGEX:
                return new CypherRegularExpression(generateExpression(depth + 1, MemgraphType.STRING),
                        generateExpression(depth + 1, MemgraphType.STRING));
            case FUNCTION:
                return generateFunction(depth + 1, MemgraphType.BOOLEAN);
            default:
                throw new AssertionError(option);
        }
    }

    private CypherExpression generateComparison(int depth, MemgraphType type) {
        CypherExpression left = generateExpression(depth + 1, type);
        CypherExpression right = generateExpression(depth + 1, type);
        return new CypherBinaryComparisonOperation(left, right,
                CypherBinaryComparisonOperation.BinaryComparisonOperator.getRandom());
    }

    /*
     * UNARY_OPERATION: +, -
     * BINARY_ARITHMETIC_EXPRESSION: +, -, *, /, %, ^
     * FUNCTION:
     */
    private enum IntExpression {
        UNARY_OPERATION, BINARY_ARITHMETIC_EXPRESSION, FUNCTION
    }

    private CypherExpression generateIntegerExpression(int depth) {
        switch (Randomization.fromOptions(MemgraphExpressionGenerator.IntExpression.values())) {
            case UNARY_OPERATION:
                CypherExpression intExpression = generateExpression(depth + 1, MemgraphType.INTEGER);
                CypherPrefixOperation.PrefixOperator operator = Randomization.getBoolean()
                        ? CypherPrefixOperation.PrefixOperator.UNARY_PLUS
                        : CypherPrefixOperation.PrefixOperator.UNARY_MINUS;

                return new CypherPrefixOperation(intExpression, operator);
            case BINARY_ARITHMETIC_EXPRESSION:
                CypherExpression left = generateExpression(depth + 1, MemgraphType.INTEGER);
                CypherExpression right = generateExpression(depth + 1, MemgraphType.INTEGER);

                return new CypherBinaryArithmeticOperation(left, right, CypherBinaryArithmeticOperation.ArithmeticOperator.getRandomIntegerOperator());
            case FUNCTION:
                return generateFunction(depth + 1, MemgraphType.INTEGER);
            default:
                throw new AssertionError();
        }
    }

    /*
     * UNARY_OPERATION: +, -
     * BINARY_ARITHMETIC_EXPRESSION: +, -, *, /, %, ^
     * FUNCTION:
     */
    private enum FloatExpression {
        UNARY_OPERATION, BINARY_ARITHMETIC_EXPRESSION, FUNCTION
    }

    private CypherExpression generateFloatExpression(int depth) {
        switch (Randomization.fromOptions(MemgraphExpressionGenerator.FloatExpression.values())) {
            case UNARY_OPERATION:
                CypherExpression intExpression = generateExpression(depth + 1, MemgraphType.FLOAT);
                CypherPrefixOperation.PrefixOperator operator = Randomization.getBoolean()
                        ? CypherPrefixOperation.PrefixOperator.UNARY_PLUS
                        : CypherPrefixOperation.PrefixOperator.UNARY_MINUS;

                return new CypherPrefixOperation(intExpression, operator);
            case BINARY_ARITHMETIC_EXPRESSION:
                CypherExpression left;
                CypherExpression right;

                // At least one of the two expressions has to be a float
                if (Randomization.getBoolean()) {
                    left = generateExpression(depth + 1, MemgraphType.FLOAT);
                    right = generateExpression(depth + 1, MemgraphType.FLOAT);
                } else {
                    if (Randomization.getBoolean()) {
                        left = generateExpression(depth + 1, MemgraphType.INTEGER);
                        right = generateExpression(depth + 1, MemgraphType.FLOAT);
                    } else {
                        left = generateExpression(depth + 1, MemgraphType.FLOAT);
                        right = generateExpression(depth + 1, MemgraphType.INTEGER);
                    }
                }

                return new CypherBinaryArithmeticOperation(left, right, CypherBinaryArithmeticOperation.ArithmeticOperator.getRandomFloatOperator());
            case FUNCTION:
                return generateFunction(depth + 1, MemgraphType.FLOAT);
            default:
                throw new AssertionError();
        }
    }

    private enum StringExpression {
        CONCAT, FUNCTION
    }

    private CypherExpression generateStringExpression(int depth) {
        switch (Randomization.fromOptions(MemgraphExpressionGenerator.StringExpression.values())) {
            case CONCAT:
                CypherExpression left = generateExpression(depth + 1, MemgraphType.STRING);
                CypherExpression right = generateExpression(depth + 1, Randomization.fromOptions(MemgraphType.STRING, MemgraphType.FLOAT, MemgraphType.INTEGER));
                return new CypherConcatOperation(left, right);
            case FUNCTION:
                return generateFunction(depth + 1, MemgraphType.STRING);
            default:
                throw new AssertionError();
        }
    }

    private CypherExpression generateDurationExpression(int depth) {
        return generateFunction(depth + 1, MemgraphType.DURATION);
    }

    public static CypherExpression generateExpression() {
        return generateExpression(MemgraphType.getRandom());
    }

    public static CypherExpression generateExpression(MemgraphType type) {
        return new MemgraphExpressionGenerator().generateExpression(0, type);
    }

    public CypherExpression generateExpression(int depth) {
        return generateExpression(depth, MemgraphType.getRandom());
    }

    private CypherExpression generateExpression(int depth, MemgraphType type) {
        if (!filterVariables(type).isEmpty() && Randomization.getBoolean()) {
            return getVariableExpression(type);
        }

        return generateExpressionInternal(depth, type);
    }

    public static CypherExpression generateExpression(Map<String, Entity<MemgraphType>> variables, MemgraphType type) {
        return new MemgraphExpressionGenerator(variables).generateExpression(0, type);
    }

    public static CypherExpression generateExpression(Map<String, Entity<MemgraphType>> variables) {
        return generateExpression(variables, MemgraphType.getRandom());
    }

    private CypherExpression getVariableExpression(MemgraphType type) {
        List<String> variables = filterVariables(type);
        return new CypherVariablePropertyAccess(Randomization.fromList(variables));
    }

    private List<String> filterVariables(MemgraphType type) {
        if (variables == null) {
            return Collections.emptyList();
        } else {
            List<String> filteredVariables = new ArrayList<>();

            for (String variable : variables.keySet()) {
                Map<String, MemgraphType> properties = variables.get(variable).availableProperties();

                for (String property : properties.keySet()) {
                    if (properties.get(property) == type) {
                        filteredVariables.add(variable + "." + property);
                    }
                }
            }

            return filteredVariables;
        }
    }

    private CypherExpression generateExpressionInternal(int depth, MemgraphType type) {
        if (depth > MAX_DEPTH || Randomization.smallBiasProbability()) {
            return generateConstant(type);
        } else {
            return switch (type) {
                case BOOLEAN -> generateBooleanExpression(depth);
                case INTEGER -> generateIntegerExpression(depth);
                case STRING -> generateStringExpression(depth);
                case DURATION -> generateDurationExpression(depth);
                case FLOAT -> generateFloatExpression(depth);
                default -> generateConstant(type);
            };
        }
    }

    private CypherFunctionCall<MemgraphType> generateFunction(int depth, MemgraphType returnType) {
        List<MemgraphFunction> functions = Stream.of(MemgraphFunction.values())
                .filter(function -> function.supportReturnType(returnType))
                .collect(Collectors.toList());

        if (functions.isEmpty()) {
            throw new IgnoreMeException();
        }

        MemgraphFunction chosenFunction = Randomization.fromList(functions);
        int arity = chosenFunction.getArity();
        MemgraphType[] argumentTypes = chosenFunction.getArgumentTypes(returnType);
        CypherExpression[] arguments = new CypherExpression[arity];

        for (int i = 0; i < arity; i++) {
            arguments[i] = generateExpression(depth + 1, argumentTypes[i]);
        }

        return new CypherFunctionCall<>(chosenFunction, arguments);
    }
}
