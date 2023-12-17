package ch.cypherchecker.memgraph.ast;

import ch.cypherchecker.cypher.ast.CypherFunctionDescription;
import ch.cypherchecker.memgraph.schema.MemgraphType;
import ch.cypherchecker.util.Randomization;

// See: https://memgraph.com/docs/cypher-manual/functions/
public enum MemgraphFunction implements CypherFunctionDescription<MemgraphType> {

    TO_BOOLEAN("toBoolean", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.BOOLEAN;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            MemgraphType chosenType = Randomization.fromOptions(MemgraphType.BOOLEAN,
                    MemgraphType.STRING,
                    MemgraphType.INTEGER);

            return new MemgraphType[]{chosenType};
        }
    },
    ABS("abs", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.INTEGER || returnType == MemgraphType.FLOAT;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            assert returnType == MemgraphType.INTEGER || returnType == MemgraphType.FLOAT;
            return new MemgraphType[]{returnType};
        }
    },
    SIGN("sign", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.INTEGER;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{Randomization.fromOptions(MemgraphType.INTEGER, MemgraphType.FLOAT)};
        }
    },
    TO_INTEGER("toInteger", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.INTEGER;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            MemgraphType chosenType = Randomization.fromOptions(MemgraphType.BOOLEAN,
                    MemgraphType.STRING,
                    MemgraphType.INTEGER,
                    MemgraphType.FLOAT);

            return new MemgraphType[]{chosenType};
        }
    },
/*    DURATION_BETWEEN("duration.between", 2) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.DURATION;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.DATE, MemgraphType.DATE};
        }
    },
    DURATION_IN_MONTHS("duration.inMonths", 2) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.DURATION;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.DATE, MemgraphType.DATE};
        }
    },
    DURATION_IN_DAYS("duration.inDays", 2) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.DURATION;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.DATE, MemgraphType.DATE};
        }
    },
    DURATION_IN_SECONDS("duration.inSeconds", 2) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.DURATION;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.DATE, MemgraphType.DATE};
        }
    },*/
    LEFT("left", 2) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.STRING;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.STRING, MemgraphType.INTEGER};
        }
    },
    RIGHT("right", 2) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.STRING;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.STRING, MemgraphType.INTEGER};
        }
    },
    LTRIM("lTrim", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.STRING;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.STRING};
        }
    },
    RTRIM("rTrim", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.STRING;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.STRING};
        }
    },
    TRIM("trim", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.STRING;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.STRING};
        }
    },
    TO_LOWER("toLower", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.STRING;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.STRING};
        }
    },
    TO_UPPER("toUpper", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.STRING;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.STRING};
        }
    },
    REVERSE("reverse", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.STRING;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.STRING};
        }
    },
    REPLACE("replace", 3) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.STRING;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.STRING, MemgraphType.STRING, MemgraphType.STRING};
        }
    },
    SUBSTRING("substring", 3) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.STRING;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.STRING, MemgraphType.INTEGER, MemgraphType.INTEGER};
        }
    },
    TO_STRING("toString", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.STRING;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.getRandom()};
        }
    },
    CEIL("ceil", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.INTEGER;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.FLOAT};
        }
    },
    FLOOR("floor", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.INTEGER;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.FLOAT};
        }
    },
    TO_FLOAT("toFloat", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.FLOAT;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{Randomization.fromOptions(MemgraphType.STRING,
                    MemgraphType.INTEGER,
                    MemgraphType.FLOAT)};
        }
    },
    SIZE("size", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.INTEGER;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.STRING};
        }
    },
    ROUND("round", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.FLOAT;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{MemgraphType.FLOAT};
        }
    },
    E("e", 0) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.FLOAT;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{};
        }
    },
    EXP("exp", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.FLOAT;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{ Randomization.fromOptions(MemgraphType.INTEGER, MemgraphType.FLOAT) };
        }
    },
    LOG("log", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.FLOAT;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{ Randomization.fromOptions(MemgraphType.INTEGER, MemgraphType.FLOAT) };
        }
    },
    LOG_10("log10", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.FLOAT;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{ Randomization.fromOptions(MemgraphType.INTEGER, MemgraphType.FLOAT) };
        }
    },
    SQRT("sqrt", 1) {
        @Override
        public boolean supportReturnType(MemgraphType returnType) {
            return returnType == MemgraphType.FLOAT;
        }

        @Override
        public MemgraphType[] getArgumentTypes(MemgraphType returnType) {
            return new MemgraphType[]{ Randomization.fromOptions(MemgraphType.INTEGER, MemgraphType.FLOAT) };
        }
    };

    private final String name;
    private final int arity;

    MemgraphFunction(String name, int arity) {
        this.name = name;
        this.arity = arity;
    }

    @Override
    public int getArity() {
        return arity;
    }

    @Override
    public String getName() {
        return name;
    }

}
