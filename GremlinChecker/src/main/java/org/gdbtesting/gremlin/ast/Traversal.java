package org.gdbtesting.gremlin.ast;

import org.gdbtesting.gremlin.ConstantType;

public class Traversal implements GraphExpression {

    protected String startStep = "null";
    protected String endStep = "null";

    protected ConstantType dataType = ConstantType.STRING;

    protected String traversalType;

    public String getTraversalType() {
        return traversalType;
    }

    public void setTraversalType(String traversalType) {
        this.traversalType = traversalType;
    }

    public String getStartStep() {
        return startStep;
    }

    public void setStartStep(String startStep) {
        this.startStep = startStep;
    }

    public String getEndStep() {
        return endStep;
    }

    public void setEndStep(String endStep) {
        this.endStep = endStep;
    }

    public ConstantType getDataType() {
        return dataType;
    }

    public void setDataType(ConstantType dataType) {
        this.dataType = dataType;
    }

}
