package ch.cypherchecker.memgraph.ast;

import ch.cypherchecker.cypher.ast.CypherConstant;

public class MemgraphLocalTimeConstant extends CypherConstant {

    private final int hours;
    private final String separator;
    private final Integer minutes;
    private final Integer seconds;
    private final String nanoSecondSeparator;
    private final Integer nanoSeconds;

    public MemgraphLocalTimeConstant(int hours,
                                     String separator,
                                     Integer minutes,
                                     Integer seconds,
                                     String nanoSecondSeparator,
                                     Integer nanoSeconds) {
        this.hours = hours;
        this.separator = separator;
        this.minutes = minutes;
        this.seconds = seconds;
        this.nanoSecondSeparator = nanoSecondSeparator;
        this.nanoSeconds = nanoSeconds;
    }

    @Override
    public String getTextRepresentation() {
        StringBuilder representation = new StringBuilder();
        representation.append("localTime('");
        representation.append(String.format("%02d", hours));

        if (minutes != null) {
            representation.append(separator);
            representation.append(String.format("%02d", minutes));

            if (seconds != null) {
                representation.append(separator);
                representation.append(String.format("%02d", seconds));

                if (nanoSeconds != null) {
                    representation.append(nanoSecondSeparator);
                    representation.append(nanoSeconds);
                }
            }
        }

        representation.append("')");

        return representation.toString();
    }
}
