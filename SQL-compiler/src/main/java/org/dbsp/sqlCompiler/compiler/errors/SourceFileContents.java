package org.dbsp.sqlCompiler.compiler.errors;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Keep track of the contents of the source file supplied to the compiler.
 */
public class SourceFileContents {
    /**
     * Currently we can have a single source file, but perhaps some day we will
     * support something like #include.
     */
    final String sourceFileName;
    final List<String> lines;
    public final String wholeProgram;

    public SourceFileContents(String sourceFileName, InputStream stream) throws IOException {
        this.sourceFileName = sourceFileName;
        this.lines = new ArrayList<>();
        BufferedReader br = new BufferedReader(new InputStreamReader(stream));
        String line;
        StringBuilder builder = new StringBuilder();
        while ((line = br.readLine()) != null) {
            this.lines.add(line);
            builder.append(line)
                    .append(SourceFileContents.newline());
        }
        this.wholeProgram = builder.toString();
    }

    public static String newline() {
        return System.lineSeparator();
    }

    public String getFragment(SourcePositionRange range) {
        if (!range.isValid())
            return "";
        int startLine = range.start.line - 1;
        int endLine = range.end.line - 1;
        int startCol = range.start.column - 1;
        int endCol = range.end.column;
        StringBuilder result = new StringBuilder();
        if (startLine == endLine) {
            if (startLine >= this.lines.size())
                // This should not really happen.
                return "";
            String line = this.lines.get(startLine);
            result.append(line).append(SourceFileContents.newline());
            for (int i = 0; i < startCol; i++)
                result.append(" ");
            for (int i = startCol; i < endCol; i++)
                result.append("^");
            result.append(SourceFileContents.newline());
        } else {
            if (endLine - startLine < 5) {
                result.append("Error appears in this block:").append(SourceFileContents.newline());
                for (int i = startLine; i < endLine; i++) {
                    result.append(this.lines.get(i)).append(SourceFileContents.newline());
                }
            }
        }
        return result.toString();
    }

    public String getSourceFileName(SourcePosition position) {
        return this.sourceFileName;
    }
}
