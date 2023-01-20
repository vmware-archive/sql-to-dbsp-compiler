/*
 * Copyright 2022 VMware, Inc.
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

package org.dbsp.zetasqltest;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.dbsp.Zetalexer;
import org.dbsp.Zetatest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstraction for a .test file from Zeta SQL Tests.
 */
public class ZetaSQLTestFile {
    public final List<ZetaSQLTest> tests;

    public ZetaSQLTestFile() {
        this.tests = new ArrayList<>();
    }

    public static ZetaSQLTestFile parse(String file) throws IOException {
        Zetalexer lexer = new Zetalexer(CharStreams.fromFileName(file));
        Zetatest parser = new Zetatest(new CommonTokenStream(lexer));
        ZetatestVisitor visitor = new ZetatestVisitor();
        parser.tests().accept(visitor);
        return visitor.tests;
    }

    public void add(ZetaSQLTest test) {
        this.tests.add(test);
    }

    public int size() {
        return this.tests.size();
    }

    public ZetaSQLTest get(int i) {
        return this.tests.get(i);
    }
}
