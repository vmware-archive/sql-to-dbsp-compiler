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
 *
 *
 */

package org.dbsp.sqlCompiler.frontend;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * Packages together the results of compiling a set of view declarations.
 */
public class CalciteProgram {
    /**
     * Views declared in SQL.
     */
    public final List<ViewDDL> views;
    /**
     * Declarations of tables that these views depend on.
     */
    public final List<TableDDL> inputTables;

    public CalciteProgram() {
        this.views = new ArrayList<>();
        this.inputTables = new ArrayList<>();
    }

    public void addView(ViewDDL result) {
        this.views.add(result);
    }

    public void addInput(TableDDL table) {
        this.inputTables.add(table);
    }

    @Nullable
    public TableDDL getInputTable(String table) {
        for (TableDDL t : this.inputTables)
            if (t.name.equals(table))
                return t;
        return null;
    }
}
