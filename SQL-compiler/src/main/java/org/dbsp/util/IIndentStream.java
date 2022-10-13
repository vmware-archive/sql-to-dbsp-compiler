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

package org.dbsp.util;

import java.util.List;

public interface IIndentStream extends Appendable {
    IIndentStream appendChar(char c);
    IIndentStream append(String string);
    <T extends ToIndentableString> IIndentStream append(T value);
    IIndentStream append(int value);
    IIndentStream append(long value);
    IIndentStream joinS(String separator, List<String> data);
    IIndentStream join(String separator, String[] data);
    <T extends ToIndentableString> IIndentStream join(String separator, T[] data);
    <T extends ToIndentableString> IIndentStream join(String separator, List<T> data);
    <T extends ToIndentableString> IIndentStream intercalate(String separator, List<T> data);
    <T extends ToIndentableString> IIndentStream intercalate(String separator, T[] data);
    IIndentStream intercalateS(String separator, List<String> data);
    IIndentStream newline();
    IIndentStream increase();
    IIndentStream decrease();
}
