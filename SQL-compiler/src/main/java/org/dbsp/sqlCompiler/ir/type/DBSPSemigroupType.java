/*
 * Copyright 2023 VMware, Inc.
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

package org.dbsp.sqlCompiler.ir.type;

import org.dbsp.sqlCompiler.ir.InnerVisitor;
import org.dbsp.util.IIndentStream;
import org.dbsp.util.Linq;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * Represents a Semigroup trait implementation.
 */
public class DBSPSemigroupType extends DBSPTypeUser {
    /**
     * Keep track of the semigroup sizes that appear in the program to properly generate
     * Rust code to instantiate them.
     */
    public static final Set<Integer> sizesUsed = new HashSet<>();

    @SuppressWarnings("UnusedReturnValue")
    public static IIndentStream preamble(IIndentStream stream) {
        /*
        #[derive(Clone)]
        pub struct Semigroup2<T0, T1, TS0, TS1>(PhantomData<(T0, T1, TS0, TS1)>);

        impl<T0, T1, TS0, TS1> Semigroup<(T0, T1)> for Semigroup2<T0, T1, TS0, TS1>
        where
            TS0: Semigroup<T0>,
            TS1: Semigroup<T1>,
        {
            fn combine(left: &(T0, T1), right: &(T0, T1)) -> (T0, T1) {
                (
                    TS0::combine(&left.0, &right.0),
                    TS1::combine(&left.1, &right.1),
                )
            }
        }
         */
        for (int i: DBSPSemigroupType.sizesUsed) {
            Integer[] indexes = new Integer[i];
            IntStream.range(0, i).forEach(ix -> indexes[ix] = ix);
            String[] ts = Linq.map(indexes, ix -> "T" + ix, String.class);
            String[] tts = Linq.map(indexes, ix -> "TS" + ix, String.class);

            stream.append("#[derive(Clone)]").newline()
                    .append("pub struct Semigroup")
                    .append(i)
                    .append("<")
                    .intercalate(", ", ts)
                    .join(", ", tts)
                    .append(">(PhantomData<(")
                    .intercalate(", ", ts)
                    .join(", ", tts)
                    .append(")>);")
                    .newline()
                    .newline();

            stream.append("impl<")
                    .intercalate(", ", ts)
                    .join(", ", tts)
                    .append("> Semigroup")
                    .append("<(")
                    .intercalate(", ", indexes, ix -> "T" + ix)
                    .append(")> for Semigroup")
                    .append(i)
                    .append("<")
                    .intercalate(", ", ts)
                    .join(", ", tts)
                    .append(">")
                    .newline()
                    .append("where").increase()
                    .join(",\n", indexes, ix -> "TS" + ix + ": Semigroup<T" + ix + ">")
                    .newline().decrease()
                    .append("{").increase()
                    .append("fn combine(left: &(")
                    .intercalate(", ", ts)
                    .append("), right:&(")
                    .intercalate(", ", ts)
                    .append(")) -> (")
                    .intercalate(", ", ts)
                    .append(") {").increase()
                    .append("(").increase()
                    .join("\n", indexes, ix -> "TS" + ix + "::combine(&left." + ix + ", &right." + ix + "),")
                    .newline().decrease()
                    .append(")").newline()
                    .decrease()
                    .append("}").newline()
                    .decrease()
                    .append("}").newline();
        }
        sizesUsed.clear();
        return stream;
    }

    public DBSPSemigroupType(DBSPType[] elementTypes, DBSPType[] semigroupTypes) {
        super(null, "Semigroup" + elementTypes.length, false,
                Linq.concat(semigroupTypes, elementTypes));
        sizesUsed.add(elementTypes.length);
        if (elementTypes.length != semigroupTypes.length)
            throw new RuntimeException("Each element must have a corresponding semigroup, but I have " +
                    elementTypes.length + " and " + semigroupTypes.length);
    }

    @Override
    public void accept(InnerVisitor visitor) {
        if (!visitor.preorder(this)) return;
        for (DBSPType type: this.typeArgs)
            type.accept(visitor);
        visitor.postorder(this);
    }
}
