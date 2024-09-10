/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.client.spooling.encoding;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.trino.client.Column;
import io.trino.client.spooling.encoding.JsonDecodingUtils.TypeDecoder;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterators.unmodifiableIterator;
import static io.trino.client.spooling.encoding.JsonDecodingUtils.createTypeDecoders;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class JsonQueryDataAccess
        implements QueryDataAccess
{
    private final InputStream stream;
    private final TypeDecoder[] decoders;

    public JsonQueryDataAccess(List<Column> columns, InputStream stream)
    {
        this.decoders = createTypeDecoders(columns);
        this.stream = requireNonNull(stream, "stream is null");
    }

    @SuppressModernizer
    @Override
    public Iterable<List<Object>> toIterable()
            throws IOException
    {
        return new RowWiseIterator(stream, decoders);
    }

    private static class RowWiseIterator
            implements Iterable<List<Object>>, Iterator<List<Object>>
    {
        private final InputStream stream;
        private boolean closed;
        private final JsonParser parser;
        private final TypeDecoder[] decoders;

        @SuppressModernizer // There is no JsonFactory in the client module
        public RowWiseIterator(InputStream stream, TypeDecoder[] decoders)
                throws IOException
        {
            this.stream = requireNonNull(stream, "stream is null");
            this.parser = new JsonFactory().createParser(stream);
            this.decoders = requireNonNull(decoders, "decoders is null");

            try {
                verify(parser.nextToken() == START_ARRAY, "Expected start of array, but got %s", parser.currentToken());
                verify(parser.nextToken() == START_ARRAY, "Expected start of array, but got %s", parser.currentToken());
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void advanceEndValues()
        {
            try {
                verify(parser.nextToken() == END_ARRAY, "Expected end of array, but got %s", parser.currentToken());
                switch (parser.nextToken()) {
                    case END_ARRAY:
                        closed = true;
                        stream.close();
                        break;
                    case START_ARRAY:
                        closed = false;
                        break;
                    default:
                        throw new IllegalStateException("Expected end of array or start of array, but got " + parser.currentToken());
                }
            }
            catch (IOException e) {
                closed = true;
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public boolean hasNext()
        {
            return !closed;
        }

        @Override
        public List<Object> next()
        {
            List<Object> row = new ArrayList<>(decoders.length);
            for (TypeDecoder decoder : decoders) {
                try {
                    row.add(decoder.decode(parser));
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            advanceEndValues();
            return unmodifiableList(row);
        }

        @Override
        public Iterator<List<Object>> iterator()
        {
            return unmodifiableIterator(this);
        }
    }
}
