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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.client.Column;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static io.trino.client.spooling.encoding.FixJsonDataUtils.fixData;
import static java.util.Objects.requireNonNull;

public class JsonQueryDataAccess
        implements QueryDataAccess
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<List<List<Object>>> TYPE = new TypeReference<List<List<Object>>>() {};

    private final List<Column> columns;
    private final InputStream stream;

    public JsonQueryDataAccess(List<Column> columns, InputStream stream)
    {
        this.columns = requireNonNull(columns, "columns is null");
        this.stream = requireNonNull(stream, "stream is null");
    }

    @Override
    public Iterable<List<Object>> toIterable()
            throws IOException
    {
        return fixData(columns, OBJECT_MAPPER.readValue(stream, TYPE));
    }
}
