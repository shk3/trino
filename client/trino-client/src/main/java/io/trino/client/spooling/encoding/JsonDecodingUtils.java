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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.collect.ImmutableList;
import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;
import io.trino.client.Column;
import io.trino.client.NamedClientTypeSignature;
import io.trino.client.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.fasterxml.jackson.core.JsonToken.END_ARRAY;
import static com.fasterxml.jackson.core.JsonToken.START_ARRAY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.client.ClientStandardTypes.ARRAY;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.ClientStandardTypes.BOOLEAN;
import static io.trino.client.ClientStandardTypes.CHAR;
import static io.trino.client.ClientStandardTypes.DATE;
import static io.trino.client.ClientStandardTypes.DECIMAL;
import static io.trino.client.ClientStandardTypes.DOUBLE;
import static io.trino.client.ClientStandardTypes.GEOMETRY;
import static io.trino.client.ClientStandardTypes.INTEGER;
import static io.trino.client.ClientStandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.client.ClientStandardTypes.INTERVAL_YEAR_TO_MONTH;
import static io.trino.client.ClientStandardTypes.IPADDRESS;
import static io.trino.client.ClientStandardTypes.JSON;
import static io.trino.client.ClientStandardTypes.MAP;
import static io.trino.client.ClientStandardTypes.REAL;
import static io.trino.client.ClientStandardTypes.ROW;
import static io.trino.client.ClientStandardTypes.SMALLINT;
import static io.trino.client.ClientStandardTypes.SPHERICAL_GEOGRAPHY;
import static io.trino.client.ClientStandardTypes.TIME;
import static io.trino.client.ClientStandardTypes.TIMESTAMP;
import static io.trino.client.ClientStandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.client.ClientStandardTypes.TIME_WITH_TIME_ZONE;
import static io.trino.client.ClientStandardTypes.TINYINT;
import static io.trino.client.ClientStandardTypes.UUID;
import static io.trino.client.ClientStandardTypes.VARCHAR;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public final class JsonDecodingUtils
{
    private JsonDecodingUtils() {}

    static TypeDecoder[] createTypeDecoders(List<Column> columns)
    {
        requireNonNull(columns, "columns is null");
        TypeDecoder[] decoders = new TypeDecoder[columns.size()];
        int index = 0;
        for (Column column : columns) {
            decoders[index++] = createTypeDecoder(column.getTypeSignature());
        }
        return decoders;
    }

    interface TypeDecoder
    {
        Object decode(JsonParser parser)
                throws IOException;
    }

    private static TypeDecoder createTypeDecoder(ClientTypeSignature signature)
    {
        switch (signature.getRawType()) {
            case BIGINT:
                return new BigIntegerDecoder();
            case INTEGER:
                return new IntegerDecoder();
            case SMALLINT:
                return new SmallintDecoder();
            case TINYINT:
                return new TinyintDecoder();
            case DOUBLE:
                return new DoubleDecoder();
            case REAL:
                return new RealDecoder();
            case BOOLEAN:
                return new BooleanDecoder();
            case ARRAY:
                return new ArrayDecoder(signature);
            case MAP:
                return new MapDecoder(signature);
            case ROW:
                return new RowDecoder(signature);
            case VARCHAR:
            case JSON:
            case TIME:
            case TIME_WITH_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_TIME_ZONE:
            case DATE:
            case INTERVAL_YEAR_TO_MONTH:
            case INTERVAL_DAY_TO_SECOND:
            case IPADDRESS:
            case UUID:
            case DECIMAL:
            case CHAR:
            case GEOMETRY:
            case SPHERICAL_GEOGRAPHY:
                return new StringDecoder();
            default:
                return new Base64Decoder();
        }
    }

    private static class BigIntegerDecoder
            implements TypeDecoder
    {
        @Override
        public Object decode(JsonParser parser)
                throws IOException
        {
            switch (parser.nextToken()) {
                case VALUE_NULL:
                    return null;
                case VALUE_NUMBER_INT:
                    return parser.getLongValue();
                case VALUE_STRING:
                    return Long.parseLong(parser.getText());
                default:
                    return illegalToken(parser);
            }
        }
    }

    private static class IntegerDecoder
            implements TypeDecoder
    {
        @Override
        public Object decode(JsonParser parser)
                throws IOException
        {
            switch (parser.nextToken()) {
                case VALUE_NULL:
                    return null;
                case VALUE_NUMBER_INT:
                    return parser.getIntValue();
                case VALUE_STRING:
                    return Integer.parseInt(parser.getText());
                default:
                    return illegalToken(parser);
            }
        }
    }

    private static class SmallintDecoder
            implements TypeDecoder
    {
        @Override
        public Object decode(JsonParser parser)
                throws IOException
        {
            switch (parser.nextToken()) {
                case VALUE_NULL:
                    return null;
                case VALUE_NUMBER_INT:
                    return parser.getShortValue();
                case VALUE_STRING:
                    return Short.parseShort(parser.getText());
                default:
                    return illegalToken(parser);
            }
        }
    }

    private static class TinyintDecoder
            implements TypeDecoder
    {
        @Override
        public Object decode(JsonParser parser)
                throws IOException
        {
            switch (parser.nextToken()) {
                case VALUE_NULL:
                    return null;
                case VALUE_NUMBER_INT:
                    return parser.getByteValue();
                case VALUE_STRING:
                    return Byte.parseByte(parser.getText());
                default:
                    return illegalToken(parser);
            }
        }
    }

    private static class DoubleDecoder
            implements TypeDecoder
    {
        @Override
        public Object decode(JsonParser parser)
                throws IOException
        {
            switch (parser.nextToken()) {
                case VALUE_NULL:
                    return null;
                case VALUE_NUMBER_FLOAT:
                    return parser.getDoubleValue();
                case VALUE_STRING:
                    return Double.parseDouble(parser.getText());
                default:
                    return illegalToken(parser);
            }
        }
    }

    private static class RealDecoder
            implements TypeDecoder
    {
        @Override
        public Object decode(JsonParser parser)
                throws IOException
        {
            switch (parser.nextToken()) {
                case VALUE_NULL:
                    return null;
                case VALUE_NUMBER_FLOAT:
                    return parser.getFloatValue();
                case VALUE_STRING:
                    return Float.parseFloat(parser.getText());
                default:
                    return illegalToken(parser);
            }
        }
    }

    private static class BooleanDecoder
            implements TypeDecoder
    {
        @Override
        public Object decode(JsonParser parser)
                throws IOException
        {
            switch (parser.nextToken()) {
                case VALUE_NULL:
                    return null;
                case VALUE_FALSE:
                    return false;
                case VALUE_TRUE:
                    return true;
                case VALUE_STRING:
                    return Boolean.parseBoolean(parser.getText());
                default:
                    return illegalToken(parser);
            }
        }
    }

    private static class StringDecoder
            implements TypeDecoder
    {
        @Override
        public Object decode(JsonParser parser)
                throws IOException
        {
            switch (parser.nextToken()) {
                case VALUE_NULL:
                    return null;
                case VALUE_STRING:
                    return parser.getText();
                default:
                    return illegalToken(parser);
            }
        }
    }

    private static class Base64Decoder
            implements TypeDecoder
    {
        @Override
        public Object decode(JsonParser parser)
                throws IOException
        {
            switch (parser.nextToken()) {
                case VALUE_NULL:
                    return null;
                case VALUE_STRING:
                    return Base64.getDecoder().decode(parser.getText());
                default:
                    return illegalToken(parser);
            }
        }
    }

    private static class ArrayDecoder
            implements TypeDecoder
    {
        private final TypeDecoder typeDecoder;

        public ArrayDecoder(ClientTypeSignature signature)
        {
            requireNonNull(signature, "signature is null");
            checkArgument(signature.getRawType().equals(ARRAY), "not an array type signature: %s", signature);
            this.typeDecoder = createTypeDecoder(signature.getArgumentsAsTypeSignatures().get(0));
        }

        @Override
        public Object decode(JsonParser parser)
                throws IOException
        {
            switch (parser.nextToken()) {
                case START_ARRAY:
                    break;
                case VALUE_NULL:
                    return null;
                default:
                    return illegalToken(parser);
            }
            List<Object> values = new ArrayList<>(); // nulls allowed
            while (true) {
                try {
                    values.add(typeDecoder.decode(parser));
                }
                catch (IllegalTokenException e) {
                    if (e.currentToken() == JsonToken.END_ARRAY) {
                        // We've read till the end of array
                        break;
                    }
                    throw e;
                }
            }
            return unmodifiableList(values);
        }
    }

    private static class MapDecoder
            implements TypeDecoder
    {
        private final TypeDecoder keyDecoder;
        private final TypeDecoder valueDecoder;

        public MapDecoder(ClientTypeSignature signature)
        {
            requireNonNull(signature, "signature is null");
            checkArgument(signature.getRawType().equals(MAP), "not a map type signature: %s", signature);
            this.keyDecoder = createTypeDecoder(signature.getArgumentsAsTypeSignatures().get(0));
            this.valueDecoder = createTypeDecoder(signature.getArgumentsAsTypeSignatures().get(1));
        }

        @Override
        public Object decode(JsonParser parser)
                throws IOException
        {
            Map<Object, Object> values = new HashMap<>();
            if (parser.nextToken() != START_ARRAY) {
                return illegalToken(parser);
            }

            try {
                while (true) {
                    values.put(keyDecoder.decode(parser), valueDecoder.decode(parser)); // nulls allowed
                }
            }
            catch (IllegalTokenException e) {
                if (e.currentToken() == JsonToken.END_ARRAY) {
                    // We've read till the end of map
                }
                else {
                    throw e;
                }
            }
            return unmodifiableMap(values);
        }
    }

    private static class RowDecoder
            implements TypeDecoder
    {
        private final TypeDecoder[] fieldDecoders;
        private final List<Optional<String>> fieldNames;

        private RowDecoder(ClientTypeSignature signature)
        {
            requireNonNull(signature, "signature is null");
            checkArgument(signature.getRawType().equals(ROW), "not a row type signature: %s", signature);
            fieldDecoders = new TypeDecoder[signature.getArguments().size()];
            ImmutableList.Builder<Optional<String>> fieldNames = ImmutableList.builderWithExpectedSize(fieldDecoders.length);

            int index = 0;
            for (ClientTypeSignatureParameter parameter : signature.getArguments()) {
                checkArgument(
                        parameter.getKind() == ClientTypeSignatureParameter.ParameterKind.NAMED_TYPE,
                        "Unexpected parameter [%s] for row type",
                        parameter);
                NamedClientTypeSignature namedTypeSignature = parameter.getNamedTypeSignature();
                fieldDecoders[index] = createTypeDecoder(namedTypeSignature.getTypeSignature());
                fieldNames.add(namedTypeSignature.getName());
                index++;
            }
            this.fieldNames = fieldNames.build();
        }

        @Override
        public Object decode(JsonParser parser)
                throws IOException
        {
            switch (parser.nextToken()) {
                case START_ARRAY:
                    break;
                case VALUE_NULL:
                    return null;
                default:
                    return illegalToken(parser);
            }
            Row.Builder row = Row.builderWithExpectedSize(fieldDecoders.length);
            for (int i = 0; i < fieldDecoders.length; i++) {
                row.addField(fieldNames.get(i), fieldDecoders[i].decode(parser));
            }
            verify(parser.nextToken() == END_ARRAY, "Expected end object, but got %s", parser.currentToken());
            return row.build();
        }
    }

    private static Object illegalToken(JsonParser parser)
    {
        throw new IllegalTokenException(parser);
    }

    private static class IllegalTokenException
            extends RuntimeException
    {
        private final JsonToken currentToken;

        public IllegalTokenException(JsonParser parser)
        {
            super(format("Unexpected token %s [location: %s]", parser.currentToken(), parser.currentLocation()));
            this.currentToken = parser.currentToken();
        }

        public JsonToken currentToken()
        {
            return currentToken;
        }
    }
}
