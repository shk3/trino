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
package io.trino.filesystem.s3;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import io.airlift.log.Logging;
import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestS3FileSystem
        extends AbstractTestTrinoFileSystem
{
    private S3FileSystemFactory fileSystemFactory;
    private TrinoFileSystem fileSystem;

    @BeforeAll
    final void init()
    {
        Logging.initialize();

        initEnvironment();
        fileSystemFactory = createS3FileSystemFactory();
        fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
    }

    @AfterAll
    final void cleanup()
    {
        fileSystem = null;
        fileSystemFactory.destroy();
        fileSystemFactory = null;
    }

    @Override
    protected final boolean isHierarchical()
    {
        return false;
    }

    @Override
    protected final TrinoFileSystem getFileSystem()
    {
        return fileSystem;
    }

    @Override
    protected final Location getRootLocation()
    {
        return Location.of("s3://%s/".formatted(bucket()));
    }

    @Override
    protected final boolean supportsRenameFile()
    {
        return false;
    }

    @Override
    protected boolean supportsPreSignedUri()
    {
        return true;
    }

    @Override
    protected final void verifyFileSystemIsEmpty()
    {
        try (S3Client client = createS3Client()) {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucket())
                    .build();
            assertThat(client.listObjectsV2(request).contents()).isEmpty();
        }
    }

    protected void initEnvironment() {}

    protected abstract String bucket();

    protected abstract S3FileSystemFactory createS3FileSystemFactory();

    protected abstract S3Client createS3Client();

    protected List<FileEntry> toList(FileIterator fileIterator)
            throws IOException
    {
        ImmutableList.Builder<FileEntry> list = ImmutableList.builder();
        while (fileIterator.hasNext()) {
            list.add(fileIterator.next());
        }
        return list.build();
    }

    /**
     * Tests same things as {@link #testFileWithTrailingWhitespace()} but with setup and assertions using {@link S3Client}.
     */
    @Test
    void testFileWithTrailingWhitespaceAgainstNativeClient()
            throws IOException
    {
        try (S3Client s3Client = createS3Client()) {
            String key = "foo/bar with whitespace ";
            byte[] contents = "abc foo bar".getBytes(UTF_8);
            s3Client.putObject(
                    request -> request.bucket(bucket()).key(key),
                    RequestBody.fromBytes(contents.clone()));
            try {
                // Verify listing
                List<FileEntry> listing = toList(fileSystem.listFiles(getRootLocation().appendPath("foo")));
                assertThat(listing).hasSize(1);
                FileEntry fileEntry = getOnlyElement(listing);
                assertThat(fileEntry.location()).isEqualTo(getRootLocation().appendPath(key));
                assertThat(fileEntry.length()).isEqualTo(contents.length);

                // Verify reading
                TrinoInputFile inputFile = fileSystem.newInputFile(fileEntry.location());
                assertThat(inputFile.exists()).as("exists").isTrue();
                try (TrinoInputStream inputStream = inputFile.newStream()) {
                    byte[] bytes = ByteStreams.toByteArray(inputStream);
                    assertThat(bytes).isEqualTo(contents);
                }

                // Verify writing
                byte[] newContents = "bar bar baz new content".getBytes(UTF_8);
                fileSystem.newOutputFile(fileEntry.location()).createOrOverwrite(newContents);
                assertThat(s3Client.getObjectAsBytes(request -> request.bucket(bucket()).key(key)).asByteArray())
                        .isEqualTo(newContents);

                // Verify deleting
                fileSystem.deleteFile(fileEntry.location());
                assertThat(inputFile.exists()).as("exists after delete").isFalse();
            }
            finally {
                s3Client.deleteObject(delete -> delete.bucket(bucket()).key(key));
            }
        }
    }

    @Test
    void testExistingDirectoryWithTrailingSlash()
            throws IOException
    {
        try (S3Client s3Client = createS3Client(); Closer closer = Closer.create()) {
            String key = "data/dir/";
            createDirectory(closer, s3Client, key);
            assertThat(fileSystem.listFiles(getRootLocation()).hasNext()).isFalse();

            Location data = getRootLocation().appendPath("data/");
            assertThat(fileSystem.listDirectories(getRootLocation())).containsExactly(data);
            assertThat(fileSystem.listDirectories(data)).containsExactly(data.appendPath("dir/"));

            fileSystem.deleteDirectory(data);
            assertThat(fileSystem.listDirectories(getRootLocation())).isEmpty();

            fileSystem.deleteDirectory(getRootLocation());
            assertThat(fileSystem.listDirectories(getRootLocation())).isEmpty();
        }
    }

    @Test
    void testDeleteEmptyDirectoryWithDeepHierarchy()
            throws IOException
    {
        try (S3Client s3Client = createS3Client(); Closer closer = Closer.create()) {
            createDirectory(closer, s3Client, "deep/dir");
            createBlob(closer, "deep/dir/file1.txt");
            createBlob(closer, "deep/dir/file2.txt");
            createBlob(closer, "deep/dir/file3.txt");
            createDirectory(closer, s3Client, "deep/dir/dir4");
            createBlob(closer, "deep/dir/dir4/file5.txt");

            assertThat(fileSystem.listFiles(getRootLocation()).hasNext()).isTrue();

            Location directory = getRootLocation().appendPath("deep/dir/");
            assertThat(fileSystem.listDirectories(getRootLocation().appendPath("deep"))).containsExactly(directory);
            assertThat(fileSystem.listDirectories(directory)).containsExactly(getRootLocation().appendPath("deep/dir/dir4/"));

            fileSystem.deleteDirectory(directory);
            assertThat(fileSystem.listDirectories(getRootLocation().appendPath("deep"))).isEmpty();
            assertThat(fileSystem.listDirectories(getRootLocation())).isEmpty();
            assertThat(fileSystem.listFiles(getRootLocation()).hasNext()).isFalse();
        }
    }

    protected Location createDirectory(Closer closer, S3Client s3Client, String path)
    {
        Location location = createLocation(path);
        closer.register(new TempDirectory(s3Client, path)).create();
        return location;
    }

    protected class TempDirectory
            implements Closeable
    {
        private final S3Client s3Client;
        private final String path;

        public TempDirectory(S3Client s3Client, String path)
        {
            this.s3Client = requireNonNull(s3Client, "s3Client is null");
            String key = requireNonNull(path, "path is null");
            this.path = key.endsWith("/") ? key : key + "/";
        }

        public void create()
        {
            s3Client.putObject(request -> request.bucket(bucket()).key(path), RequestBody.empty());
        }

        @Override
        public void close()
                throws IOException
        {
            s3Client.deleteObject(delete -> delete.bucket(bucket()).key(path));
        }
    }
}
