/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.store.FsDirectoryFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

public final class SwitchFsDirectoryFactory extends FsDirectoryFactory {

    @Override
    protected Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings) throws IOException {
        Directory wrapped = new MMapDirectory(location, lockFactory);
        Set<String> preLoadExtensions = new HashSet<>(
            indexSettings.getValue(IndexModule.INDEX_STORE_PRE_LOAD_SETTING));
        wrapped = setPreload(wrapped, location, lockFactory, preLoadExtensions);
        return wrapped;
    }

    private static Directory setPreload(Directory directory, Path location, LockFactory lockFactory,
                                        Set<String> preLoadExtensions) throws IOException {
        if (preLoadExtensions.isEmpty()) {
            return directory;
        }

        ((MMapDirectory) directory).setPreload(true);
        if (preLoadExtensions.contains("*")) {
            return directory;
        }

        final int countName = location.getNameCount();
        final Path dataDirectory = Paths.get(location.getRoot().toString(),location.subpath(0, countName - 4).toString(), "indices.io");
        final Path locationSwitchfs = Paths.get(dataDirectory.toString(), location.subpath(countName - 3, countName).toString());
        Files.createDirectories(locationSwitchfs);

        NIOFSDirectory secondary = new NIOFSDirectory(locationSwitchfs, lockFactory);
        return new FileSwitchDirectory(preLoadExtensions, directory, secondary, true);
/*
            return new FileSwitchDirectory(preLoadExtensions, primary, directory, true) {
                @Override
                public String[] listAll() throws IOException {
                    // avoid listing twice
                    return primary.listAll();
                }
            };
 */
    }
}
