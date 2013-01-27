/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.swiftnative;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;

/**
 * <p>
 * An abstraction for a key-based {@link File} store.
 * </p>
 */
interface NativeFileSystemStore {

    public void initialize(URI uri, Configuration conf) throws IOException;

    public void storeFile(String key, File file, byte[] md5Hash) throws IOException;
    public void storeEmptyFile(String key) throws IOException;

    public FileMetadata retrieveMetadata(String key) throws IOException;
    public InputStream retrieve(String key) throws IOException;
    public InputStream retrieve(String key, long byteRangeStart) throws IOException;

    public PartialListing list(String prefix, int maxListingLength) throws IOException;
    public PartialListing list(String prefix, int maxListingLength, String priorLastKey) throws IOException;    
    public PartialListing listAll(String prefix, int maxListingLength, String priorLastKey) throws IOException;

    public void delete(String key) throws IOException;

    public void rename(String srcKey, String dstKey) throws IOException;

    /**
     * Delete all keys with the given prefix. Used for testing.
     * @throws IOException
     */
    public void purge(String prefix) throws IOException;

    /**
     * Diagnostic method to dump state to the console.
     * @throws IOException
     */
    public void dump() throws IOException;
}
