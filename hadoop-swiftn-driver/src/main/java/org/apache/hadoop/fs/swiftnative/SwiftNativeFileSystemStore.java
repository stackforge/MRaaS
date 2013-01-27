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

import static org.apache.hadoop.fs.swiftnative.NativeSwiftFileSystem.PATH_DELIMITER;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.swift.SwiftCredentials;
import org.apache.hadoop.fs.swift.SwiftException;
import org.codehaus.jackson.JsonNode;

import org.hpswift.service.Credentials;
import org.hpswift.service.Swift;


class SwiftNativeFileSystemStore implements NativeFileSystemStore {

    private SwiftCredentials swiftCredentials;
    private Swift swift;
    private String container;

    public void initialize(URI uri, Configuration conf) throws IOException {
        try {
            swiftCredentials = new SwiftCredentials();
            Credentials hpswiftCredentials = swiftCredentials.initialize(uri, conf);
            swift = new Swift(hpswiftCredentials);
        } catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new SwiftException(e);
        }
        container = uri.getHost();
    }

    public void storeFile(String key, File  file, byte[] md5Hash)
            throws IOException {

        BufferedInputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(file));
            try {
                swift.putStream(container, key, in, md5Hash);
            } catch (Exception e) {
                if (e.getCause() instanceof IOException) {
                    throw (IOException) e.getCause();
                }
                throw new SwiftException(e);
            }
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    public void storeEmptyFile(String key) throws IOException {
        try {
            swift.put(container, key, new byte[0]);
        } catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new SwiftException(e);
        }
    }

    public FileMetadata retrieveMetadata(String key) throws IOException {
        try {
            org.hpswift.service.SwiftMetadata metadata = swift.getMetaData(container, key);
            if (metadata == null) return null;
            return new FileMetadata(key,metadata.getLength(),metadata.getLastModified());
        } catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new SwiftException(e);
        }
    }

    public InputStream retrieve(String key) throws IOException {
        try {
            // returns null if not found
            return swift.getS(container,key);
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new SwiftException(e);
        }
    }

    public InputStream retrieve(String key, long byteRangeStart)
            throws IOException {
        try {
            // returns null if not found
            return swift.getS(container,key,byteRangeStart);
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new SwiftException(e);
        }
    }

    public PartialListing list(String prefix, int maxListingLength)
            throws IOException {
        return list(prefix, maxListingLength, null);
    }

    public PartialListing list(String prefix, int maxListingLength,
            String priorLastKey) throws IOException {

        return list(prefix, PATH_DELIMITER, maxListingLength, priorLastKey);
    }

    public PartialListing listAll(String prefix, int maxListingLength,
            String priorLastKey) throws IOException {

        return list(prefix, null, maxListingLength, priorLastKey);
    }

    private PartialListing list(String prefix, String delimiter,
            int maxListingLength, String priorLastKey) throws IOException {
        try {
            String lastKey = null;
            if (prefix.length() > 0 && !prefix.endsWith(PATH_DELIMITER)) {
                prefix += PATH_DELIMITER;
            }

            JsonNode list = swift.list(container, prefix, delimiter, maxListingLength, priorLastKey);

            int size = list.size();
            FileMetadata[] metadata = new FileMetadata[size];
            String[] keys = new String[size];

            for (int i =0; i < size; i++) {
                JsonNode item = list.get(i);
                String key = item.get("name").getTextValue();
                long len = item.get("bytes").getLongValue();
                String lm = item.get("last_modified").getTextValue();
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
                format.setTimeZone(TimeZone.getTimeZone("GMT"));
                Date date = format.parse(lm);
                long lastMod = date.getTime();
                keys[i] = key;
                metadata[i] = new FileMetadata(key,len,lastMod);
                lastKey = key;
            }

            return new PartialListing(lastKey,metadata,keys);
        } catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            e.printStackTrace();
            throw new SwiftException(e);
        }
    }

    public void delete(String key) throws IOException {
        try {
            swift.delete(container,key);
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new SwiftException(e);
        }
    }

    public void rename(String srcKey, String dstKey) throws IOException {
        try {
            swift.copy(container,srcKey,dstKey);
            swift.delete(container,srcKey);
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new SwiftException(e);
        }     
    }

    public void purge(String prefix) throws IOException {
        try {
            JsonNode list = swift.list(container, prefix, null, 0, null);
            int size = list.size();
            for (int i = 0; i < size; i++) {
                JsonNode item = list.get(i);
                String key = item.get("name").getTextValue();
                swift.delete(container, key);
            }
        } catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new SwiftException(e);
        }
    }

    public void dump() throws IOException {
        StringBuilder sb = new StringBuilder("Swift Native Filesystem, ");
        sb.append(container).append("\n");
        try {
            JsonNode list = swift.list(container, null, null, 0, null);
            int size = list.size();
            for (int i = 0; i < size; i++) {
                JsonNode item = list.get(i);
                String key = item.get("name").getTextValue();
                sb.append(key).append("\n");
            }
        } catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new SwiftException(e);
        }
    }

}
