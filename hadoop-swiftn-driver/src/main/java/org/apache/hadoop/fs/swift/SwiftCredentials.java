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

package org.apache.hadoop.fs.swift;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.hpswift.service.Credentials;

/**
 * <p>
 * Extracts OpenStack credentials from the configuration.
 * </p>
 */
public class SwiftCredentials {

    private String secretKey;
    private String accessKey; 
    private String tenantId;
    private String authUri;

    /**
     * @throws IllegalArgumentException if credentials for Swift cannot be
     * determined.
     */
    public Credentials initialize(URI uri, Configuration conf) {

        String scheme = uri.getScheme();
        String accessKeyProperty = String.format("fs.%s.openstackAccessKeyId", scheme);
        String secretKeyProperty = String.format("fs.%s.openstackSecretKey", scheme);
        String tenantIdProperty = String.format("fs.%s.openStackTenantId", scheme);
        String authUriProperty = String.format("fs.%s.openstackauthUri", scheme);

        accessKey = conf.get(accessKeyProperty);
        secretKey = conf.get(secretKeyProperty);
        tenantId = conf.get(tenantIdProperty);
        authUri = conf.get(authUriProperty);


        if (accessKey == null || secretKey == null || tenantId == null || authUri == null) {
            System.out.println("**" + accessKey +  secretKey + tenantId + authUri);

            throw new IllegalArgumentException("OpenStack Swift must specify all of: secretKey, accessKey, tentantId, authUri");
        }
        Credentials hpCredentials = new Credentials(accessKey, secretKey, tenantId, authUri);
        return hpCredentials;
    }

    public String getSecretKey(){
        return secretKey;
    }
    
    public String getAccessKey(){
        return accessKey;
    }
    
    public String getTenantId(){
        return tenantId;
    }
    
    public String getAuthUri(){
        return authUri;
    }
}
