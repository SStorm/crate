/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.user.metadata;

import static io.crate.testing.Asserts.assertThat;

import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Map;

import org.elasticsearch.common.settings.SecureString;

import io.crate.user.SecureHash;

public final class UserDefinitions {

    public static final Map<String, UsersMetadata.UserProperties> SINGLE_USER_ONLY = Collections.singletonMap("Arthur", new UsersMetadata.UserProperties(null, null, null));

    public static final Map<String, UsersMetadata.UserProperties> DUMMY_USERS = Map.of(
        "Ford",
        new UsersMetadata.UserProperties(getSecureHash("fords-password"), null, null),
        "Arthur",
        new UsersMetadata.UserProperties(getSecureHash("arthurs-password"), null, null)
    );

    private static SecureHash getSecureHash(String password) {
        SecureHash hash = null;
        try {
            hash = SecureHash.of(new SecureString(password.toCharArray()));
        } catch (GeneralSecurityException e) {
            // do nothing;
        }
        assertThat(hash).isNotNull();
        return hash;
    }
}
