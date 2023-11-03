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

package io.crate.user;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.jetbrains.annotations.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

public class CreateUserRequest extends AcknowledgedRequest<CreateUserRequest> {

    private final String userName;
    @Nullable
    private final SecureHash secureHash;

    @Nullable
    private final String jwkUrl;
    @Nullable
    private final List<String> requiredClaims;

    public CreateUserRequest(String userName, @Nullable SecureHash attributes) {
        this(userName, attributes, null, null);
    }

    public CreateUserRequest(String userName, @Nullable SecureHash attributes, @Nullable String jwkUrl, @Nullable List<String> requiredClaims) {
        this.userName = userName;
        this.secureHash = attributes;
        this.jwkUrl = jwkUrl;
        this.requiredClaims = requiredClaims;
    }

    public CreateUserRequest(StreamInput in) throws IOException {
        super(in);
        userName = in.readString();
        secureHash = in.readOptionalWriteable(SecureHash::readFrom);
        // TODO RS: Version check here
        jwkUrl = in.readOptionalString();
        var claims = in.readOptionalStringArray();
        if (claims != null) {
            requiredClaims = List.of(claims);
        } else {
            requiredClaims = null;
        }
    }

    public String userName() {
        return userName;
    }

    @Nullable
    public SecureHash secureHash() {
        return secureHash;
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(userName);
        out.writeOptionalWriteable(secureHash);
        out.writeOptionalString(jwkUrl);
        out.writeOptionalStringArray(requiredClaims != null ? requiredClaims.toArray(new String[0]) : null);
    }
}
