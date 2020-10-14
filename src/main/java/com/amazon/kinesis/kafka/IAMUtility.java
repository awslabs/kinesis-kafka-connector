/*
 * Copyright 2020 Scott Kidder
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazon.kinesis.kafka;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.util.StringUtils;

/**
 * IAMUtility offers convenience functions for creating AWS IAM credential providers.
 *
 */
public class IAMUtility {

    /**
     * Create an IAM credentials provider.
     *
     * If a role ARN is provided, then an STS assume-role credentials provider is created. The
     * provider will automatically renew the assume-role session as needed.
     *
     * If the role ARN is empty or null, then the default AWS credentials provider is returned.
     *
     * @param regionName AWS region-name (must be non-empty when using assume-role).
     * @param roleARN IAM role ARN to assume (if non-empty then STS assume-role provider is returned).
     * @param roleExternalID Optional external-id string to scope access within AWS account.
     * @param roleSessionName Optional role session-name used for logging & debugging.
     * @param roleDurationSeconds Duration of the STS assume-role session (auto-renewed on expiration).
     * @return AWS credentials provider
     */
    static AWSCredentialsProvider createCredentials(String regionName, String roleARN, String roleExternalID, String roleSessionName, int roleDurationSeconds) {
        if (StringUtils.isNullOrEmpty(roleARN))
            return new DefaultAWSCredentialsProviderChain();

        // Use STS to assume a role if one was given
        final AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.standard()
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .withRegion(regionName)
                .build();

        STSAssumeRoleSessionCredentialsProvider.Builder providerBuilder = new STSAssumeRoleSessionCredentialsProvider.Builder(roleARN, roleSessionName).withStsClient(stsClient);
        if (!StringUtils.isNullOrEmpty(roleExternalID))
            providerBuilder = providerBuilder.withExternalId(roleExternalID);
        if (roleDurationSeconds > 0)
            providerBuilder = providerBuilder.withRoleSessionDurationSeconds(roleDurationSeconds);

        return providerBuilder.build();
    }
}
