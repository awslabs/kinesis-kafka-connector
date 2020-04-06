package com.amazon.kinesis.kafka;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.util.StringUtils;

public class IAMUtility {
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
