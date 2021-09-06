package com.amazonaws.connectors.athena.jdbc.manager;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.Tag;

import org.apache.arrow.util.VisibleForTesting;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Same as: @see com.amazonaws.athena.connector.lambda.security.CachableSecretsManager
 * Assumes a role before fetching the secret
 */
public class CachableSecretsManagerAssumingRole
{
    private static final Logger logger = LoggerFactory.getLogger(CachableSecretsManagerAssumingRole.class);
    private static final Pattern USERNAME_PATTERN = Pattern.compile("(\\w+\\.\\w+\\@\\w+\\.\\w+)");

    private static final long MAX_CACHE_AGE_MS = 60_000;
    protected static final int MAX_CACHE_SIZE = 100;

    private final LinkedHashMap<String, CacheEntry> cache = new LinkedHashMap<>();
    private final AWSSecurityTokenService stsService;
    private final String assumeRoleArn;

    public CachableSecretsManagerAssumingRole(final AWSSecurityTokenService stsService, final String assumeRoleArn)
    {
        this.stsService = Validate.notNull(stsService, "Security token service must be assigned");
        this.assumeRoleArn = Validate.notBlank(assumeRoleArn, "We need the role which should be assumed when generating the access token");
    }

    /**
     * Retrieves a secret from SecretsManager, first checking the cache. Newly fetched secrets are added to the cache.
     *
     * @param secretName The name of the secret to retrieve.
     * @return The value of the secret, throws if no such secret is found.
     */
    public String getSecret(String secretName, FederatedIdentity callerIdentity)
    {
        String name = buildCacheEntryName(secretName, callerIdentity);
        CacheEntry cacheEntry = cache.get(name);

        if (cacheEntry == null || cacheEntry.getAge() > MAX_CACHE_AGE_MS) {
            logger.info("getSecret: Resolving secret[{}] for identity [{}].", secretName, callerIdentity.getArn());

            var roleSessionName = "";
            var matcher = USERNAME_PATTERN.matcher(callerIdentity.getArn());
            if (matcher.find()) {
                roleSessionName = matcher.group(1);
            } else {
                roleSessionName = UUID.randomUUID().toString();
            }

            var tmpCredentials = this.stsService.assumeRole(
                new AssumeRoleRequest()
                    .withRoleArn(this.assumeRoleArn)
                    .withRoleSessionName(roleSessionName)
            ).getCredentials();
            
            var sessionCredentials = new BasicSessionCredentials(tmpCredentials.getAccessKeyId(), tmpCredentials.getSecretAccessKey(), tmpCredentials.getSessionToken());
            var secretsManager = AWSSecretsManagerClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
                .build();

            var secretValueResult = secretsManager.getSecretValue(new GetSecretValueRequest()
                    .withSecretId(secretName));

            cacheEntry = new CacheEntry(name, secretValueResult.getSecretString());
            evictCache(cache.size() >= MAX_CACHE_SIZE);
            cache.put(name, cacheEntry);
        }

        return cacheEntry.getValue();
    }

    private String buildCacheEntryName(String secretName, FederatedIdentity callerIdentity) 
    {
        StringBuilder builder = new StringBuilder();
        builder.append(secretName);
        builder.append(callerIdentity.getArn());
        callerIdentity.getPrincipalTags().forEach((key,value) -> { builder.append(key); builder.append(value); });
        return builder.toString();
    }

    private void evictCache(boolean force)
    {
        Iterator<Map.Entry<String, CacheEntry>> itr = cache.entrySet().iterator();
        int removed = 0;
        while (itr.hasNext()) {
            CacheEntry entry = itr.next().getValue();
            if (entry.getAge() > MAX_CACHE_AGE_MS) {
                itr.remove();
                removed++;
            }
        }

        if (removed == 0 && force) {
            //Remove the oldest since we found no expired entries
            itr = cache.entrySet().iterator();
            if (itr.hasNext()) {
                itr.next();
                itr.remove();
            }
        }
    }

    @VisibleForTesting
    protected void addCacheEntry(String name, String value, long createTime)
    {
        cache.put(name, new CacheEntry(name, value, createTime));
    }

    private class CacheEntry
    {
        private final String name;
        private final String value;
        private final long createTime;

        public CacheEntry(String name, String value)
        {
            this.value = value;
            this.name = name;
            this.createTime = System.currentTimeMillis();
        }

        public CacheEntry(String name, String value, long createTime)
        {
            this.value = value;
            this.name = name;
            this.createTime = createTime;
        }

        public String getValue()
        {
            return value;
        }

        public long getAge()
        {
            return System.currentTimeMillis() - createTime;
        }
    }
}
