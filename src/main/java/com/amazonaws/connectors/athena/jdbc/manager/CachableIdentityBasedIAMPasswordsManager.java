package com.amazonaws.connectors.athena.jdbc.manager;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.rds.auth.GetIamAuthTokenRequest;
import com.amazonaws.services.rds.auth.RdsIamAuthTokenGenerator;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.Tag;

import org.apache.arrow.util.VisibleForTesting;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * Caches rds credential tokens for a short timespan. Supports an ABAC type access control, where the principal tags
 * of the caller will be passed to the role assumed by the jdbc connector before generating a database token
*/
public class CachableIdentityBasedIAMPasswordsManager 
{
        private static final Logger logger = LoggerFactory.getLogger(CachableSecretsManagerAssumingRole.class);
        private static final Pattern USERNAME_PATTERN = Pattern.compile("(\\w+\\.\\w+\\@\\w+\\.\\w+)");
    
        private static final long MAX_CACHE_AGE_MS = 60_000;
        protected static final int MAX_CACHE_SIZE = 100;
    
        private final LinkedHashMap<String, CacheEntry> cache = new LinkedHashMap<>();
        private final AWSSecurityTokenService stsService;
        private final String assumeRoleArn;
    
        public CachableIdentityBasedIAMPasswordsManager(final AWSSecurityTokenService stsService, final String assumeRoleArn)
        {
            this.stsService = Validate.notNull(stsService, "Security token service must be assigned");
            this.assumeRoleArn = Validate.notBlank(assumeRoleArn, "We need the role which should be assumed when accessing the database secret");
        }
    
        /**
         * Generates a password using rds IAM token generator, assuming a role with principal tags based on the caller identity, first checking the cache. Newly fetched secrets are added to the cache.
         *
         * @param userName The username to generate a password for
         * @param endpoint Database endpoint
         * @param endpoint Database port
         * @param callerIdentity The identity of the executor/caller will be used to provide principal tags before assuming the role
         * @return IAM password token
         */
        public String getPassword(String userName, String endpoint, int port, FederatedIdentity callerIdentity)
        {
            String name = buildCacheEntryName(userName, endpoint, port, callerIdentity);
            CacheEntry cacheEntry = cache.get(name);
    
            if (cacheEntry == null || cacheEntry.getAge() > MAX_CACHE_AGE_MS) {   
                var roleSessionName = "";
                var matcher = USERNAME_PATTERN.matcher(callerIdentity.getArn());
                if (matcher.find()) {
                    roleSessionName = matcher.group(1);
                } else {
                    roleSessionName = UUID.randomUUID().toString();
                }
    
                logger.info("getPassword: Resolving IAM auth password for db user [{}] in database [{}:{}]. Querying identity [{}]. Will assume role [{}]", userName, endpoint, port, callerIdentity.getArn(), this.assumeRoleArn);

                var tmpCredentials = this.stsService.assumeRole(
                    new AssumeRoleRequest()
                        .withRoleArn(this.assumeRoleArn)
                        .withRoleSessionName(roleSessionName)
                        .withTags(
                            callerIdentity
                                .getPrincipalTags()
                                .entrySet()
                                .stream()
                                .map((tag) -> new Tag().withKey(tag.getKey()).withValue(tag.getValue())).toArray(Tag[]::new)
                        )
                ).getCredentials();
                
                var sessionCredentials = new BasicSessionCredentials(
                    tmpCredentials.getAccessKeyId(), 
                    tmpCredentials.getSecretAccessKey(), 
                    tmpCredentials.getSessionToken());
                
                var rds = RdsIamAuthTokenGenerator
                    .builder()
                    .credentials(new AWSStaticCredentialsProvider(sessionCredentials))
                    .region("eu-west-1")
                    .build();
    
                var authToken = rds.getAuthToken(
                    GetIamAuthTokenRequest.builder()
                        .hostname(endpoint)
                        .port(port)
                        .userName(userName)
                        .build());
    
                cacheEntry = new CacheEntry(name, authToken);
                evictCache(cache.size() >= MAX_CACHE_SIZE);
                cache.put(name, cacheEntry);
            }
    
            return cacheEntry.getValue();
        }
    
        private String buildCacheEntryName(String username, String endpoint, int port, FederatedIdentity callerIdentity) 
        {
            StringBuilder builder = new StringBuilder();
            builder.append(username);
            builder.append(endpoint);
            builder.append(port);
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
    