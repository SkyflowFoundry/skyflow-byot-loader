/**
 * Configuration Management
 *
 * Loads configuration from AWS Secrets Manager or environment variables.
 * Supports Skyflow Node.js SDK configuration format.
 */

const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');

/**
 * Load configuration from AWS Secrets Manager or environment
 *
 * @returns {Promise<Object>} Configuration object
 */
async function loadConfig() {
    console.log('Loading configuration...');

    // Try Secrets Manager first (if configured, this is the ONLY source)
    if (process.env.SECRETS_MANAGER_SECRET_NAME) {
        console.log('Loading from AWS Secrets Manager:', process.env.SECRETS_MANAGER_SECRET_NAME);
        console.log('IMPORTANT: Secrets Manager is configured - ignoring credentials.json and environment variables');
        return await loadFromSecretsManager();
    }

    // Try loading from local credentials.json file (only if Secrets Manager NOT configured)
    try {
        console.log('Loading from local credentials.json file');
        const fs = require('fs');
        const path = require('path');
        const credentialsPath = path.join(__dirname, 'credentials.json');
        const fileContent = fs.readFileSync(credentialsPath, 'utf8');
        const config = JSON.parse(fileContent);
        console.log('Successfully loaded credentials.json');
        return normalizeConfig(config);
    } catch (error) {
        console.log('Failed to load credentials.json:', error.message);
    }

    // Fallback to environment variables
    console.log('Loading from environment variables');
    return loadFromEnvironment();
}

/**
 * Load configuration from AWS Secrets Manager
 *
 * Creates a fresh client on each call to avoid stale credential issues.
 * Implements retry logic for transient failures.
 *
 * @returns {Promise<Object>} Configuration object
 */
async function loadFromSecretsManager() {
    const secretName = process.env.SECRETS_MANAGER_SECRET_NAME;
    const region = process.env.AWS_REGION || 'us-east-1';

    const maxRetries = 3;
    const baseDelay = 100; // milliseconds

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            // Create fresh client each time to avoid stale credentials
            const client = new SecretsManagerClient({
                region,
                maxAttempts: 3
            });
            const command = new GetSecretValueCommand({ SecretId: secretName });

            const data = await client.send(command);
            const config = JSON.parse(data.SecretString);

            console.log('Configuration loaded from Secrets Manager', {
                attempt,
                hasVaultUrl: !!(config.vaults?.vaultUrl || config.vault_url),
                hasBearerToken: !!config.bearer_token,
                hasDataTypeMappings: !!config.data_type_mappings,
                hasCredentials: !!config.credentials,
                hasVaults: !!config.vaults,
                vaultsIsObject: typeof config.vaults === 'object' && !Array.isArray(config.vaults)
            });

            return normalizeConfig(config);
        } catch (error) {
            const isLastAttempt = attempt === maxRetries;
            const isRetryable = error.name === 'InvalidSignatureException' ||
                               error.name === 'ExpiredTokenException' ||
                               error.name === 'InvalidTokenException' ||
                               error.$metadata?.httpStatusCode >= 500;

            console.error(`Failed to load from Secrets Manager (attempt ${attempt}/${maxRetries}):`, {
                error: error.message,
                errorName: error.name,
                isRetryable
            });

            if (isLastAttempt || !isRetryable) {
                throw new Error(`Failed to load configuration from Secrets Manager: ${error.message}`);
            }

            // Exponential backoff with jitter
            const delay = baseDelay * Math.pow(2, attempt - 1) + Math.random() * 100;
            console.log(`Retrying in ${Math.round(delay)}ms...`);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
}

/**
 * Load configuration from environment variables
 *
 * @returns {Object} Configuration object
 */
function loadFromEnvironment() {
    const vaults = [];
    const dataTypes = ['NAME', 'ID', 'DOB', 'SSN'];

    for (const dataType of dataTypes) {
        const vaultId = process.env[`VAULT_ID_${dataType}`];
        const clusterId = process.env[`CLUSTER_ID_${dataType}`];
        const table = process.env[`TABLE_${dataType}`];
        const column = process.env[`COLUMN_${dataType}`];

        if (vaultId && clusterId && table && column) {
            vaults.push({
                vaultId,
                clusterId,
                table,
                column,
                dataType
            });
            console.log(`Configured vault for ${dataType}: vault=${vaultId}, table=${table}`);
        }
    }

    const config = {
        credentials: {
            apiKey: process.env.SKYFLOW_API_KEY
        },
        vaults,
        logLevel: process.env.LOG_LEVEL || 'INFO'
    };

    return normalizeConfig(config);
}

/**
 * Normalize and validate configuration
 * Supports both old and new configuration formats
 *
 * @param {Object} config - Raw configuration
 * @returns {Object} Normalized configuration
 */
function normalizeConfig(config) {
    // Check if this is the OLD format (has vault_url or bearer_token)
    const isOldFormat = config.vault_url || config.bearer_token || config.data_type_mappings;

    if (isOldFormat) {
        console.log('Detected old configuration format - auto-converting to SDK format');
        config = convertOldToNewFormat(config);
    }

    // Validate credentials (support both service account and API key)
    if (!config.credentials) {
        throw new Error('Missing credentials in configuration');
    }

    const hasServiceAccount = config.credentials.clientID && config.credentials.privateKey;
    const hasApiKey = config.credentials.apiKey;

    if (!hasServiceAccount && !hasApiKey) {
        throw new Error('Credentials must have either service account fields (clientID, privateKey) or apiKey');
    }

    console.log('Credentials type:', hasServiceAccount ? 'Service Account (JWT)' : 'API Key');

    // Validate vaults structure (new format only)
    if (!config.vaults) {
        throw new Error('Missing vaults in configuration');
    }

    if (typeof config.vaults !== 'object' || Array.isArray(config.vaults)) {
        throw new Error('Invalid vaults configuration. Expected format: { vaultUrl: "...", definitions: [...] }');
    }

    // Extract vaultUrl and definitions
    const vaultUrl = config.vaults.vaultUrl;
    const vaultDefinitions = config.vaults.definitions;

    if (!vaultUrl) {
        throw new Error('Missing vaultUrl in vaults configuration');
    }
    if (!vaultDefinitions || !Array.isArray(vaultDefinitions)) {
        throw new Error('Missing or invalid definitions in vaults configuration');
    }

    // Extract clusterId from vaultUrl
    const clusterIdMatch = vaultUrl.match(/https:\/\/([^.]+)\./);
    if (!clusterIdMatch) {
        throw new Error(`Invalid vaultUrl format: ${vaultUrl}. Expected format: https://<clusterId>.vault.skyflowapis.com`);
    }
    const clusterId = clusterIdMatch[1];
    console.log('Extracted clusterId from vaultUrl:', clusterId);

    // Validate vault definitions
    if (vaultDefinitions.length === 0) {
        throw new Error('No vault definitions configured. At least one vault is required.');
    }

    // Validate each vault has required fields and inject clusterId
    for (const vault of vaultDefinitions) {
        if (!vault.vaultId) {
            throw new Error('Missing vaultId in vault configuration');
        }
        if (!vault.table) {
            throw new Error('Missing table in vault configuration');
        }
        if (!vault.column) {
            throw new Error('Missing column in vault configuration');
        }
        if (!vault.dataType) {
            throw new Error('Missing dataType in vault configuration');
        }
        // Inject clusterId from vaultUrl into each vault
        vault.clusterId = clusterId;
    }

    // Replace config.vaults with the array of definitions for internal use
    config.vaults = vaultDefinitions;

    // Create lookup map for fast access by data type
    config.vaultsByDataType = {};
    for (const vault of config.vaults) {
        const dataTypeUpper = vault.dataType.toUpperCase();
        config.vaultsByDataType[dataTypeUpper] = vault;
    }

    // Set default log level if not specified
    if (!config.logLevel) {
        config.logLevel = 'INFO';
    }

    // Validate tokenize batch size
    if (!config.tokenizeBatchSize) {
        throw new Error('Missing tokenizeBatchSize in configuration');
    }
    if (typeof config.tokenizeBatchSize !== 'number' || config.tokenizeBatchSize < 1) {
        throw new Error('tokenizeBatchSize must be a positive number');
    }

    // Validate tokenize max concurrency
    if (!config.tokenizeMaxConcurrency) {
        throw new Error('Missing tokenizeMaxConcurrency in configuration');
    }
    if (typeof config.tokenizeMaxConcurrency !== 'number' || config.tokenizeMaxConcurrency < 1) {
        throw new Error('tokenizeMaxConcurrency must be a positive number');
    }

    // Validate detokenize batch size
    if (!config.detokenizeBatchSize) {
        throw new Error('Missing detokenizeBatchSize in configuration');
    }
    if (typeof config.detokenizeBatchSize !== 'number' || config.detokenizeBatchSize < 1) {
        throw new Error('detokenizeBatchSize must be a positive number');
    }

    // Validate detokenize max concurrency
    if (!config.detokenizeMaxConcurrency) {
        throw new Error('Missing detokenizeMaxConcurrency in configuration');
    }
    if (typeof config.detokenizeMaxConcurrency !== 'number' || config.detokenizeMaxConcurrency < 1) {
        throw new Error('detokenizeMaxConcurrency must be a positive number');
    }

    console.log('Configuration validated successfully', {
        vaultCount: config.vaults.length,
        dataTypes: Object.keys(config.vaultsByDataType),
        logLevel: config.logLevel,
        tokenize: {
            batchSize: config.tokenizeBatchSize,
            maxConcurrency: config.tokenizeMaxConcurrency
        },
        detokenize: {
            batchSize: config.detokenizeBatchSize,
            maxConcurrency: config.detokenizeMaxConcurrency
        }
    });

    return config;
}

/**
 * Convert old configuration format to new SDK format
 *
 * @param {Object} oldConfig - Old format configuration
 * @returns {Object} New format configuration
 */
function convertOldToNewFormat(oldConfig) {
    console.log('Converting old config format to SDK format...', {
        hasBearerToken: !!oldConfig.bearer_token,
        bearerTokenValue: oldConfig.bearer_token ? oldConfig.bearer_token.substring(0, 10) + '...' : 'MISSING'
    });

    // Extract vaultUrl from old vault_url field
    let vaultUrl = oldConfig.vault_url;
    if (!vaultUrl) {
        throw new Error('vault_url is required in old configuration format');
    }
    console.log('Using vaultUrl from old config:', vaultUrl);

    // Convert credentials - support both bearer_token and bearerToken
    const bearerToken = oldConfig.bearer_token || oldConfig.bearerToken;
    if (!bearerToken) {
        console.error('CRITICAL: No bearer_token found in old config!', {
            configKeys: Object.keys(oldConfig)
        });
        throw new Error('bearer_token is required in old configuration format');
    }

    const credentials = {
        apiKey: bearerToken
    };

    console.log('Credentials converted', {
        hasApiKey: !!credentials.apiKey,
        apiKeyPrefix: credentials.apiKey ? credentials.apiKey.substring(0, 10) + '...' : 'MISSING'
    });

    // Convert data_type_mappings to vaults definitions (without clusterId - will be extracted from vaultUrl)
    const vaultDefinitions = [];
    const mappings = oldConfig.data_type_mappings || oldConfig.dataTypeMappings || {};

    for (const [dataType, mapping] of Object.entries(mappings)) {
        vaultDefinitions.push({
            vaultId: mapping.vault_id || mapping.vaultId,
            table: mapping.table,
            column: mapping.column,
            dataType: dataType.toUpperCase()
        });
    }

    const newConfig = {
        credentials,
        vaults: {
            vaultUrl,
            definitions: vaultDefinitions
        },
        logLevel: oldConfig.logLevel || 'INFO',
        // Legacy single values (fallback)
        batchSize: oldConfig.batch_size || oldConfig.batchSize || 100,
        maxConcurrency: oldConfig.max_concurrency || oldConfig.maxConcurrency || 20,
        // Separate tokenize/detokenize values (preferred)
        tokenizeBatchSize: oldConfig.tokenize_batch_size || oldConfig.tokenizeBatchSize || oldConfig.batchSize || 100,
        tokenizeMaxConcurrency: oldConfig.tokenize_max_concurrency || oldConfig.tokenizeMaxConcurrency || oldConfig.maxConcurrency || 20,
        detokenizeBatchSize: oldConfig.detokenize_batch_size || oldConfig.detokenizeBatchSize || oldConfig.batchSize || 100,
        detokenizeMaxConcurrency: oldConfig.detokenize_max_concurrency || oldConfig.detokenizeMaxConcurrency || oldConfig.maxConcurrency || 20
    };

    console.log('Old config converted successfully', {
        oldFields: Object.keys(oldConfig),
        newVaultCount: vaultDefinitions.length,
        hasCredentials: !!newConfig.credentials,
        hasApiKey: !!newConfig.credentials.apiKey,
        vaultUrl: newConfig.vaults.vaultUrl,
        ignoredFields: ['max_concurrency', 'max_retries', 'retry_delay_ms'].filter(f => oldConfig[f])
    });

    return newConfig;
}

module.exports = {
    loadConfig
};
