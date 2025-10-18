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

    // Try Secrets Manager first
    if (process.env.SECRETS_MANAGER_SECRET_NAME) {
        console.log('Loading from AWS Secrets Manager:', process.env.SECRETS_MANAGER_SECRET_NAME);
        return await loadFromSecretsManager();
    }

    // Try loading from local credentials.json file
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
                hasVaultUrl: !!config.vault_url,
                hasBearerToken: !!config.bearer_token,
                hasDataTypeMappings: !!config.data_type_mappings,
                hasCredentials: !!config.credentials,
                hasVaults: !!config.vaults
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

    // Validate vaults
    if (!config.vaults || config.vaults.length === 0) {
        throw new Error('No vaults configured. At least one vault is required.');
    }

    // Validate each vault has required fields
    for (const vault of config.vaults) {
        if (!vault.vaultId) {
            throw new Error('Missing vaultId in vault configuration');
        }
        if (!vault.clusterId) {
            throw new Error('Missing clusterId in vault configuration');
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
    }

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

    // Extract cluster ID from vault_url if present
    let clusterId = null;
    if (oldConfig.vault_url) {
        const match = oldConfig.vault_url.match(/https:\/\/([^.]+)\./);
        if (match) {
            clusterId = match[1];
            console.log('Extracted clusterId from vault_url:', clusterId);
        }
    }

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

    // Convert data_type_mappings to vaults array
    const vaults = [];
    const mappings = oldConfig.data_type_mappings || oldConfig.dataTypeMappings || {};

    for (const [dataType, mapping] of Object.entries(mappings)) {
        vaults.push({
            vaultId: mapping.vault_id || mapping.vaultId,
            clusterId: clusterId, // Use extracted clusterId for all vaults
            table: mapping.table,
            column: mapping.column,
            dataType: dataType.toUpperCase()
        });
    }

    const newConfig = {
        credentials,
        vaults,
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
        newVaultCount: vaults.length,
        hasCredentials: !!newConfig.credentials,
        hasApiKey: !!newConfig.credentials.apiKey,
        batchSize: newConfig.batchSize,
        ignoredFields: ['max_concurrency', 'max_retries', 'retry_delay_ms'].filter(f => oldConfig[f])
    });

    return newConfig;
}

module.exports = {
    loadConfig
};
