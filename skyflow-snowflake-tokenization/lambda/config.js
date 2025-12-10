/**
 * Configuration Management
 *
 * Loads configuration from AWS Secrets Manager or environment variables.
 * Supports Skyflow Node.js SDK configuration format.
 */

const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');

// Singleton Secrets Manager client
// Reused across all config loads to leverage AWS SDK's built-in credential refresh
let secretsManagerClient = null;

/**
 * Load configuration from AWS Secrets Manager or environment
 *
 * @returns {Promise<Object>} Configuration object
 */
async function loadConfig() {
    // Try Secrets Manager first (if configured, this is the ONLY source)
    if (process.env.SECRETS_MANAGER_SECRET_NAME) {
        return await loadFromSecretsManager();
    }

    // Load from SKYFLOW_* environment variables

    // Reconstruct credentials object (JWT first, then API Key)
    let credentials;
    if (process.env.SKYFLOW_CLIENT_ID) {
        credentials = {
            clientID: process.env.SKYFLOW_CLIENT_ID,
            clientName: process.env.SKYFLOW_CLIENT_NAME,
            tokenURI: process.env.SKYFLOW_TOKEN_URI,
            keyID: process.env.SKYFLOW_KEY_ID,
            privateKey: process.env.SKYFLOW_PRIVATE_KEY,
            keyAlgorithm: process.env.SKYFLOW_KEY_ALGORITHM
        };
    } else if (process.env.SKYFLOW_API_KEY) {
        credentials = {
            apiKey: process.env.SKYFLOW_API_KEY
        };
    } else {
        throw new Error('Missing Skyflow credentials. Set either SKYFLOW_CLIENT_ID or SKYFLOW_API_KEY environment variables.');
    }

    // Parse vault definitions from JSON string
    const vaultDefinitions = JSON.parse(process.env.SKYFLOW_VAULT_DEFINITIONS || '[]');

    // Reconstruct config object
    const config = {
        credentials,
        vaults: {
            vaultUrl: process.env.SKYFLOW_VAULT_URL,
            definitions: vaultDefinitions
        },
        tokenizeBatchSize: parseInt(process.env.SKYFLOW_TOKENIZE_BATCH_SIZE, 10),
        tokenizeMaxConcurrency: parseInt(process.env.SKYFLOW_TOKENIZE_MAX_CONCURRENCY, 10),
        detokenizeBatchSize: parseInt(process.env.SKYFLOW_DETOKENIZE_BATCH_SIZE, 10),
        detokenizeMaxConcurrency: parseInt(process.env.SKYFLOW_DETOKENIZE_MAX_CONCURRENCY, 10),
        deleteBatchSize: parseInt(process.env.SKYFLOW_DELETE_BATCH_SIZE, 10),
        deleteMaxConcurrency: parseInt(process.env.SKYFLOW_DELETE_MAX_CONCURRENCY, 10),
        logLevel: process.env.SKYFLOW_LOG_LEVEL || 'INFO'
    };

    return normalizeConfig(config);
}

/**
 * Load configuration from AWS Secrets Manager
 *
 * Uses singleton client to leverage AWS SDK's built-in credential refresh.
 * Implements retry logic for transient failures.
 *
 * @returns {Promise<Object>} Configuration object
 */
async function loadFromSecretsManager() {
    const secretName = process.env.SECRETS_MANAGER_SECRET_NAME;
    const region = process.env.AWS_REGION || 'us-east-1';

    // Initialize singleton client on first use
    if (!secretsManagerClient) {
        secretsManagerClient = new SecretsManagerClient({
            region,
            maxAttempts: 1  // No retries - fail fast
        });
    }

    // No retries - fail fast on errors
    try {
        const command = new GetSecretValueCommand({ SecretId: secretName });

        const data = await secretsManagerClient.send(command);
        const config = JSON.parse(data.SecretString);

        return normalizeConfig(config);
    } catch (error) {
        console.error('Failed to load from Secrets Manager:', {
            error: error.message,
            errorName: error.name
        });

        throw new Error(`Failed to load configuration from Secrets Manager: ${error.message}`);
    }
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

    // Validate delete batch size
    if (!config.deleteBatchSize) {
        throw new Error('Missing deleteBatchSize in configuration');
    }
    if (typeof config.deleteBatchSize !== 'number' || config.deleteBatchSize < 1) {
        throw new Error('deleteBatchSize must be a positive number');
    }

    // Validate delete max concurrency
    if (!config.deleteMaxConcurrency) {
        throw new Error('Missing deleteMaxConcurrency in configuration');
    }
    if (typeof config.deleteMaxConcurrency !== 'number' || config.deleteMaxConcurrency < 1) {
        throw new Error('deleteMaxConcurrency must be a positive number');
    }

    return config;
}

/**
 * Convert old configuration format to new SDK format
 *
 * @param {Object} oldConfig - Old format configuration
 * @returns {Object} New format configuration
 */
function convertOldToNewFormat(oldConfig) {
    // Extract vaultUrl from old vault_url field
    let vaultUrl = oldConfig.vault_url;
    if (!vaultUrl) {
        throw new Error('vault_url is required in old configuration format');
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
        detokenizeMaxConcurrency: oldConfig.detokenize_max_concurrency || oldConfig.detokenizeMaxConcurrency || oldConfig.maxConcurrency || 20,
        // Delete values (for one-way tokenization)
        deleteBatchSize: oldConfig.delete_batch_size || oldConfig.deleteBatchSize || 25,
        deleteMaxConcurrency: oldConfig.delete_max_concurrency || oldConfig.deleteMaxConcurrency || 100
    };

    return newConfig;
}

module.exports = {
    loadConfig
};
