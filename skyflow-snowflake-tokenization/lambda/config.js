/**
 * Configuration Module
 *
 * Loads configuration from AWS Secrets Manager or environment variables.
 * Supports credential rotation without code redeployment.
 */

const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');

/**
 * Load configuration from AWS Secrets Manager or environment variables
 *
 * @returns {Promise<Object>} Configuration object
 */
async function loadConfig() {
    const useSecretsManager = process.env.USE_SECRETS_MANAGER === 'true';

    if (useSecretsManager) {
        console.log('Loading configuration from AWS Secrets Manager');
        return await loadFromSecretsManager();
    }

    console.log('Loading configuration from environment variables');
    return loadFromEnvironment();
}

/**
 * Load configuration from AWS Secrets Manager
 *
 * @returns {Promise<Object>} Configuration object
 * @private
 */
async function loadFromSecretsManager() {
    const secretName = process.env.SECRET_NAME || 'skyflow-tokenization-config';
    const region = process.env.AWS_REGION || 'us-east-1';

    console.log(`Fetching secret: ${secretName} from region: ${region}`);

    try {
        const client = new SecretsManagerClient({ region });
        const command = new GetSecretValueCommand({ SecretId: secretName });
        const response = await client.send(command);

        if (!response.SecretString) {
            throw new Error(`Secret ${secretName} has no string value`);
        }

        const config = JSON.parse(response.SecretString);

        // Validate required fields
        validateConfig(config);

        console.log('Successfully loaded configuration from Secrets Manager', {
            vaultUrl: config.vault_url || config.vaultUrl,
            batchSize: config.batch_size || config.batchSize,
            maxConcurrency: config.max_concurrency || config.maxConcurrency
        });

        // Normalize field names (support both snake_case and camelCase)
        return normalizeConfig(config);

    } catch (error) {
        console.error('Failed to load configuration from Secrets Manager:', error);
        throw new Error(`Failed to load configuration from Secrets Manager: ${error.message}`);
    }
}

/**
 * Load configuration from environment variables
 *
 * @returns {Object} Configuration object
 * @private
 */
function loadFromEnvironment() {
    const config = {
        vaultUrl: process.env.VAULT_URL,
        bearerToken: process.env.BEARER_TOKEN,
        batchSize: parseInt(process.env.BATCH_SIZE || '100'),
        maxConcurrency: parseInt(process.env.MAX_CONCURRENCY || '20'),
        maxRetries: parseInt(process.env.MAX_RETRIES || '3'),
        retryDelay: parseFloat(process.env.RETRY_DELAY || '1.0'),
        dataTypeMappings: {}
    };

    // Load data type mappings from environment variables
    const dataTypes = ['NAME', 'ID', 'DOB', 'SSN'];
    for (const dataType of dataTypes) {
        const vaultId = process.env[`VAULT_ID_${dataType}`];
        const table = process.env[`TABLE_${dataType}`];
        const column = process.env[`COLUMN_${dataType}`];

        if (vaultId && table && column) {
            config.dataTypeMappings[dataType] = {
                vaultId: vaultId,
                table: table,
                column: column
            };
        }
    }

    validateConfig(config);

    console.log('Successfully loaded configuration from environment variables', {
        vaultUrl: config.vaultUrl,
        batchSize: config.batchSize,
        maxConcurrency: config.maxConcurrency
    });

    return config;
}

/**
 * Normalize configuration field names (snake_case to camelCase)
 *
 * @param {Object} config - Raw configuration
 * @returns {Object} Normalized configuration
 * @private
 */
function normalizeConfig(config) {
    return {
        vaultUrl: config.vault_url || config.vaultUrl,
        bearerToken: config.bearer_token || config.bearerToken,
        batchSize: config.batch_size || config.batchSize || 100,
        maxConcurrency: config.max_concurrency || config.maxConcurrency || 20,
        maxRetries: config.max_retries || config.maxRetries || 3,
        retryDelay: config.retry_delay_ms ? config.retry_delay_ms / 1000 : (config.retryDelay || 1.0),
        dataTypeMappings: normalizeDataTypeMappings(config.data_type_mappings || config.dataTypeMappings || {})
    };
}

/**
 * Normalize data type mappings field names
 *
 * @param {Object} mappings - Raw mappings
 * @returns {Object} Normalized mappings
 * @private
 */
function normalizeDataTypeMappings(mappings) {
    const normalized = {};

    for (const [dataType, mapping] of Object.entries(mappings)) {
        normalized[dataType] = {
            vaultId: mapping.vault_id || mapping.vaultId,
            table: mapping.table,
            column: mapping.column
        };
    }

    return normalized;
}

/**
 * Validate configuration
 *
 * @param {Object} config - Configuration to validate
 * @throws {Error} If configuration is invalid
 * @private
 */
function validateConfig(config) {
    const vaultUrl = config.vault_url || config.vaultUrl;
    const bearerToken = config.bearer_token || config.bearerToken;

    if (!vaultUrl) {
        throw new Error('vault_url is required in configuration');
    }

    if (!bearerToken) {
        throw new Error('bearer_token is required in configuration');
    }

    const batchSize = config.batch_size || config.batchSize;
    if (batchSize && (batchSize <= 0 || batchSize > 200)) {
        throw new Error('batch_size must be between 1 and 200');
    }

    const maxConcurrency = config.max_concurrency || config.maxConcurrency;
    if (maxConcurrency && maxConcurrency <= 0) {
        throw new Error('max_concurrency must be > 0');
    }
}

module.exports = {
    loadConfig
};
