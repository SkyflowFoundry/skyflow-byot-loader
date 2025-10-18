/**
 * Skyflow SDK Client Wrapper
 *
 * Handles tokenization and detokenization using official Skyflow Node.js SDK v2.0.0
 */

const { Skyflow, LogLevel, RedactionType, InsertRequest, InsertOptions, DetokenizeRequest, DetokenizeOptions } = require('skyflow-node');

/**
 * Skyflow Client for tokenization and detokenization
 */
class SkyflowClient {
    /**
     * @param {Object} config - Configuration
     * @param {Object} config.credentials - Skyflow credentials
     * @param {string} config.credentials.apiKey - API key (bearer token)
     * @param {Array} config.vaults - Array of vault configurations
     * @param {Object} config.vaultsByDataType - Vault lookup by data type
     * @param {string} config.logLevel - Log level (INFO, ERROR, WARN, DEBUG)
     */
    constructor(config) {
        this.config = config;
        this.vaultsByDataType = config.vaultsByDataType;

        // Separate batch size and concurrency for tokenize vs detokenize
        this.TOKENIZE_BATCH_SIZE = config.tokenizeBatchSize;
        this.TOKENIZE_MAX_CONCURRENCY = config.tokenizeMaxConcurrency;
        this.DETOKENIZE_BATCH_SIZE = config.detokenizeBatchSize;
        this.DETOKENIZE_MAX_CONCURRENCY = config.detokenizeMaxConcurrency;

        // Map log level string to SDK enum
        const logLevelMap = {
            'ERROR': LogLevel.ERROR,
            'WARN': LogLevel.WARN,
            'INFO': LogLevel.INFO,
            'DEBUG': LogLevel.DEBUG
        };

        // Initialize SDK clients for each vault
        this.skyflowClients = {};

        for (const vault of config.vaults) {
            // SDK expects credentials wrapped in specific format
            // For service account: { credentialsString: JSON.stringify(serviceAccountObject) }
            // For API key: { apiKey: 'key' }
            let credentials;
            if (config.credentials.apiKey) {
                // API key format
                credentials = { apiKey: config.credentials.apiKey };
            } else {
                // Service account format - SDK needs credentialsString
                credentials = { credentialsString: JSON.stringify(config.credentials) };
            }

            const vaultConfig = {
                vaultId: vault.vaultId,
                clusterId: vault.clusterId,
                env: 'PROD',
                credentials: credentials
            };

            const skyflowConfig = {
                vaultConfigs: [vaultConfig],
                logLevel: logLevelMap[config.logLevel] || LogLevel.INFO
            };

            console.log(`Initializing Skyflow SDK for ${vault.dataType}`, {
                vaultId: vault.vaultId,
                clusterId: vault.clusterId,
                credentialType: config.credentials.apiKey ? 'API Key' : 'Service Account'
            });

            this.skyflowClients[vault.dataType] = new Skyflow(skyflowConfig);
        }

        console.log('SkyflowClient initialized with SDK', {
            vaultCount: config.vaults.length,
            dataTypes: Object.keys(this.vaultsByDataType),
            logLevel: config.logLevel,
            tokenize: {
                batchSize: this.TOKENIZE_BATCH_SIZE,
                maxConcurrency: this.TOKENIZE_MAX_CONCURRENCY
            },
            detokenize: {
                batchSize: this.DETOKENIZE_BATCH_SIZE,
                maxConcurrency: this.DETOKENIZE_MAX_CONCURRENCY
            }
        });
    }

    /**
     * Tokenize a batch of values
     * @param {Array} values - Array of {rowIndex, value, vaultId, table, column, dataType}
     * @returns {Promise<Array>} Array of {rowIndex, token, error}
     */
    async tokenizeBatch(values) {
        if (!values || values.length === 0) {
            return [];
        }

        // Group by data type (each data type may use different vault)
        const groupedByDataType = {};
        for (const item of values) {
            const dataType = item.dataType;
            if (!groupedByDataType[dataType]) {
                groupedByDataType[dataType] = [];
            }
            groupedByDataType[dataType].push(item);
        }

        console.log(`Tokenizing ${values.length} values across ${Object.keys(groupedByDataType).length} data types`);

        // Process each data type group SEQUENTIALLY (with parallelization within each)
        const allResults = [];
        for (const [dataType, groupValues] of Object.entries(groupedByDataType)) {
            const results = await this._tokenizeDataTypeGroup(dataType, groupValues);
            allResults.push(...results);
        }

        // No need to sort - results are already in order from sequential processing

        console.log(`Tokenization complete: ${allResults.length} results`);
        return allResults;
    }

    /**
     * Detokenize a batch of tokens
     * @param {Array} tokens - Array of {rowIndex, token, vaultId, dataType}
     * @returns {Promise<Array>} Array of {rowIndex, value, error}
     */
    async detokenizeBatch(tokens) {
        if (!tokens || tokens.length === 0) {
            return [];
        }

        // Group by data type
        const groupedByDataType = {};
        for (const item of tokens) {
            const dataType = item.dataType;
            if (!groupedByDataType[dataType]) {
                groupedByDataType[dataType] = [];
            }
            groupedByDataType[dataType].push(item);
        }

        console.log(`Detokenizing ${tokens.length} tokens across ${Object.keys(groupedByDataType).length} data types`);

        // Process each data type group SEQUENTIALLY (with parallelization within each)
        const allResults = [];
        for (const [dataType, groupTokens] of Object.entries(groupedByDataType)) {
            const results = await this._detokenizeDataTypeGroup(dataType, groupTokens);
            allResults.push(...results);
        }

        // No need to sort - results are already in order from sequential processing

        console.log(`Detokenization complete: ${allResults.length} results`);
        return allResults;
    }

    /**
     * Tokenize a group of values for a specific data type
     * @private
     */
    async _tokenizeDataTypeGroup(dataType, values) {
        const vault = this.vaultsByDataType[dataType];
        if (!vault) {
            console.error(`No vault configured for data type: ${dataType}`);
            return values.map(v => ({
                rowIndex: v.rowIndex,
                token: null,
                error: `No vault configured for data type: ${dataType}`
            }));
        }

        const client = this.skyflowClients[dataType];
        const { table, column, vaultId } = vault;

        // Split into batches if needed
        if (values.length > this.TOKENIZE_BATCH_SIZE) {
            console.log(`Splitting ${values.length} values into batches of ${this.TOKENIZE_BATCH_SIZE} for ${dataType}`);

            // Create batches
            const batches = [];
            for (let i = 0; i < values.length; i += this.TOKENIZE_BATCH_SIZE) {
                batches.push(values.slice(i, i + this.TOKENIZE_BATCH_SIZE));
            }

            console.log(`Processing ${batches.length} batches with max concurrency of ${this.TOKENIZE_MAX_CONCURRENCY}`);

            // Process batches in parallel with concurrency control
            const allResults = [];
            for (let i = 0; i < batches.length; i += this.TOKENIZE_MAX_CONCURRENCY) {
                const batchGroup = batches.slice(i, i + this.TOKENIZE_MAX_CONCURRENCY);
                const groupPromises = batchGroup.map(batch =>
                    this._tokenizeBatch(dataType, batch, client, vaultId, table, column)
                );
                const groupResults = await Promise.all(groupPromises);
                allResults.push(...groupResults.flat());
            }

            return allResults;
        }

        return await this._tokenizeBatch(dataType, values, client, vaultId, table, column);
    }

    /**
     * Tokenize a single batch (internal helper)
     * @private
     */
    async _tokenizeBatch(dataType, values, client, vaultId, table, column) {
        try {
            // Prepare insert data for SDK
            const insertData = values.map(item => ({
                [column]: item.value
            }));

            console.log(`Tokenizing ${values.length} values for ${dataType} (vault: ${vaultId}, table: ${table})`);

            // Use SDK's insert with upsert and tokenization
            const insertRequest = new InsertRequest(table, insertData);
            const insertOptions = new InsertOptions();
            insertOptions.setReturnTokens(true); // Return tokens in response
            insertOptions.setUpsertColumn(column); // Upsert on column
            insertOptions.setContinueOnError(true); // Continue on individual errors

            const startTime = Date.now();
            const response = await client.vault(vaultId).insert(insertRequest, insertOptions);
            const elapsed = Date.now() - startTime;

            console.log(`SDK insert completed in ${elapsed}ms for ${dataType}`);
            console.log('SDK Response:', JSON.stringify(response, null, 2));

            // Parse SDK response
            return this._parseInsertResponse(values, response, column);

        } catch (error) {
            console.error(`Tokenization failed for ${dataType}:`, error.message);
            return values.map(v => ({
                rowIndex: v.rowIndex,
                token: null,
                error: error.message
            }));
        }
    }

    /**
     * Detokenize a group of tokens for a specific data type
     * @private
     */
    async _detokenizeDataTypeGroup(dataType, tokens) {
        const vault = this.vaultsByDataType[dataType];
        if (!vault) {
            console.error(`No vault configured for data type: ${dataType}`);
            return tokens.map(t => ({
                rowIndex: t.rowIndex,
                value: null,
                error: `No vault configured for data type: ${dataType}`
            }));
        }

        const client = this.skyflowClients[dataType];
        const { vaultId } = vault;

        // Split into batches if needed
        if (tokens.length > this.DETOKENIZE_BATCH_SIZE) {
            console.log(`Splitting ${tokens.length} tokens into batches of ${this.DETOKENIZE_BATCH_SIZE} for ${dataType}`);

            // Create batches
            const batches = [];
            for (let i = 0; i < tokens.length; i += this.DETOKENIZE_BATCH_SIZE) {
                batches.push(tokens.slice(i, i + this.DETOKENIZE_BATCH_SIZE));
            }

            console.log(`Processing ${batches.length} batches with max concurrency of ${this.DETOKENIZE_MAX_CONCURRENCY}`);

            // Process batches in parallel with concurrency control
            const allResults = [];
            for (let i = 0; i < batches.length; i += this.DETOKENIZE_MAX_CONCURRENCY) {
                const batchGroup = batches.slice(i, i + this.DETOKENIZE_MAX_CONCURRENCY);
                const groupPromises = batchGroup.map(batch =>
                    this._detokenizeBatch(dataType, batch, client, vaultId)
                );
                const groupResults = await Promise.all(groupPromises);
                allResults.push(...groupResults.flat());
            }

            return allResults;
        }

        return await this._detokenizeBatch(dataType, tokens, client, vaultId);
    }

    /**
     * Detokenize a single batch (internal helper)
     * @private
     */
    async _detokenizeBatch(dataType, tokens, client, vaultId) {
        try {
            console.log(`Detokenizing ${tokens.length} tokens for ${dataType} (vault: ${vaultId})`);

            // Prepare detokenize request for SDK
            const detokenizeData = tokens.map(item => ({
                token: item.token,
                redactionType: RedactionType.PLAIN_TEXT
            }));

            const detokenizeRequest = new DetokenizeRequest(detokenizeData);
            const detokenizeOptions = new DetokenizeOptions();
            detokenizeOptions.setContinueOnError(true);

            const startTime = Date.now();
            const response = await client.vault(vaultId).detokenize(detokenizeRequest, detokenizeOptions);
            const elapsed = Date.now() - startTime;

            console.log(`SDK detokenize completed in ${elapsed}ms for ${dataType}`);

            // Parse SDK response
            return this._parseDetokenizeResponse(tokens, response);

        } catch (error) {
            console.error(`Detokenization failed for ${dataType}:`, error.message);
            return tokens.map(t => ({
                rowIndex: t.rowIndex,
                value: null,
                error: error.message
            }));
        }
    }

    /**
     * Parse SDK insert response
     * @private
     */
    _parseInsertResponse(values, response, column) {
        const results = [];

        // SDK response format: { insertedFields: [{skyflowId, column_name: token_value}], errors: [...] }
        // The token is returned as the field value itself, not in a nested 'tokens' object
        const insertedFields = response.insertedFields || [];
        const errors = response.errors || [];

        console.log(`Parsing insert response: ${insertedFields.length} insertedFields, ${errors.length} errors`);

        for (let i = 0; i < values.length; i++) {
            const item = values[i];

            // Check if this index has an error
            const errorForIndex = errors.find(e => e.index === i);
            if (errorForIndex) {
                console.log(`Error at index ${i}:`, errorForIndex);
                results.push({
                    rowIndex: item.rowIndex,
                    token: null,
                    error: errorForIndex.error || 'Unknown error'
                });
                continue;
            }

            // Get token from insertedFields
            // The token is the field value itself (e.g., insertedFields[i][column])
            const inserted = insertedFields[i];
            console.log(`Index ${i} inserted:`, JSON.stringify(inserted));

            if (inserted && inserted[column]) {
                results.push({
                    rowIndex: item.rowIndex,
                    token: inserted[column],  // Token is the field value directly
                    error: null
                });
            } else {
                console.log(`No token found at index ${i}. Column: ${column}, inserted:`, inserted);
                results.push({
                    rowIndex: item.rowIndex,
                    token: null,
                    error: 'No token returned from SDK'
                });
            }
        }

        return results;
    }

    /**
     * Parse SDK detokenize response
     * @private
     */
    _parseDetokenizeResponse(tokens, response) {
        const results = [];

        // SDK response format: { detokenizedFields: [{value}], errors: [...] }
        const detokenizedFields = response.detokenizedFields || [];
        const errors = response.errors || [];

        for (let i = 0; i < tokens.length; i++) {
            const item = tokens[i];

            // Check if this index has an error
            const errorForIndex = errors.find(e => e.index === i);
            if (errorForIndex) {
                results.push({
                    rowIndex: item.rowIndex,
                    value: null,
                    error: errorForIndex.error || 'Unknown error'
                });
                continue;
            }

            // Get value from detokenizedFields
            const detokenized = detokenizedFields[i];
            if (detokenized && detokenized.value !== undefined) {
                results.push({
                    rowIndex: item.rowIndex,
                    value: detokenized.value,
                    error: null
                });
            } else {
                results.push({
                    rowIndex: item.rowIndex,
                    value: null,
                    error: 'No value returned from SDK'
                });
            }
        }

        return results;
    }

    /**
     * Clean up resources (if needed)
     */
    destroy() {
        console.log('SkyflowClient destroyed');
    }
}

module.exports = SkyflowClient;
