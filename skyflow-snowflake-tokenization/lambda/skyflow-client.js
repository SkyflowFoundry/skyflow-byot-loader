/**
 * Skyflow SDK Client Wrapper
 *
 * Handles tokenization and detokenization using official Skyflow Node.js SDK v2.0.0
 * Supports context-aware authorization via SDK's built-in context field
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

        // Detect auth type - ctx field only supported with JWT/Service Account auth
        this.isJwtAuth = !config.credentials.apiKey;

        // Store service account credentials for token generation
        this.serviceAccountCreds = this.isJwtAuth ? JSON.stringify(config.credentials) : null;

        // Cache for context-aware clients (JWT with context field)
        // Key format: "dataType:username"
        // Relies on Lambda container recycling for natural cleanup
        this.contextClientCache = new Map();

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
     * Get or create SDK client for a specific data type and context
     * For JWT auth with context: creates client with context in credentials
     * For API key auth: returns existing client (no context support)
     *
     * @param {string} dataType - Data type (NAME, SSN, etc.)
     * @param {string} ctx - Optional Snowflake username for context
     * @returns {Promise<Skyflow>} SDK client instance
     * @private
     */
    async _getClientForContext(dataType, ctx) {
        // For API key auth, use existing client (no context support)
        if (!this.isJwtAuth) {
            return this.skyflowClients[dataType];
        }

        // For JWT auth without context, use existing client
        if (!ctx) {
            return this.skyflowClients[dataType];
        }

        // For JWT auth with context, check cache first
        const cacheKey = `${dataType}:${ctx}`;
        if (this.contextClientCache.has(cacheKey)) {
            return this.contextClientCache.get(cacheKey);
        }

        // Cache miss - create new client with context field
        const vault = this.vaultsByDataType[dataType];
        if (!vault) {
            throw new Error(`No vault configured for data type: ${dataType}`);
        }

        // Map log level
        const logLevelMap = {
            'ERROR': LogLevel.ERROR,
            'WARN': LogLevel.WARN,
            'INFO': LogLevel.INFO,
            'DEBUG': LogLevel.DEBUG
        };

        // Create credentials with context field (SDK v2 feature)
        const credentials = {
            credentialsString: this.serviceAccountCreds,
            context: ctx
        };

        const vaultConfig = {
            vaultId: vault.vaultId,
            clusterId: vault.clusterId,
            env: 'PROD',
            credentials: credentials
        };

        const skyflowConfig = {
            vaultConfigs: [vaultConfig],
            logLevel: logLevelMap[this.config.logLevel] || LogLevel.INFO
        };

        const client = new Skyflow(skyflowConfig);

        // Store in cache for reuse
        this.contextClientCache.set(cacheKey, client);
        console.log(`Created and cached context-aware client for ${dataType}:${ctx} (cache size: ${this.contextClientCache.size})`);

        return client;
    }

    /**
     * Strip punctuation from value (keep only alphanumeric and whitespace)
     * @param {string} value - Input value
     * @returns {string} Value with punctuation removed
     * @private
     */
    _stripPunctuation(value) {
        if (!value || typeof value !== 'string') {
            return value;
        }
        // Remove all non-alphanumeric characters except whitespace
        return value.replace(/[^\w\s]/g, '');
    }

    /**
     * Get length of alphanumeric characters only (excluding punctuation and whitespace)
     * @param {string} value - Input value
     * @returns {number} Length of alphanumeric characters
     * @private
     */
    _getAlphanumericLength(value) {
        if (!value || typeof value !== 'string') {
            return 0;
        }
        // Remove punctuation and whitespace, then get length
        return value.replace(/[^\w]/g, '').length;
    }

    /**
     * Validate DOB is a real date within allowed range
     * @param {string} value - Date value (YYYY-MM-DD format)
     * @param {Object} validation - Validation config with minDate and maxDate
     * @returns {boolean} True if valid date in range
     * @private
     */
    _validateDOB(value, validation) {
        if (!value || typeof value !== 'string') {
            return false;
        }

        try {
            const date = new Date(value);

            // Check if valid date
            if (isNaN(date.getTime())) {
                return false;
            }

            // Check date range
            const minDate = new Date(validation.minDate);
            const maxDate = new Date(validation.maxDate);

            return date >= minDate && date <= maxDate;
        } catch (error) {
            console.error('DOB validation error:', error);
            return false;
        }
    }

    /**
     * Preprocess value before tokenization (Protegrity-compatible behavior)
     * - Validate DOB date range (must happen before stripping punctuation)
     * - Strip punctuation
     * - Check minimum length (< minLength = skip tokenization)
     * - Apply uppercase (NAME, ID only)
     *
     * @param {string} value - Original value
     * @param {string} dataType - Data type (NAME, ID, SSN, DOB)
     * @returns {Object} { value: processed value, skipTokenization: boolean }
     * @private
     */
    _preprocessValue(value, dataType) {
        const vault = this.vaultsByDataType[dataType];
        if (!vault || !vault.transformations) {
            // No transformations configured, return as-is
            return { value: value, skipTokenization: false };
        }

        const transforms = vault.transformations;
        let processed = value;

        // Step 1: Validate DOB date range FIRST (before stripping punctuation)
        // This must happen first because date validation needs the original format (e.g., "2005-04-05")
        if (transforms.validation) {
            const isValid = this._validateDOB(processed, transforms.validation);
            if (!isValid) {
                return { value: value, skipTokenization: true };
            }
        }

        // Step 2: Strip punctuation if configured
        if (transforms.stripPunctuation) {
            processed = this._stripPunctuation(processed);
        }

        // Step 3: Check minimum length (return original value if too short)
        if (transforms.minLength) {
            const alphanumLength = this._getAlphanumericLength(processed);
            if (alphanumLength < transforms.minLength) {
                return { value: value, skipTokenization: true };
            }
        }

        // Step 4: Apply uppercase if configured
        if (transforms.uppercase) {
            processed = processed.toUpperCase();
        }

        return { value: processed, skipTokenization: false };
    }

    /**
     * Tokenize a batch of values
     * @param {Array} values - Array of {rowIndex, value, vaultId, table, column, dataType}
     * @param {string} ctx - Optional context (e.g., Snowflake username) for audit logging
     * @returns {Promise<Array>} Array of {rowIndex, token, error}
     */
    async tokenizeBatch(values, ctx = null) {
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
            const results = await this._tokenizeDataTypeGroup(dataType, groupValues, ctx);
            allResults.push(...results);
        }

        // No need to sort - results are already in order from sequential processing

        console.log(`Tokenization complete: ${allResults.length} results`);
        return allResults;
    }

    /**
     * Detokenize a batch of tokens
     * @param {Array} tokens - Array of {rowIndex, token, vaultId, dataType}
     * @param {string} ctx - Optional context (e.g., Snowflake username) for audit logging
     * @returns {Promise<Array>} Array of {rowIndex, value, error}
     */
    async detokenizeBatch(tokens, ctx = null) {
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
            const results = await this._detokenizeDataTypeGroup(dataType, groupTokens, ctx);
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
    async _tokenizeDataTypeGroup(dataType, values, ctx) {
        const vault = this.vaultsByDataType[dataType];
        if (!vault) {
            console.error(`No vault configured for data type: ${dataType}`);
            return values.map(v => ({
                rowIndex: v.rowIndex,
                token: null,
                error: `No vault configured for data type: ${dataType}`
            }));
        }

        // Get client with context-aware token if JWT auth
        const client = await this._getClientForContext(dataType, ctx);
        const { table, column, vaultId } = vault;

        // Split into batches if needed
        if (values.length > this.TOKENIZE_BATCH_SIZE) {
            // Create batches
            const batches = [];
            for (let i = 0; i < values.length; i += this.TOKENIZE_BATCH_SIZE) {
                batches.push(values.slice(i, i + this.TOKENIZE_BATCH_SIZE));
            }

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
            // Preprocess values and separate into "skip" vs "process" groups
            const preprocessedValues = values.map(item => {
                const result = this._preprocessValue(item.value, dataType);
                return {
                    ...item,
                    preprocessedValue: result.value,
                    skipTokenization: result.skipTokenization
                };
            });

            // Separate values that should skip tokenization
            const valuesToSkip = preprocessedValues.filter(v => v.skipTokenization);
            const valuesToProcess = preprocessedValues.filter(v => !v.skipTokenization);

            // Collect results for skipped values (return original value as "token")
            const skippedResults = valuesToSkip.map(item => ({
                rowIndex: item.rowIndex,
                token: item.value,  // Original value returned as token
                error: null
            }));

            // If no values to process, return skipped results only
            if (valuesToProcess.length === 0) {
                return skippedResults;
            }

            // Prepare insert data for SDK (using preprocessed values)
            const insertData = valuesToProcess.map(item => ({
                [column]: item.preprocessedValue
            }));

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

            // Parse SDK response for processed values
            const processedResults = this._parseInsertResponse(valuesToProcess, response, column);

            // Merge skipped and processed results, maintaining original order
            const allResults = [...skippedResults, ...processedResults];
            allResults.sort((a, b) => a.rowIndex - b.rowIndex);

            return allResults;

        } catch (error) {
            console.error(`Tokenization failed for ${dataType}:`, error.message);
            // Merge skipped results with errors for processed values
            const errorResults = valuesToProcess.map(v => ({
                rowIndex: v.rowIndex,
                token: null,
                error: error.message
            }));
            return [...skippedResults, ...errorResults].sort((a, b) => a.rowIndex - b.rowIndex);
        }
    }

    /**
     * Detokenize a group of tokens for a specific data type
     * @private
     */
    async _detokenizeDataTypeGroup(dataType, tokens, ctx) {
        const vault = this.vaultsByDataType[dataType];
        if (!vault) {
            console.error(`No vault configured for data type: ${dataType}`);
            return tokens.map(t => ({
                rowIndex: t.rowIndex,
                value: null,
                error: `No vault configured for data type: ${dataType}`
            }));
        }

        // Get client with context-aware token if JWT auth
        const client = await this._getClientForContext(dataType, ctx);
        const { vaultId } = vault;

        // Split into batches if needed
        if (tokens.length > this.DETOKENIZE_BATCH_SIZE) {
            // Create batches
            const batches = [];
            for (let i = 0; i < tokens.length; i += this.DETOKENIZE_BATCH_SIZE) {
                batches.push(tokens.slice(i, i + this.DETOKENIZE_BATCH_SIZE));
            }

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

        for (let i = 0; i < values.length; i++) {
            const item = values[i];

            // Check if this index has an error
            const errorForIndex = errors.find(e => e.index === i);
            if (errorForIndex) {
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

            if (inserted && inserted[column]) {
                results.push({
                    rowIndex: item.rowIndex,
                    token: inserted[column],  // Token is the field value directly
                    error: null
                });
            } else {
                results.push({
                    rowIndex: item.rowIndex,
                    token: null,
                    error: 'No token returned from SDK'
                });
            }
        }

        // Log summary only
        const successCount = results.filter(r => !r.error).length;
        const errorCount = results.filter(r => r.error).length;
        if (errorCount > 0) {
            console.log(`Insert response parsed: ${successCount} successful, ${errorCount} errors`);
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
