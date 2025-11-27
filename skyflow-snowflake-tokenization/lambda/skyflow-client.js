/**
 * Skyflow SDK Client Wrapper
 *
 * Handles tokenization and detokenization using official Skyflow Node.js SDK v2.0.0
 */

const { Skyflow, LogLevel, RedactionType, InsertRequest, InsertOptions, DetokenizeRequest, DetokenizeOptions, QueryRequest, DeleteRequest, DeleteResponse, TokenMode } = require('skyflow-node');

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

        // Detect auth type
        this.isJwtAuth = !config.credentials.apiKey;

        // Separate batch size and concurrency for tokenize vs detokenize vs delete
        this.TOKENIZE_BATCH_SIZE = config.tokenizeBatchSize;
        this.TOKENIZE_MAX_CONCURRENCY = config.tokenizeMaxConcurrency;
        this.DETOKENIZE_BATCH_SIZE = config.detokenizeBatchSize;
        this.DETOKENIZE_MAX_CONCURRENCY = config.detokenizeMaxConcurrency;
        this.DELETE_BATCH_SIZE = config.deleteBatchSize;
        this.DELETE_MAX_CONCURRENCY = config.deleteMaxConcurrency;

        // Map log level string to SDK enum
        const logLevelMap = {
            'ERROR': LogLevel.ERROR,
            'WARN': LogLevel.WARN,
            'INFO': LogLevel.INFO,
            'DEBUG': LogLevel.DEBUG
        };
        this.logLevelMap = logLevelMap;

        // Lazy initialization: SDK clients created on-demand per data type
        this.skyflowClients = {};

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
            },
            delete: {
                batchSize: this.DELETE_BATCH_SIZE,
                maxConcurrency: this.DELETE_MAX_CONCURRENCY
            }
        });
    }

    /**
     * Get or lazily initialize Skyflow SDK client for a specific data type
     * @param {string} dataType - Data type (NAME, ID, DOB, SSN)
     * @returns {Skyflow} Skyflow SDK client instance
     * @private
     */
    _getOrInitializeClient(dataType) {
        // Return cached client if already initialized
        if (this.skyflowClients[dataType]) {
            return this.skyflowClients[dataType];
        }

        // Find vault configuration for this data type
        const vault = this.config.vaults.find(v => v.dataType === dataType);
        if (!vault) {
            throw new Error(`No vault configuration found for data type: ${dataType}`);
        }

        // SDK expects credentials wrapped in specific format
        // For service account: { credentialsString: JSON.stringify(serviceAccountObject) }
        // For API key: { apiKey: 'key' }
        let credentials;
        if (this.config.credentials.apiKey) {
            // API key format
            credentials = { apiKey: this.config.credentials.apiKey };
        } else {
            // Service account format - SDK needs credentialsString
            credentials = { credentialsString: JSON.stringify(this.config.credentials) };
        }

        const vaultConfig = {
            vaultId: vault.vaultId,
            clusterId: vault.clusterId,
            env: 'PROD',
            credentials: credentials
        };

        const skyflowConfig = {
            vaultConfigs: [vaultConfig],
            logLevel: this.logLevelMap[this.config.logLevel] || LogLevel.INFO
        };

            vaultId: vault.vaultId,
            clusterId: vault.clusterId,
            credentialType: this.config.credentials.apiKey ? 'API Key' : 'Service Account'
        });

        // Initialize and cache the client
        this.skyflowClients[dataType] = new Skyflow(skyflowConfig);
        return this.skyflowClients[dataType];
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
     * Preprocess value before tokenization
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
     * @param {boolean} isOneway - If true, automatically use <table>_oneway and <column>_oneway
     * @returns {Promise<Array>} Array of {rowIndex, token, error}
     */
    async tokenizeBatch(values, isOneway = false) {
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


        // Process each data type group SEQUENTIALLY (with parallelization within each)
        const allResults = [];
        for (const [dataType, groupValues] of Object.entries(groupedByDataType)) {
            const results = await this._tokenizeDataTypeGroup(dataType, groupValues, isOneway);
            allResults.push(...results);
        }

        // No need to sort - results are already in order from sequential processing

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


        // Process each data type group SEQUENTIALLY (with parallelization within each)
        const allResults = [];
        for (const [dataType, groupTokens] of Object.entries(groupedByDataType)) {
            const results = await this._detokenizeDataTypeGroup(dataType, groupTokens);
            allResults.push(...results);
        }

        // No need to sort - results are already in order from sequential processing

        return allResults;
    }

    /**
     * Tokenize a group of values for a specific data type
     * @private
     * @param {string} dataType - Data type
     * @param {Array} values - Values to tokenize
     * @param {boolean} isOneway - If true, use <table>_oneway and <column>_oneway
     */
    async _tokenizeDataTypeGroup(dataType, values, isOneway = false) {
        const vault = this.vaultsByDataType[dataType];
        if (!vault) {
            console.error(`No vault configured for data type: ${dataType}`);
            return values.map(v => ({
                rowIndex: v.rowIndex,
                token: null,
                error: `No vault configured for data type: ${dataType}`
            }));
        }

        // Step 1: Preprocess all input values
        // Map: transformedValue -> list of {rowIndex, originalValue}
        const transformedMap = new Map();
        const skippedResults = [];
        for (const item of values) {
            const result = this._preprocessValue(item.value, dataType);
            if (result.skipTokenization) {
                // Skipped: return original value as token
                skippedResults.push({
                    rowIndex: item.rowIndex,
                    token: item.value,
                    error: null
                });
            } else {
                // Deduplicate by transformed value
                if (!transformedMap.has(result.value)) {
                    transformedMap.set(result.value, []);
                }
                transformedMap.get(result.value).push({
                    rowIndex: item.rowIndex,
                    originalValue: item.value
                });
            }
        }

        // If no values to process, return skipped results only
        if (transformedMap.size === 0) {
            skippedResults.sort((a, b) => a.rowIndex - b.rowIndex);
            return skippedResults;
        }

        // Step 2: Tokenize only unique transformed values (in batches)
        const client = this._getOrInitializeClient(dataType);
        // If oneway, automatically append _oneway to table and column
        const table = isOneway ? `${vault.table}_oneway` : vault.table;
        const column = isOneway ? `${vault.column}_oneway` : vault.column;
        const { vaultId } = vault;
        const uniqueTransformedValues = Array.from(transformedMap.keys());
        const processedResults = [];

        // Helper: batch tokenize unique transformed values
        const tokenizeBatch = async (transformedVals) => {
            try {
                const insertData = transformedVals.map(val => ({ [column]: val }));
                const insertRequest = new InsertRequest(table, insertData);
                const insertOptions = new InsertOptions();
                insertOptions.setReturnTokens(true);
                insertOptions.setUpsertColumn(column);
                insertOptions.setContinueOnError(false);
                const response = await client.vault(vaultId).insert(insertRequest, insertOptions);
                // SDK returns results in same order as input
                const insertedFields = response.insertedFields || [];
                const errors = response.errors || [];
                const batchResults = [];
                for (let i = 0; i < transformedVals.length; i++) {
                    const val = transformedVals[i];
                    const errorForIndex = errors.find(e => e.index === i);
                    if (errorForIndex) {
                        batchResults.push({
                            transformedValue: val,
                            token: null,
                            error: errorForIndex.error || 'Unknown error',
                            skyflowId: null
                        });
                    } else {
                        const inserted = insertedFields[i];
                        if (inserted && inserted[column]) {
                            batchResults.push({
                                transformedValue: val,
                                token: inserted[column],
                                error: null,
                                skyflowId: inserted.skyflowId || null
                            });
                        } else {
                            batchResults.push({
                                transformedValue: val,
                                token: null,
                                error: 'No token returned from SDK',
                                skyflowId: null
                            });
                        }
                    }
                }
                return batchResults;
            } catch (error) {
                console.error(`Tokenize batch failed for ${dataType}:`, error.message);
                return transformedVals.map(val => ({
                    transformedValue: val,
                    token: null,
                    error: error.message || 'Tokenization failed',
                    skyflowId: null
                }));
            }
        };

        // Batch processing
        if (uniqueTransformedValues.length > this.TOKENIZE_BATCH_SIZE) {
            const batches = [];
            for (let i = 0; i < uniqueTransformedValues.length; i += this.TOKENIZE_BATCH_SIZE) {
                batches.push(uniqueTransformedValues.slice(i, i + this.TOKENIZE_BATCH_SIZE));
            }
            for (let i = 0; i < batches.length; i += this.TOKENIZE_MAX_CONCURRENCY) {
                const batchGroup = batches.slice(i, i + this.TOKENIZE_MAX_CONCURRENCY);
                const groupResults = await Promise.all(batchGroup.map(tokenizeBatch));
                processedResults.push(...groupResults.flat());
            }
        } else {
            const batchResults = await tokenizeBatch(uniqueTransformedValues);
            processedResults.push(...batchResults);
        }

        // Step 3: Map tokens back to all original input rows
        const tokenMap = new Map();
        for (const result of processedResults) {
            tokenMap.set(result.transformedValue, {
                token: result.token,
                error: result.error,
                skyflowId: result.skyflowId
            });
        }

        const finalResults = [];
        // Add skipped results (with null skyflowId since no record was created)
        finalResults.push(...skippedResults.map(r => ({ ...r, skyflowId: null })));
        // Add processed results for all rowIndexes
        for (const [transformedValue, rowInfos] of transformedMap.entries()) {
            if (tokenMap.has(transformedValue)) {
                const { token, error, skyflowId } = tokenMap.get(transformedValue);
                for (const rowInfo of rowInfos) {
                    finalResults.push({
                        rowIndex: rowInfo.rowIndex,
                        token,
                        error,
                        skyflowId
                    });
                }
            }
        }

        // Sort by rowIndex to preserve original order
        finalResults.sort((a, b) => a.rowIndex - b.rowIndex);
        return finalResults;
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
            insertOptions.setContinueOnError(false);

            const startTime = Date.now();
            const response = await client.vault(vaultId).insert(insertRequest, insertOptions);
            const elapsed = Date.now() - startTime;


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

        // Step 1: Build token -> rowIndexes map
        const tokenToRowIndexes = new Map();
        tokens.forEach(item => {
            if (!tokenToRowIndexes.has(item.token)) {
                tokenToRowIndexes.set(item.token, []);
            }
            tokenToRowIndexes.get(item.token).push(item.rowIndex);
        });

        // Step 2: Preprocess unique tokens for skip logic
        const uniqueTokens = Array.from(tokenToRowIndexes.keys());
        const preprocessedUniqueTokens = uniqueTokens.map(token => {
            const result = this._preprocessValue(token, dataType);
            return {
                token,
                preprocessedToken: result.value,
                skipDetokenization: result.skipTokenization
            };
        });

        // Step 3: Separate tokens to skip and to process
        const tokensToSkip = preprocessedUniqueTokens.filter(t => t.skipDetokenization);
        const tokensToProcess = preprocessedUniqueTokens.filter(t => !t.skipDetokenization);

        // Step 4: Results for skipped tokens: return original value for all rowIndexes
        const skippedResults = [];
        for (const item of tokensToSkip) {
            const rowIndexes = tokenToRowIndexes.get(item.token) || [];
            for (const rowIndex of rowIndexes) {
                skippedResults.push({
                    rowIndex,
                    value: item.token,
                    error: null
                });
            }
        }

        // If no tokens to process, return skipped results only (sorted)
        if (tokensToProcess.length === 0) {
            skippedResults.sort((a, b) => a.rowIndex - b.rowIndex);
            return skippedResults;
        }

        // Step 5: Detokenize only unique tokens to process, in batches
        const client = this._getOrInitializeClient(dataType);
        const { vaultId } = vault;
        const processedResults = [];

        // Helper: batch detokenize unique tokens
        const detokenizeBatch = async (tokenObjs) => {
            try {
                // Prepare input for SDK
                const detokenizeData = tokenObjs.map(item => ({
                    token: item.token,
                    redactionType: RedactionType.PLAIN_TEXT
                }));
                // Call SDK
                const detokenizeRequest = new DetokenizeRequest(detokenizeData);
                const detokenizeOptions = new DetokenizeOptions();
                detokenizeOptions.setContinueOnError(true);
                const response = await client.vault(vaultId).detokenize(detokenizeRequest, detokenizeOptions);
                // Parse SDK response
                // SDK returns results in same order as input
                const detokenizedFields = response.detokenizedFields || [];
                const errors = response.errors || [];
                const batchResults = [];
                for (let i = 0; i < tokenObjs.length; i++) {
                    const item = tokenObjs[i];
                    const errorForIndex = errors.find(e => e.index === i);
                    if (errorForIndex) {
                        batchResults.push({
                            token: item.token,
                            value: null,
                            error: errorForIndex.error || 'Unknown error'
                        });
                    } else {
                        const detokenized = detokenizedFields[i];
                        if (detokenized && detokenized.value !== undefined) {
                            batchResults.push({
                                token: item.token,
                                value: detokenized.value,
                                error: null
                            });
                        } else {
                            batchResults.push({
                                token: item.token,
                                value: null,
                                error: 'No value returned from SDK'
                            });
                        }
                    }
                }
                return batchResults;
            } catch (error) {
                console.error(`Detokenize batch failed for ${dataType}:`, error.message);
                return tokenObjs.map(item => ({
                    token: item.token,
                    value: null,
                    error: error.message || 'Token not found'
                }));
            }
        };

        // Batch processing
        if (tokensToProcess.length > this.DETOKENIZE_BATCH_SIZE) {
            const batches = [];
            for (let i = 0; i < tokensToProcess.length; i += this.DETOKENIZE_BATCH_SIZE) {
                batches.push(tokensToProcess.slice(i, i + this.DETOKENIZE_BATCH_SIZE));
            }
            for (let i = 0; i < batches.length; i += this.DETOKENIZE_MAX_CONCURRENCY) {
                const batchGroup = batches.slice(i, i + this.DETOKENIZE_MAX_CONCURRENCY);
                const groupResults = await Promise.all(batchGroup.map(detokenizeBatch));
                processedResults.push(...groupResults.flat());
            }
        } else {
            const batchResults = await detokenizeBatch(tokensToProcess);
            processedResults.push(...batchResults);
        }

        // Step 6: Map detokenized results back to all rowIndexes
        const detokenizedMap = new Map();
        for (const result of processedResults) {
            detokenizedMap.set(result.token, { value: result.value, error: result.error });
        }

        const finalResults = [];
        // Add skipped results
        finalResults.push(...skippedResults);
        // Add processed results for all rowIndexes
        for (const [token, rowIndexes] of tokenToRowIndexes.entries()) {
            if (detokenizedMap.has(token)) {
                const { value, error } = detokenizedMap.get(token);
                for (const rowIndex of rowIndexes) {
                    finalResults.push({
                        rowIndex,
                        value,
                        error
                    });
                }
            }
        }

        // Sort by rowIndex to preserve original order
        finalResults.sort((a, b) => a.rowIndex - b.rowIndex);
        return finalResults;
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
                    error: errorForIndex.error || 'Unknown error',
                    skyflowId: null
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
                    error: null,
                    skyflowId: inserted.skyflowId || null  // Capture skyflowId for one-way tokenization
                });
            } else {
                results.push({
                    rowIndex: item.rowIndex,
                    token: null,
                    error: 'No token returned from SDK',
                    skyflowId: null
                });
            }
        }

        // Log summary only
        const successCount = results.filter(r => !r.error).length;
        const errorCount = results.filter(r => r.error).length;
        if (errorCount > 0) {
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
     * Delete records by skyflowId in batches
     *
     * @param {Array} deleteGroups - Array of {vaultId, table, dataType, records: [{skyflowId, rowIndex}]}
     * @returns {Promise<Array>} Array of {skyflowId, rowIndex, error: null|string}
     */
    async deleteRecordsBatch(deleteGroups) {
        const allResults = [];

        for (const group of deleteGroups) {
            const { vaultId, table, dataType, records } = group;

            const client = this._getOrInitializeClient(dataType);

            // Batch delete in chunks
            const chunks = [];
            for (let i = 0; i < records.length; i += this.DELETE_BATCH_SIZE) {
                chunks.push(records.slice(i, i + this.DELETE_BATCH_SIZE));
            }

            // Process chunks with concurrency control
            for (let i = 0; i < chunks.length; i += this.DELETE_MAX_CONCURRENCY) {
                const batchGroup = chunks.slice(i, i + this.DELETE_MAX_CONCURRENCY);
                const batchResults = await Promise.all(
                    batchGroup.map(chunk => this._deleteBatch(client, vaultId, table, chunk))
                );
                allResults.push(...batchResults.flat());
            }
        }

        return allResults;
    }

    /**
     * Delete a single batch of records
     * @private
     */
    async _deleteBatch(client, vaultId, table, records) {
        try {
            const skyflowIds = records.map(r => r.skyflowId);

            const startTime = Date.now();

            const deleteRequest = new DeleteRequest(table, skyflowIds);
            const response = await client.vault(vaultId).delete(deleteRequest);

            const elapsed = Date.now() - startTime;

            // Parse SDK response
            const deletedIds = response.deletedIds || [];
            const errors = response.errors || [];

            // Map results back to records
            return records.map(record => {
                // Check if this record had an error
                const errorForId = errors.find(e => e.id === record.skyflowId);
                if (errorForId) {
                    return {
                        skyflowId: record.skyflowId,
                        rowIndex: record.rowIndex,
                        error: errorForId.error || 'Delete failed'
                    };
                }

                // Check if this record was successfully deleted
                const wasDeleted = deletedIds.includes(record.skyflowId);
                if (wasDeleted) {
                    return {
                        skyflowId: record.skyflowId,
                        rowIndex: record.rowIndex,
                        error: null
                    };
                }

                // Not in deletedIds and no error - shouldn't happen
                return {
                    skyflowId: record.skyflowId,
                    rowIndex: record.rowIndex,
                    error: 'Delete failed: record not in deletedIds response'
                };
            });
        } catch (error) {
            console.error(`Delete batch failed for table ${table}:`, error.message);
            // All records in this batch failed
            return records.map(r => ({
                skyflowId: r.skyflowId,
                rowIndex: r.rowIndex,
                error: error.message
            }));
        }
    }

    /**
     * Tokenize values and immediately delete the vault records (one-way tokenization)
     * This operation is atomic: tokens are only returned if delete succeeds
     *
     * @param {Array} values - Array of {rowIndex, value, vaultId, table, column, dataType}
     * @returns {Promise<Array>} Array of {rowIndex, token, error}
     */
    async tokenizeOneWayBatch(values) {
        if (!values || values.length === 0) {
            return [];
        }


        // Step 1: Tokenize normally (captures skyflowIds in results)
        // Pass isOneway=true to automatically use <table>_oneway and <column>_oneway
        const tokenizeResults = await this.tokenizeBatch(values, true);

        // Step 2: Extract successful tokenizations with skyflowIds
        const successfulTokenizations = tokenizeResults.filter(r => !r.error && r.skyflowId);

        if (successfulTokenizations.length === 0) {
            return tokenizeResults;
        }


        // Step 3: Group by vaultId + table + dataType for batch deletion
        const deleteGroups = this._groupForDeletion(successfulTokenizations, values);

        // Step 4: Delete records in batches (atomic per record)
        const deleteResults = await this.deleteRecordsBatch(deleteGroups);

        // Step 5: Build final results (atomic per record)
        // Only return tokens for successfully deleted records
        const finalResults = tokenizeResults.map(tokenResult => {
            if (tokenResult.error) {
                // Tokenization failed - return original error
                return {
                    rowIndex: tokenResult.rowIndex,
                    token: null,
                    error: tokenResult.error
                };
            }

            if (!tokenResult.skyflowId) {
                // No skyflowId means record wasn't created (shouldn't happen)
                return {
                    rowIndex: tokenResult.rowIndex,
                    token: null,
                    error: 'Tokenization succeeded but no skyflowId returned'
                };
            }

            // Check if delete succeeded for this record
            const deleteResult = deleteResults.find(d => d.skyflowId === tokenResult.skyflowId);

            if (deleteResult && !deleteResult.error) {
                // Atomic success: tokenized AND deleted - return token
                return {
                    rowIndex: tokenResult.rowIndex,
                    token: tokenResult.token,
                    error: null
                };
            } else {
                // Delete failed - DON'T return token (not truly one-way)
                const deleteError = deleteResult?.error || 'Delete failed: unknown error';
                return {
                    rowIndex: tokenResult.rowIndex,
                    token: null,
                    error: `One-way tokenization failed: ${deleteError}`
                };
            }
        });

        const successCount = finalResults.filter(r => !r.error).length;
        const errorCount = finalResults.filter(r => r.error).length;

        return finalResults;
    }

    /**
     * Group successful tokenizations for deletion
     * @private
     */
    _groupForDeletion(successfulTokenizations, originalValues) {
        const groups = {};

        for (const result of successfulTokenizations) {
            const originalValue = originalValues.find(v => v.rowIndex === result.rowIndex);
            if (!originalValue) continue;

            const { vaultId, dataType } = originalValue;
            const table = `${originalValue.table}_oneway`;
            const groupKey = `${vaultId}_${table}_${dataType}`;

            if (!groups[groupKey]) {
                groups[groupKey] = {
                    vaultId,
                    table,
                    dataType,
                    records: []
                };
            }

            groups[groupKey].records.push({
                skyflowId: result.skyflowId,
                rowIndex: result.rowIndex
            });
        }

        return Object.values(groups);
    }

    /**
     * BYOT (Bring Your Own Token) - Insert values with custom tokens
     * Allows specifying your own token values instead of Skyflow generating them
     *
     * @param {Array} values - Array of {rowIndex, value, token, vaultId, table, column, dataType}
     * @returns {Promise<Array>} Array of {rowIndex, token, error}
     */
    async byotBatch(values) {
        if (!values || values.length === 0) {
            return [];
        }


        const groups = {};
        for (const item of values) {
            const { dataType } = item;
            if (!groups[dataType]) {
                groups[dataType] = [];
            }
            groups[dataType].push(item);
        }

        const allResults = [];
        for (const [dataType, groupValues] of Object.entries(groups)) {
            if (dataType === 'DOB') {
                const results = await this._byotDOBGroup(groupValues);
                allResults.push(...results);
            } else {
                const results = await this._byotRegularGroup(groupValues);
                allResults.push(...results);
            }
        }

        allResults.sort((a, b) => a.rowIndex - b.rowIndex);
        return allResults;
    }

    /**
     * BYOT for regular data types (NAME, ID, SSN)
     * @private
     */
    async _byotRegularGroup(values) {
        if (values.length === 0) return [];

        const dataType = values[0].dataType;
        const vault = this.vaultsByDataType[dataType];
        if (!vault) {
            console.error(`No vault configured for data type: ${dataType}`);
            return values.map(v => ({
                rowIndex: v.rowIndex,
                token: null,
                error: `No vault configured for data type: ${dataType}`
            }));
        }

        const client = this._getOrInitializeClient(dataType);
        const { vaultId, table, column } = vault;
        const results = [];

        try {
            const insertData = values.map(v => ({ [column]: v.value }));
            const tokens = values.map(v => ({ [column]: v.token }));

            const insertRequest = new InsertRequest(table, insertData);
            const insertOptions = new InsertOptions();
            insertOptions.setReturnTokens(true);
            insertOptions.setTokenMode(TokenMode.ENABLE);
            insertOptions.setTokens(tokens);
            insertOptions.setContinueOnError(false);

            const response = await client.vault(vaultId).insert(insertRequest, insertOptions);

            const insertedFields = response.insertedFields || [];
            const errors = response.errors || [];

            for (let i = 0; i < values.length; i++) {
                const value = values[i];
                const errorForIndex = errors.find(e => e.index === i);

                if (errorForIndex) {
                    results.push({
                        rowIndex: value.rowIndex,
                        token: null,
                        error: errorForIndex.error || 'BYOT failed'
                    });
                } else {
                    const inserted = insertedFields[i];
                    if (inserted && inserted[column]) {
                        results.push({
                            rowIndex: value.rowIndex,
                            token: inserted[column],
                            error: null
                        });
                    } else {
                        results.push({
                            rowIndex: value.rowIndex,
                            token: null,
                            error: 'No token returned from BYOT'
                        });
                    }
                }
            }

            return results;
        } catch (error) {
            console.error(`BYOT failed for ${dataType}:`, error.message);
            return values.map(v => ({
                rowIndex: v.rowIndex,
                token: null,
                error: error.message || 'BYOT failed'
            }));
        }
    }

    /**
     * BYOT for DOB with year preservation
     * Token format: YYYY-MM-DD (year preserved, month-day tokenized)
     * @private
     */
    async _byotDOBGroup(values) {
        const vault = this.vaultsByDataType['DOB'];
        if (!vault) {
            console.error('No vault configured for DOB');
            return values.map(v => ({
                rowIndex: v.rowIndex,
                token: null,
                error: 'No vault configured for DOB'
            }));
        }

        const client = this._getOrInitializeClient('DOB');
        const { vaultId } = vault;
        const table = 'dob_partial_token';

        const columns = {
            dob_full: 'dob_full',
            dob_year: 'dob_year',
            month_day_token: 'month_day_token'
        };

        const results = [];

        for (const item of values) {
            try {
                const dobFull = item.value;
                const customToken = item.token;

                if (!/^\d{4}-\d{2}-\d{2}$/.test(dobFull)) {
                    results.push({
                        rowIndex: item.rowIndex,
                        token: null,
                        error: `Invalid DOB format: ${dobFull}. Expected YYYY-MM-DD`
                    });
                    continue;
                }

                if (!/^\d{4}-\d{2}-\d{2}$/.test(customToken)) {
                    results.push({
                        rowIndex: item.rowIndex,
                        token: null,
                        error: `Invalid custom token format: ${customToken}. Expected YYYY-MM-DD`
                    });
                    continue;
                }

                const dobYear = dobFull.substring(0, 4);
                const tokenizedMonthDay = customToken.substring(5);

                const insertData = [{
                    [columns.dob_year]: dobYear,
                    [columns.dob_full]: dobFull,
                    [columns.month_day_token]: tokenizedMonthDay
                }];

                const tokens = [{
                    [columns.dob_full]: customToken
                }];

                const insertRequest = new InsertRequest(table, insertData);
                const insertOptions = new InsertOptions();
                insertOptions.setReturnTokens(true);
                insertOptions.setTokenMode(TokenMode.ENABLE);
                insertOptions.setTokens(tokens);
                insertOptions.setUpsertColumn(columns.dob_full);
                insertOptions.setContinueOnError(false);

                await client.vault(vaultId).insert(insertRequest, insertOptions);

                const finalToken = `${dobYear}-${tokenizedMonthDay}`;

                results.push({
                    rowIndex: item.rowIndex,
                    token: finalToken,
                    error: null
                });


            } catch (error) {
                console.error(`BYOT DOB failed for row ${item.rowIndex}:`, error.message);
                results.push({
                    rowIndex: item.rowIndex,
                    token: null,
                    error: error.message
                });
            }
        }

        return results;
    }

    /**
     * Tokenize values with partial tokenization (e.g., DOB with preserved year)
     * For DOB: tokenizes month-day, preserves year in plaintext
     *
     * @param {Array} values - Array of {rowIndex, value, vaultId, table, column, dataType}
     * @returns {Promise<Array>} Array of {rowIndex, token, error}
     */
    async tokenizePartialBatch(values) {
        if (!values || values.length === 0) {
            return [];
        }


        const groups = {};
        for (const value of values) {
            const dt = value.dataType || 'UNKNOWN';
            if (!groups[dt]) groups[dt] = [];
            groups[dt].push(value);
        }

        const allResults = [];

        for (const [dataType, groupValues] of Object.entries(groups)) {
            if (dataType === 'DOB') {
                const results = await this._tokenizeDOBPartialGroup(groupValues);
                allResults.push(...results);
            } else if (dataType === 'SSN') {
                const results = await this._tokenizeSSNPartialGroup(groupValues);
                allResults.push(...results);
            } else {
                const errorResults = groupValues.map(v => ({
                    rowIndex: v.rowIndex,
                    token: null,
                    error: `Partial tokenization not supported for data type: ${dataType}`
                }));
                allResults.push(...errorResults);
            }
        }

        allResults.sort((a, b) => a.rowIndex - b.rowIndex);
        return allResults;
    }

    /**
     * Tokenize DOB values with year preservation (partial tokenization)
     * Uses dob_partial_token table in same DOB vault
     * Stores: dob_full, dob_year (plaintext), month_day_token (tokenized)
     *
     * @private
     * @param {Array} values - Values to tokenize
     * @returns {Promise<Array>} Array of {rowIndex, token, error}
     */
    async _tokenizeDOBPartialGroup(values) {
        const vault = this.vaultsByDataType['DOB'];
        if (!vault) {
            console.error('No vault configured for DOB');
            return values.map(v => ({
                rowIndex: v.rowIndex,
                token: null,
                error: 'No vault configured for DOB'
            }));
        }

        const client = this._getOrInitializeClient('DOB');
        const { vaultId } = vault;
        const table = 'dob_partial_token';

        const columns = {
            dob_full: 'dob_full',
            dob_year: 'dob_year',
            month_day_token: 'month_day_token'
        };

        const results = [];

        for (const item of values) {
            try {
                const dobFull = item.value;

                if (!/^\d{4}-\d{2}-\d{2}$/.test(dobFull)) {
                    results.push({
                        rowIndex: item.rowIndex,
                        token: null,
                        error: `Invalid date format: ${dobFull}. Expected YYYY-MM-DD`
                    });
                    continue;
                }

                const dobYear = dobFull.substring(0, 4);

                const insertData1 = [{
                    [columns.dob_year]: dobYear,
                    [columns.dob_full]: dobFull
                }];

                const insertRequest1 = new InsertRequest(table, insertData1);
                const insertOptions1 = new InsertOptions();
                insertOptions1.setReturnTokens(true);
                insertOptions1.setUpsertColumn(columns.dob_full);
                insertOptions1.setContinueOnError(false);

                const response1 = await client.vault(vaultId).insert(insertRequest1, insertOptions1);

                const insertedFields1 = response1.insertedFields || [];
                if (!insertedFields1[0] || !insertedFields1[0][columns.dob_full]) {
                    results.push({
                        rowIndex: item.rowIndex,
                        token: null,
                        error: 'No FPT token returned from Call 1'
                    });
                    continue;
                }

                const fptToken = insertedFields1[0][columns.dob_full];

                const tokenizedMonthDay = fptToken.substring(5);

                const insertData2 = [{
                    [columns.dob_year]: dobYear,
                    [columns.dob_full]: dobFull,
                    [columns.month_day_token]: tokenizedMonthDay
                }];

                const insertRequest2 = new InsertRequest(table, insertData2);
                const insertOptions2 = new InsertOptions();
                insertOptions2.setReturnTokens(true);
                insertOptions2.setUpsertColumn(columns.dob_full);
                insertOptions2.setContinueOnError(false);

                await client.vault(vaultId).insert(insertRequest2, insertOptions2);


                const finalToken = `${dobYear}-${tokenizedMonthDay}`;

                results.push({
                    rowIndex: item.rowIndex,
                    token: finalToken,
                    error: null
                });


            } catch (error) {
                console.error(`DOB Partial tokenization failed for row ${item.rowIndex}:`, error.message);
                results.push({
                    rowIndex: item.rowIndex,
                    token: null,
                    error: error.message
                });
            }
        }

        return results;
    }

    /**
     * Tokenize SSN values with last 4 preservation (partial tokenization)
     * Uses ssn_partial_token table in same SSN vault
     * Stores: ssn_full, ssn_last4 (plaintext), ssn_first5_token (tokenized)
     *
     * @private
     * @param {Array} values - Values to tokenize
     * @returns {Promise<Array>} Array of {rowIndex, token, error}
     */
    async _tokenizeSSNPartialGroup(values) {
        const vault = this.vaultsByDataType['SSN'];
        if (!vault) {
            console.error('No vault configured for SSN');
            return values.map(v => ({
                rowIndex: v.rowIndex,
                token: null,
                error: 'No vault configured for SSN'
            }));
        }

        const client = this._getOrInitializeClient('SSN');
        const { vaultId } = vault;
        const table = 'ssn_partial_token';

        const columns = {
            ssn_full: 'ssn_full',
            ssn_last4: 'ssn_last4',
            ssn_first5_token: 'ssn_first5_token'
        };

        const results = [];

        for (const item of values) {
            try {
                const ssnFull = item.value;

                if (!/^\d{3}-\d{2}-\d{4}$/.test(ssnFull)) {
                    results.push({
                        rowIndex: item.rowIndex,
                        token: null,
                        error: `Invalid SSN format: ${ssnFull}. Expected XXX-XX-XXXX`
                    });
                    continue;
                }

                const ssnLast4 = ssnFull.substring(7);

                const insertData1 = [{
                    [columns.ssn_last4]: ssnLast4,
                    [columns.ssn_full]: ssnFull
                }];

                const insertRequest1 = new InsertRequest(table, insertData1);
                const insertOptions1 = new InsertOptions();
                insertOptions1.setReturnTokens(true);
                insertOptions1.setUpsertColumn(columns.ssn_full);
                insertOptions1.setContinueOnError(false);

                const response1 = await client.vault(vaultId).insert(insertRequest1, insertOptions1);

                const insertedFields1 = response1.insertedFields || [];
                if (!insertedFields1[0] || !insertedFields1[0][columns.ssn_full]) {
                    results.push({
                        rowIndex: item.rowIndex,
                        token: null,
                        error: 'No FPT token returned from Call 1'
                    });
                    continue;
                }

                const fptToken = insertedFields1[0][columns.ssn_full];

                const tokenizedFirst5 = fptToken.substring(0, 6);

                const insertData2 = [{
                    [columns.ssn_last4]: ssnLast4,
                    [columns.ssn_full]: ssnFull,
                    [columns.ssn_first5_token]: tokenizedFirst5
                }];

                const insertRequest2 = new InsertRequest(table, insertData2);
                const insertOptions2 = new InsertOptions();
                insertOptions2.setReturnTokens(true);
                insertOptions2.setUpsertColumn(columns.ssn_full);
                insertOptions2.setContinueOnError(false);

                await client.vault(vaultId).insert(insertRequest2, insertOptions2);


                const finalToken = `${tokenizedFirst5}-${ssnLast4}`;

                results.push({
                    rowIndex: item.rowIndex,
                    token: finalToken,
                    error: null
                });


            } catch (error) {
                console.error(`SSN Partial tokenization failed for row ${item.rowIndex}:`, error.message);
                results.push({
                    rowIndex: item.rowIndex,
                    token: null,
                    error: error.message
                });
            }
        }

        return results;
    }

    /**
     * Detokenize partial tokens (e.g., DOB with preserved year)
     * For DOB: queries dob_partial_token table by month_day_token
     *
     * @param {Array} tokens - Array of {rowIndex, token, vaultId, dataType}
     * @returns {Promise<Array>} Array of {rowIndex, value, error}
     */
    async detokenizePartialBatch(tokens) {
        if (!tokens || tokens.length === 0) {
            return [];
        }


        const groups = {};
        for (const token of tokens) {
            const dt = token.dataType || 'UNKNOWN';
            if (!groups[dt]) groups[dt] = [];
            groups[dt].push(token);
        }

        const allResults = [];

        for (const [dataType, groupTokens] of Object.entries(groups)) {
            if (dataType === 'DOB') {
                const results = await this._detokenizeDOBPartialGroup(groupTokens);
                allResults.push(...results);
            } else if (dataType === 'SSN') {
                const results = await this._detokenizeSSNPartialGroup(groupTokens);
                allResults.push(...results);
            } else {
                const errorResults = groupTokens.map(t => ({
                    rowIndex: t.rowIndex,
                    value: null,
                    error: `Partial detokenization not supported for data type: ${dataType}`
                }));
                allResults.push(...errorResults);
            }
        }

        allResults.sort((a, b) => a.rowIndex - b.rowIndex);
        return allResults;
    }

    /**
     * Detokenize DOB partial tokens (query-based)
     * Token format: "YYYY-MM-DD" where MM-DD is tokenized
     * Queries dob_partial_token table by month_day_token to retrieve dob_full
     *
     * @private
     * @param {Array} tokens - Tokens to detokenize
     * @returns {Promise<Array>} Array of {rowIndex, value, error}
     */
    async _detokenizeDOBPartialGroup(tokens) {
        const vault = this.vaultsByDataType['DOB'];
        if (!vault) {
            console.error('No vault configured for DOB');
            return tokens.map(t => ({
                rowIndex: t.rowIndex,
                value: null,
                error: 'No vault configured for DOB'
            }));
        }

        const client = this._getOrInitializeClient('DOB');
        const { vaultId } = vault;
        const table = 'dob_partial_token';

        const results = [];

        for (const item of tokens) {
            try {
                const partialToken = item.token;

                if (!/^\d{4}-\d{2}-\d{2}$/.test(partialToken)) {
                    results.push({
                        rowIndex: item.rowIndex,
                        value: null,
                        error: `Invalid partial token format: ${partialToken}. Expected YYYY-MM-DD`
                    });
                    continue;
                }

                const monthDayToken = partialToken.substring(5);

                const query = `SELECT dob_full FROM ${table} WHERE month_day_token = '${monthDayToken}'`;
                const queryRequest = new QueryRequest(query);

                const response = await client.vault(vaultId).query(queryRequest);

                const fields = response.fields || [];
                if (fields.length === 0) {
                    results.push({
                        rowIndex: item.rowIndex,
                        value: null,
                        error: `No record found for month_day_token: ${monthDayToken}`
                    });
                    continue;
                }

                const dobFull = fields[0].dob_full;
                results.push({
                    rowIndex: item.rowIndex,
                    value: dobFull,
                    error: null
                });


            } catch (error) {
                console.error(`DOB Partial detokenization failed for ${item.token}:`, error.message);
                results.push({
                    rowIndex: item.rowIndex,
                    value: null,
                    error: error.message
                });
            }
        }

        return results;
    }

    /**
     * Detokenize SSN partial tokens (query-based)
     * Token format: "XXX-XX-XXXX" where XXX-XX is tokenized, last 4 is plaintext
     * Queries ssn_partial_token table by ssn_first5_token and ssn_last4 to retrieve ssn_full
     *
     * @private
     * @param {Array} tokens - Tokens to detokenize
     * @returns {Promise<Array>} Array of {rowIndex, value, error}
     */
    async _detokenizeSSNPartialGroup(tokens) {
        const vault = this.vaultsByDataType['SSN'];
        if (!vault) {
            console.error('No vault configured for SSN');
            return tokens.map(t => ({
                rowIndex: t.rowIndex,
                value: null,
                error: 'No vault configured for SSN'
            }));
        }

        const client = this._getOrInitializeClient('SSN');
        const { vaultId } = vault;
        const table = 'ssn_partial_token';

        const results = [];

        for (const item of tokens) {
            try {
                const partialToken = item.token;

                if (!/^\d{3}-\d{2}-\d{4}$/.test(partialToken)) {
                    results.push({
                        rowIndex: item.rowIndex,
                        value: null,
                        error: `Invalid partial token format: ${partialToken}. Expected XXX-XX-XXXX`
                    });
                    continue;
                }

                const first5Token = partialToken.substring(0, 6);
                const last4 = partialToken.substring(7);

                const query = `SELECT ssn_full FROM ${table} WHERE ssn_first5_token = '${first5Token}' AND ssn_last4 = '${last4}'`;
                const queryRequest = new QueryRequest(query);

                const response = await client.vault(vaultId).query(queryRequest);

                const fields = response.fields || [];
                if (fields.length === 0) {
                    results.push({
                        rowIndex: item.rowIndex,
                        value: null,
                        error: `No record found for ssn_first5_token: ${first5Token} and ssn_last4: ${last4}`
                    });
                    continue;
                }

                const ssnFull = fields[0].ssn_full;
                results.push({
                    rowIndex: item.rowIndex,
                    value: ssnFull,
                    error: null
                });


            } catch (error) {
                console.error(`SSN Partial detokenization failed for ${item.token}:`, error.message);
                results.push({
                    rowIndex: item.rowIndex,
                    value: null,
                    error: error.message
                });
            }
        }

        return results;
    }

    /**
     * Execute SQL queries against Skyflow vaults
     * Routes queries to appropriate vaults based on table names
     *
     * @param {Array} queries - Array of {rowIndex, sqlQuery}
     * @returns {Promise<Array>} Array of {rowIndex, results, error}
     */
    async executeQueryBatch(queries) {
        if (!queries || queries.length === 0) {
            return [];
        }


        const allResults = [];

        for (const query of queries) {
            try {
                const result = await this._executeQuery(query.sqlQuery);

                // Clean up Skyflow response: remove tokenizedData field (always empty for queries)
                const cleanedResults = (result.fields || []).map(record => {
                    const { tokenizedData, ...cleanRecord } = record;
                    return cleanRecord;
                });

                allResults.push({
                    rowIndex: query.rowIndex,
                    results: cleanedResults,
                    error: null
                });
            } catch (error) {
                console.error(`Query execution failed for rowIndex ${query.rowIndex}:`, error.message);
                allResults.push({
                    rowIndex: query.rowIndex,
                    results: [],
                    error: error.message
                });
            }
        }

        return allResults;
    }

    /**
     * Execute single SQL query - routes to appropriate vault
     * @private
     * @param {string} sqlQuery - SQL query to execute
     * @returns {Promise<Object>} Query response from Skyflow SDK
     */
    async _executeQuery(sqlQuery) {

        // Parse table name from SQL
        const tableName = this._extractTableName(sqlQuery);
        const dataType = this._mapTableToDataType(tableName);

        if (!dataType) {
            throw new Error(`Unknown table: ${tableName}. Cannot map to vault. Available tables: name, id, dob, ssn, email`);
        }

        // Get vault configuration for this data type
        const vault = this.vaultsByDataType[dataType];
        if (!vault) {
            throw new Error(`No vault configured for data type: ${dataType}`);
        }

        // Get or initialize SDK client for this vault
        const client = this._getOrInitializeClient(dataType);
        const { vaultId } = vault;


        // Execute query using Skyflow SDK
        const queryRequest = new QueryRequest(sqlQuery);
        const response = await client.vault(vaultId).query(queryRequest);


        return response;
    }

    /**
     * Extract table name from SQL query
     * @private
     * @param {string} sqlQuery - SQL query string
     * @returns {string} Table name
     */
    _extractTableName(sqlQuery) {
        // Simple regex to extract table name from "SELECT ... FROM table_name ..."
        // Handles: FROM table, FROM table WHERE, FROM table GROUP BY, etc.
        const match = sqlQuery.match(/FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)/i);
        if (!match) {
            throw new Error('Could not extract table name from query. Query must include FROM clause.');
        }
        return match[1].toLowerCase();
    }

    /**
     * Map table name to data type (vault identifier)
     * @private
     * @param {string} tableName - Table name from SQL query
     * @returns {string|null} Data type or null if not found
     */
    _mapTableToDataType(tableName) {
        // Map table names to data types based on config
        const tableMap = {
            'name': 'NAME',
            'id': 'ID',
            'dob': 'DOB',
            'ssn': 'SSN',
            'email': 'EMAIL'
        };

        return tableMap[tableName] || null;
    }

    /**
     * Clean up resources (if needed)
     */
    destroy() {
    }
}

module.exports = SkyflowClient;
