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

        // Detect auth type
        this.isJwtAuth = !config.credentials.apiKey;

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
        const client = this.skyflowClients[dataType];
        const { table, column, vaultId } = vault;
        const uniqueTransformedValues = Array.from(transformedMap.keys());
        const processedResults = [];

        // Helper: batch tokenize unique transformed values
        const tokenizeBatch = async (transformedVals) => {
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
                        error: errorForIndex.error || 'Unknown error'
                    });
                } else {
                    const inserted = insertedFields[i];
                    if (inserted && inserted[column]) {
                        batchResults.push({
                            transformedValue: val,
                            token: inserted[column],
                            error: null
                        });
                    } else {
                        batchResults.push({
                            transformedValue: val,
                            token: null,
                            error: 'No token returned from SDK'
                        });
                    }
                }
            }
            return batchResults;
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
            tokenMap.set(result.transformedValue, { token: result.token, error: result.error });
        }

        const finalResults = [];
        // Add skipped results
        finalResults.push(...skippedResults);
        // Add processed results for all rowIndexes
        for (const [transformedValue, rowInfos] of transformedMap.entries()) {
            if (tokenMap.has(transformedValue)) {
                const { token, error } = tokenMap.get(transformedValue);
                for (const rowInfo of rowInfos) {
                    finalResults.push({
                        rowIndex: rowInfo.rowIndex,
                        token,
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
        const client = this.skyflowClients[dataType];
        const { vaultId } = vault;
        const processedResults = [];

        // Helper: batch detokenize unique tokens
        const detokenizeBatch = async (tokenObjs) => {
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
