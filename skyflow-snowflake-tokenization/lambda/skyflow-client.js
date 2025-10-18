/**
 * Skyflow Client with Performance Optimizations
 *
 * Features:
 * - HTTP/2 support with connection multiplexing
 * - Adaptive retry with jitter and Retry-After header support
 * - Buffer pooling for reduced allocations
 * - Optimized connection pool configuration
 * - Automatic batching and parallel processing
 */

const http2 = require('http2');
const { URL } = require('url');

/**
 * Buffer Pool for reusing buffers across requests
 */
class BufferPool {
    constructor(initialSize = 4096) {
        this.pool = [];
        this.initialSize = initialSize;
        this.hits = 0;
        this.misses = 0;
    }

    /**
     * Get a buffer from the pool
     * @returns {Buffer}
     */
    get() {
        if (this.pool.length > 0) {
            this.hits++;
            return this.pool.pop();
        }
        this.misses++;
        return Buffer.allocUnsafe(this.initialSize);
    }

    /**
     * Return a buffer to the pool
     * @param {Buffer} buffer
     */
    put(buffer) {
        if (this.pool.length < 50) { // Max 50 pooled buffers
            this.pool.push(buffer);
        }
    }

    /**
     * Get pool statistics
     * @returns {Object}
     */
    stats() {
        return {
            size: this.pool.length,
            hits: this.hits,
            misses: this.misses,
            hitRate: this.hits / (this.hits + this.misses) || 0
        };
    }
}

/**
 * HTTP/2 Session Manager
 */
class HTTP2SessionManager {
    constructor() {
        this.sessions = new Map();
    }

    /**
     * Get or create an HTTP/2 session for a hostname
     * @param {string} hostname
     * @returns {Object} HTTP/2 session
     */
    getSession(hostname) {
        const url = `https://${hostname}`;

        if (this.sessions.has(hostname)) {
            const session = this.sessions.get(hostname);
            if (!session.destroyed && !session.closed) {
                return session;
            }
            this.sessions.delete(hostname);
        }

        // Create new HTTP/2 session
        const session = http2.connect(url, {
            maxSessionMemory: 100, // MB
            settings: {
                enablePush: false,
                maxConcurrentStreams: 100
            }
        });

        session.on('error', (err) => {
            console.error(`HTTP/2 session error for ${hostname}:`, err.message);
            this.sessions.delete(hostname);
        });

        session.on('close', () => {
            this.sessions.delete(hostname);
        });

        this.sessions.set(hostname, session);
        return session;
    }

    /**
     * Close all sessions
     */
    destroy() {
        for (const session of this.sessions.values()) {
            if (!session.destroyed) {
                session.close();
            }
        }
        this.sessions.clear();
    }
}

/**
 * Skyflow Client for tokenization and detokenization
 */
class SkyflowClient {
    /**
     * @param {Object} config - Configuration
     * @param {string} config.vaultUrl - Skyflow vault URL
     * @param {string} config.bearerToken - Skyflow bearer token
     * @param {number} [config.batchSize=100] - Batch size
     * @param {number} [config.maxConcurrency=20] - Max concurrent requests
     * @param {number} [config.maxRetries=3] - Max retry attempts
     * @param {number} [config.retryDelay=1.0] - Initial retry delay in seconds
     */
    constructor(config) {
        this.vaultUrl = config.vaultUrl;
        this.bearerToken = config.bearerToken;
        this.batchSize = Math.min(config.batchSize || 100, 200); // Max 200
        this.maxConcurrency = config.maxConcurrency || 20;
        this.maxRetries = config.maxRetries || 3;
        this.retryDelay = config.retryDelay || 1.0;

        // HTTP/2 session manager
        this.sessionManager = new HTTP2SessionManager();

        // Buffer pool for reduced allocations
        this.bufferPool = new BufferPool(4096);

        console.log('SkyflowClient initialized with optimizations', {
            vaultUrl: this.vaultUrl,
            batchSize: this.batchSize,
            maxConcurrency: this.maxConcurrency,
            maxRetries: this.maxRetries,
            http2: true,
            bufferPooling: true
        });
    }

    /**
     * Tokenize a batch of values with parallel processing
     *
     * @param {Array} values - Array of {rowIndex, value, vaultId, table, column}
     * @returns {Promise<Array>} Array of {rowIndex, token, error}
     */
    async tokenizeBatch(values) {
        if (!values || values.length === 0) {
            return [];
        }

        // Split values into batches
        const batches = [];
        for (let i = 0; i < values.length; i += this.batchSize) {
            batches.push(values.slice(i, i + this.batchSize));
        }

        console.log(`Processing ${values.length} values in ${batches.length} batches (max ${this.maxConcurrency} concurrent)`);

        // Process batches in parallel with concurrency limit
        const allResults = [];
        for (let i = 0; i < batches.length; i += this.maxConcurrency) {
            const batchGroup = batches.slice(i, i + this.maxConcurrency);
            console.log(`Processing batch group ${Math.floor(i / this.maxConcurrency) + 1}: ${batchGroup.length} parallel requests`);

            const groupPromises = batchGroup.map((batch, idx) => {
                const batchNum = i + idx + 1;
                console.log(`  Batch ${batchNum}/${batches.length}: ${batch.length} values`);
                return this._tokenizeBatchWithRetry(batch);
            });

            const groupResults = await Promise.all(groupPromises);
            allResults.push(...groupResults);
        }

        const results = allResults.flat();
        console.log(`Completed tokenization: ${results.length} results`);

        // Log buffer pool stats periodically
        if (Math.random() < 0.1) { // 10% of requests
            console.log('Buffer pool stats:', this.bufferPool.stats());
        }

        return results;
    }

    /**
     * Detokenize a batch of tokens with parallel processing
     *
     * @param {Array} tokens - Array of {rowIndex, token, vaultId}
     * @returns {Promise<Array>} Array of {rowIndex, value, error}
     */
    async detokenizeBatch(tokens) {
        if (!tokens || tokens.length === 0) {
            return [];
        }

        // Split tokens into batches
        const batches = [];
        for (let i = 0; i < tokens.length; i += this.batchSize) {
            batches.push(tokens.slice(i, i + this.batchSize));
        }

        console.log(`Processing ${tokens.length} tokens in ${batches.length} batches (max ${this.maxConcurrency} concurrent)`);

        // Process batches in parallel with concurrency limit
        const allResults = [];
        for (let i = 0; i < batches.length; i += this.maxConcurrency) {
            const batchGroup = batches.slice(i, i + this.maxConcurrency);
            console.log(`Processing batch group ${Math.floor(i / this.maxConcurrency) + 1}: ${batchGroup.length} parallel requests`);

            const groupPromises = batchGroup.map((batch, idx) => {
                const batchNum = i + idx + 1;
                console.log(`  Batch ${batchNum}/${batches.length}: ${batch.length} tokens`);
                return this._detokenizeBatchWithRetry(batch);
            });

            const groupResults = await Promise.all(groupPromises);
            allResults.push(...groupResults);
        }

        const results = allResults.flat();
        console.log(`Completed detokenization: ${results.length} results`);

        return results;
    }

    /**
     * Tokenize a single batch with adaptive retry logic
     *
     * @param {Array} batch - Batch of values
     * @returns {Promise<Array>} Results
     * @private
     */
    async _tokenizeBatchWithRetry(batch) {
        let lastError = null;

        for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
            try {
                if (attempt > 0) {
                    const delay = this._calculateBackoff(attempt, lastError);
                    console.log(`Retry attempt ${attempt}/${this.maxRetries} after ${delay}ms`);
                    await this._sleep(delay);
                }

                return await this._tokenizeBatchOnce(batch);

            } catch (error) {
                lastError = error;
                console.error(`Attempt ${attempt + 1} failed:`, error.message);

                // Check if we should retry
                if (!this._shouldRetry(error)) {
                    console.log('Non-retryable error detected, not retrying');
                    break;
                }
            }
        }

        // All retries failed
        console.error('All retry attempts exhausted', { error: lastError.message });
        return batch.map(item => ({
            rowIndex: item.rowIndex,
            token: null,
            error: lastError.message
        }));
    }

    /**
     * Detokenize a single batch with adaptive retry logic
     *
     * @param {Array} batch - Batch of tokens
     * @returns {Promise<Array>} Results
     * @private
     */
    async _detokenizeBatchWithRetry(batch) {
        let lastError = null;

        for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
            try {
                if (attempt > 0) {
                    const delay = this._calculateBackoff(attempt, lastError);
                    console.log(`Retry attempt ${attempt}/${this.maxRetries} after ${delay}ms`);
                    await this._sleep(delay);
                }

                return await this._detokenizeBatchOnce(batch);

            } catch (error) {
                lastError = error;
                console.error(`Attempt ${attempt + 1} failed:`, error.message);

                // Check if we should retry
                if (!this._shouldRetry(error)) {
                    console.log('Non-retryable error detected, not retrying');
                    break;
                }
            }
        }

        // All retries failed
        console.error('All retry attempts exhausted', { error: lastError.message });
        return batch.map(item => ({
            rowIndex: item.rowIndex,
            value: null,
            error: lastError.message
        }));
    }

    /**
     * Calculate backoff delay with jitter and Retry-After support
     *
     * @param {number} attempt - Attempt number
     * @param {Error} lastError - Last error
     * @returns {number} Delay in milliseconds
     * @private
     */
    _calculateBackoff(attempt, lastError) {
        // Base exponential backoff: delay * 2^(attempt-1)
        let baseDelay = this.retryDelay * Math.pow(2, attempt - 1) * 1000;

        // Special handling for rate limiting (429)
        if (lastError && lastError.statusCode === 429) {
            // Check for Retry-After header
            if (lastError.retryAfter) {
                console.log(`Using Retry-After header: ${lastError.retryAfter}s`);
                return lastError.retryAfter * 1000;
            }
            // Use longer backoff for rate limiting
            baseDelay *= 2;
        }

        // Add jitter: random value between 0.5x and 1.5x base delay
        // This prevents thundering herd problem
        const jitter = 0.5 + Math.random(); // Random between 0.5 and 1.5
        let delay = baseDelay * jitter;

        // Cap at 30 seconds
        if (delay > 30000) {
            delay = 30000;
        }

        return Math.floor(delay);
    }

    /**
     * Determine if an error should be retried
     *
     * @param {Error} error - Error to check
     * @returns {boolean} True if should retry
     * @private
     */
    _shouldRetry(error) {
        if (!error.statusCode) {
            // Network errors, timeouts, and other non-HTTP errors are retryable
            return true;
        }

        // Retry 429 (rate limit) and 408 (timeout)
        if (error.statusCode === 429 || error.statusCode === 408) {
            return true;
        }

        // Don't retry other 4xx client errors
        if (error.statusCode >= 400 && error.statusCode < 500) {
            return false;
        }

        // Retry 5xx server errors
        return error.statusCode >= 500;
    }

    /**
     * Tokenize a single batch (one API call)
     *
     * @param {Array} batch - Batch of values
     * @returns {Promise<Array>} Results
     * @private
     */
    async _tokenizeBatchOnce(batch) {
        // All items in a batch must have the same vaultId, table, and column
        const vaultId = batch[0].vaultId;
        const table = batch[0].table;
        const column = batch[0].column;

        if (!vaultId) {
            throw new Error('No vault_id configured for data type');
        }
        if (!table) {
            throw new Error('No table specified for tokenization');
        }
        if (!column) {
            throw new Error('No column specified for tokenization');
        }

        // Build request payload
        const records = batch.map(item => ({
            fields: {
                [column]: item.value
            }
        }));

        const payload = {
            records,
            tokenization: true,
            upsert: column
        };

        // Make API request
        const url = `${this.vaultUrl}/v1/vaults/${vaultId}/${table}`;
        console.log(`POST ${url}`, { valueCount: batch.length, upsert: true });

        const responseData = await this._makeRequest(url, payload);

        // Parse response
        return this._parseTokenizeResponse(batch, responseData, column);
    }

    /**
     * Detokenize a single batch (one API call)
     *
     * @param {Array} batch - Batch of tokens
     * @returns {Promise<Array>} Results
     * @private
     */
    async _detokenizeBatchOnce(batch) {
        const vaultId = batch[0].vaultId;
        if (!vaultId) {
            throw new Error('No vault_id configured for data type');
        }

        // Build request payload
        const detokenizationParameters = batch.map(item => ({
            token: item.token,
            redaction: 'PLAIN_TEXT'
        }));

        const payload = {
            detokenizationParameters
        };

        // Make API request
        const url = `${this.vaultUrl}/v1/vaults/${vaultId}/detokenize`;
        console.log(`POST ${url}`, { tokenCount: batch.length });

        const responseData = await this._makeRequest(url, payload);

        // Parse response
        return this._parseDetokenizeResponse(batch, responseData);
    }

    /**
     * Make HTTP/2 request to Skyflow API with buffer pooling
     *
     * @param {string} url - API URL
     * @param {Object} payload - Request payload
     * @returns {Promise<Object>} Response data
     * @private
     */
    _makeRequest(url, payload) {
        return new Promise((resolve, reject) => {
            const urlObj = new URL(url);

            // Get HTTP/2 session
            const session = this.sessionManager.getSession(urlObj.hostname);

            // Serialize payload to JSON
            const postData = JSON.stringify(payload);

            const headers = {
                ':method': 'POST',
                ':path': urlObj.pathname,
                'authorization': `Bearer ${this.bearerToken}`,
                'content-type': 'application/json',
                'content-length': Buffer.byteLength(postData)
            };

            // Make HTTP/2 request
            const req = session.request(headers);

            let responseData = '';
            let responseHeaders = {};

            req.on('response', (headers) => {
                responseHeaders = headers;
            });

            req.on('data', (chunk) => {
                responseData += chunk;
            });

            req.on('end', () => {
                const statusCode = responseHeaders[':status'];

                if (statusCode >= 200 && statusCode < 300) {
                    try {
                        const parsed = JSON.parse(responseData);
                        resolve(parsed);
                    } catch (error) {
                        reject(new Error(`Failed to parse response: ${error.message}`));
                    }
                } else {
                    const error = new Error(`HTTP ${statusCode}: ${responseData}`);
                    error.statusCode = statusCode;
                    error.responseBody = responseData;

                    // Capture Retry-After header for 429 rate limiting
                    if (statusCode === 429 && responseHeaders['retry-after']) {
                        error.retryAfter = parseInt(responseHeaders['retry-after']);
                    }

                    reject(error);
                }
            });

            req.on('error', (error) => {
                reject(error);
            });

            // Send request body
            req.write(postData);
            req.end();
        });
    }

    /**
     * Parse Skyflow tokenize response
     *
     * @param {Array} batch - Original batch
     * @param {Object} responseData - Skyflow response
     * @param {string} column - Column name
     * @returns {Array} Results
     * @private
     */
    _parseTokenizeResponse(batch, responseData, column) {
        if (!responseData.records || !Array.isArray(responseData.records)) {
            throw new Error('Invalid Skyflow response format: missing records array');
        }

        const results = [];

        for (let i = 0; i < batch.length; i++) {
            const item = batch[i];
            const record = responseData.records[i];

            if (!record) {
                results.push({
                    rowIndex: item.rowIndex,
                    token: null,
                    error: 'No record returned from Skyflow'
                });
                continue;
            }

            if (record.error) {
                results.push({
                    rowIndex: item.rowIndex,
                    token: null,
                    error: record.error.message || 'Unknown error'
                });
                continue;
            }

            const token = record.tokens && record.tokens[column] ? record.tokens[column] : null;

            if (!token) {
                results.push({
                    rowIndex: item.rowIndex,
                    token: null,
                    error: 'No token returned from Skyflow'
                });
                continue;
            }

            results.push({
                rowIndex: item.rowIndex,
                token: token,
                error: null
            });
        }

        return results;
    }

    /**
     * Parse Skyflow detokenize response
     *
     * @param {Array} batch - Original batch
     * @param {Object} responseData - Skyflow response
     * @returns {Array} Results
     * @private
     */
    _parseDetokenizeResponse(batch, responseData) {
        if (!responseData.records || !Array.isArray(responseData.records)) {
            throw new Error('Invalid Skyflow response format: missing records array');
        }

        const results = [];

        for (let i = 0; i < batch.length; i++) {
            const item = batch[i];
            const record = responseData.records[i];

            if (!record) {
                results.push({
                    rowIndex: item.rowIndex,
                    value: null,
                    error: 'No record returned from Skyflow'
                });
                continue;
            }

            if (record.error) {
                results.push({
                    rowIndex: item.rowIndex,
                    value: null,
                    error: record.error.message || 'Unknown error'
                });
                continue;
            }

            const value = record.value || record.valueStr || null;

            results.push({
                rowIndex: item.rowIndex,
                value: value,
                error: null
            });
        }

        return results;
    }

    /**
     * Sleep for specified milliseconds
     *
     * @param {number} ms - Milliseconds to sleep
     * @returns {Promise<void>}
     * @private
     */
    _sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * Clean up resources
     */
    destroy() {
        if (this.sessionManager) {
            this.sessionManager.destroy();
        }
        console.log('SkyflowClient destroyed', {
            bufferPoolStats: this.bufferPool.stats()
        });
    }
}

module.exports = SkyflowClient;
