/**
 * Lambda Handler
 *
 * Routes Snowflake requests to appropriate Skyflow operations (tokenize/detokenize).
 * Supports data-type-specific routing (NAME, ID, DOB, SSN).
 */

const { loadConfig } = require('./config');
const SkyflowClient = require('./skyflow-client');

// Singleton client instance (reused across warm Lambda invocations)
let skyflowClient = null;
let config = null;
let configLoadTime = null;
let configLoadPromise = null; // Prevents concurrent config loads
const CONFIG_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

/**
 * Initialize Skyflow client with configuration
 *
 * Reloads config if:
 * - Not yet loaded
 * - Cache expired (older than 5 minutes)
 * - Client not initialized
 *
 * Uses promise locking to prevent race conditions when multiple
 * concurrent invocations try to reload config simultaneously.
 *
 * @returns {Promise<SkyflowClient>} Initialized client
 */
async function getSkyflowClient() {
    const now = Date.now();
    const configExpired = configLoadTime && (now - configLoadTime > CONFIG_CACHE_TTL_MS);

    if (!skyflowClient || !config || configExpired) {
        // Prevent multiple concurrent config loads with promise locking
        if (!configLoadPromise) {
            configLoadPromise = (async () => {
                try {
                    config = await loadConfig();
                    configLoadTime = Date.now();
                    skyflowClient = new SkyflowClient(config);
                    return skyflowClient;
                } finally {
                    // Clear promise to allow future reloads
                    configLoadPromise = null;
                }
            })();
        }
        return await configLoadPromise;
    }
    return skyflowClient;
}

/**
 * Parse Snowflake request and extract tokens for detokenization
 *
 * @param {Object} event - Lambda event from Snowflake
 * @param {string} dataType - Data type from header
 * @returns {Array} Array of {rowIndex, token, vaultId, dataType}
 */
function parseDetokenizeRequest(event, dataType) {
    if (!event || !event.data || !Array.isArray(event.data)) {
        throw new Error('Invalid Snowflake request format: missing "data" array');
    }

    const dataTypeUpper = dataType.toUpperCase();

    const tokens = event.data.map(row => {
        if (!Array.isArray(row) || row.length < 2) {
            throw new Error(`Invalid row format: ${JSON.stringify(row)}`);
        }

        const [rowIndex, token] = row;

        // Resolve vault ID from data type
        let vaultId = null;
        if (config.vaultsByDataType[dataTypeUpper]) {
            const vault = config.vaultsByDataType[dataTypeUpper];
            vaultId = vault.vaultId;
        }

        return {
            rowIndex,
            token,
            vaultId: vaultId || null,
            dataType: dataTypeUpper
        };
    });

    return tokens;
}

/**
 * Parse Snowflake request and extract values for tokenization
 *
 * @param {Object} event - Lambda event from Snowflake
 * @param {string} dataType - Data type (NAME, ID, DOB, SSN)
 * @returns {Array} Array of {rowIndex, value, vaultId, table, column}
 */
function parseTokenizeRequest(event, dataType) {
    if (!event || !event.data || !Array.isArray(event.data)) {
        throw new Error('Invalid Snowflake request format: missing "data" array');
    }

    const dataTypeUpper = dataType.toUpperCase();
    const vault = config.vaultsByDataType[dataTypeUpper];

    if (!vault) {
        throw new Error(`Unknown data type: ${dataType}. Available types: ${Object.keys(config.vaultsByDataType).join(', ')}`);
    }

    const values = event.data.map(row => {
        if (!Array.isArray(row) || row.length < 2) {
            throw new Error(`Invalid row format: ${JSON.stringify(row)}`);
        }

        const [rowIndex, value] = row;

        return {
            rowIndex,
            value,
            vaultId: vault.vaultId,
            table: vault.table,
            column: vault.column,
            dataType: dataTypeUpper
        };
    });

    return values;
}

/**
 * Parse Snowflake request and extract values for BYOT (Bring Your Own Token)
 *
 * @param {Object} event - Lambda event from Snowflake
 * @param {string} dataType - Data type (NAME, ID, DOB, SSN)
 * @returns {Array} Array of {rowIndex, value, token, vaultId, table, column}
 */
function parseBYOTRequest(event, dataType) {
    if (!event || !event.data || !Array.isArray(event.data)) {
        throw new Error('Invalid Snowflake request format: missing "data" array');
    }

    const dataTypeUpper = dataType.toUpperCase();
    const vault = config.vaultsByDataType[dataTypeUpper];

    if (!vault) {
        throw new Error(`Unknown data type: ${dataType}. Available types: ${Object.keys(config.vaultsByDataType).join(', ')}`);
    }

    const values = event.data.map(row => {
        if (!Array.isArray(row) || row.length < 3) {
            throw new Error(`Invalid BYOT row format: ${JSON.stringify(row)}. Expected [rowIndex, value, token]`);
        }

        const [rowIndex, value, token] = row;

        return {
            rowIndex,
            value,
            token,
            vaultId: vault.vaultId,
            table: vault.table,
            column: vault.column,
            dataType: dataTypeUpper
        };
    });

    return values;
}

/**
 * Format detokenization results for Snowflake response
 *
 * @param {Array} results - Array of {rowIndex, value, error}
 * @returns {Object} Snowflake-formatted response
 */
function formatDetokenizeResponse(results) {
    const data = results.map(result => {
        if (result.error) {
            return [result.rowIndex, `ERROR: ${result.error}`];
        }
        return [result.rowIndex, result.value];
    });

    return {
        data: data
    };
}

/**
 * Format tokenization results for Snowflake response
 *
 * @param {Array} results - Array of {rowIndex, token, error}
 * @returns {Object} Snowflake-formatted response
 */
function formatTokenizeResponse(results) {
    const data = results.map(result => {
        if (result.error) {
            return [result.rowIndex, `ERROR: ${result.error}`];
        }
        return [result.rowIndex, result.token];
    });

    return {
        data: data
    };
}

/**
 * Parse Snowflake request and extract SQL queries for vault query operation
 *
 * @param {Object} event - Lambda event from Snowflake
 * @returns {Array} Array of {rowIndex, sqlQuery}
 */
function parseQueryRequest(event) {
    if (!event || !event.data || !Array.isArray(event.data)) {
        throw new Error('Invalid Snowflake request format: missing "data" array');
    }

    const queries = event.data.map(row => {
        if (!Array.isArray(row) || row.length < 2) {
            throw new Error(`Invalid row format: ${JSON.stringify(row)}`);
        }

        const [rowIndex, sqlQuery] = row;

        if (!sqlQuery || typeof sqlQuery !== 'string') {
            throw new Error(`Invalid SQL query: ${JSON.stringify(sqlQuery)}`);
        }

        return {
            rowIndex,
            sqlQuery
        };
    });

    return queries;
}

/**
 * Format query results for Snowflake response
 *
 * @param {Array} results - Array of {rowIndex, results, error}
 * @returns {Object} Snowflake-formatted response
 */
function formatQueryResponse(results) {
    const data = results.map(result => {
        if (result.error) {
            return [result.rowIndex, { error: result.error }];
        }
        // Return query results as native array (Snowflake auto-parses to VARIANT)
        return [result.rowIndex, result.results];
    });

    return {
        data: data
    };
}

/**
 * Extract operation from HTTP headers
 *
 * Snowflake prepends 'sf-custom-' to all custom headers specified in HEADERS clause.
 * See: https://docs.snowflake.com/en/sql-reference/sql/create-external-function
 *
 * @param {Object} event - Lambda event
 * @returns {string} 'tokenize' or 'detokenize'
 */
function extractOperation(event) {
    // Convert all header keys to lowercase for case-insensitive lookup
    const headers = event.headers || {};
    const lowerHeaders = Object.keys(headers).reduce((acc, key) => {
        acc[key.toLowerCase()] = headers[key];
        return acc;
    }, {});

    const operation = lowerHeaders['sf-custom-x-operation'];

    if (!operation) {
        throw new Error('Missing required header: sf-custom-x-operation (from Snowflake HEADERS clause)');
    }

    const validOperations = ['tokenize', 'detokenize', 'tokenize_oneway', 'tokenize_partial', 'detokenize_partial', 'byot', 'query'];
    if (!validOperations.includes(operation)) {
        throw new Error(`Invalid operation: ${operation}. Must be one of: ${validOperations.join(', ')}`);
    }

    return operation.toLowerCase();
}

/**
 * Extract data type from HTTP headers
 *
 * Snowflake prepends 'sf-custom-' to all custom headers specified in HEADERS clause.
 * See: https://docs.snowflake.com/en/sql-reference/sql/create-external-function
 *
 * @param {Object} event - Lambda event
 * @returns {string} Data type in uppercase (NAME, ID, DOB, SSN)
 */
function extractDataType(event) {
    // Convert all header keys to lowercase for case-insensitive lookup
    const headers = event.headers || {};
    const lowerHeaders = Object.keys(headers).reduce((acc, key) => {
        acc[key.toLowerCase()] = headers[key];
        return acc;
    }, {});

    const dataType = lowerHeaders['sf-custom-x-data-type'];

    if (!dataType) {
        throw new Error('Missing required header: sf-custom-x-data-type (from Snowflake HEADERS clause)');
    }

    return dataType.toUpperCase();
}

/**
 * Extract Snowflake caller context from CONTEXT_HEADERS
 *
 * Snowflake prepends 'sf-context-' to context function names when creating HTTP headers.
 * See: https://docs.snowflake.com/en/sql-reference/sql/create-external-function
 *
 * @param {Object} event - Lambda event
 * @returns {Object} Caller context {user, role, account, ipAddress}
 */
function extractCallerContext(event) {
    // Convert all header keys to lowercase for case-insensitive lookup
    const headers = event.headers || {};
    const lowerHeaders = Object.keys(headers).reduce((acc, key) => {
        acc[key.toLowerCase()] = headers[key];
        return acc;
    }, {});

    return {
        user: lowerHeaders['sf-context-current-user'] || 'UNKNOWN',
        role: lowerHeaders['sf-context-current-role'] || 'UNKNOWN',
        account: lowerHeaders['sf-context-current-account'] || 'UNKNOWN',
        ipAddress: lowerHeaders['sf-context-current-ip-address'] || 'UNKNOWN'
    };
}

/**
 * Main Lambda handler
 *
 * @param {Object} event - Lambda event from API Gateway
 * @param {Object} context - Lambda context
 * @returns {Promise<Object>} API Gateway response
 */
async function handler(event, context) {
    try {
        // Extract operation and data type from headers
        const operation = extractOperation(event);
        const dataType = operation === 'query' ? null : extractDataType(event);
        const caller = extractCallerContext(event);

        // Parse request body (API Gateway format)
        let requestData = event;
        if (event.body && typeof event.body === 'string') {
            requestData = JSON.parse(event.body);
        }

        // Handle empty data
        if (!requestData.data || requestData.data.length === 0) {
            return {
                statusCode: 200,
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ data: [] })
            };
        }

        // Get Skyflow client instance
        const client = await getSkyflowClient();

        // Route to appropriate operation
        if (operation === 'tokenize') {
            // Tokenization
            const values = parseTokenizeRequest(requestData, dataType);
            const results = await client.tokenizeBatch(values);
            const response = formatTokenizeResponse(results);

            return {
                statusCode: 200,
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(response)
            };

        } else if (operation === 'tokenize_oneway') {
            const values = parseTokenizeRequest(requestData, dataType);
            const results = await client.tokenizeOneWayBatch(values);
            const response = formatTokenizeResponse(results);

            return {
                statusCode: 200,
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(response)
            };

        } else if (operation === 'tokenize_partial') {
            const values = parseTokenizeRequest(requestData, dataType);
            const results = await client.tokenizePartialBatch(values);
            const response = formatTokenizeResponse(results);

            return {
                statusCode: 200,
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(response)
            };

        } else if (operation === 'detokenize_partial') {
            const tokens = parseDetokenizeRequest(requestData, dataType);
            const results = await client.detokenizePartialBatch(tokens);
            const response = formatDetokenizeResponse(results);

            return {
                statusCode: 200,
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(response)
            };

        } else if (operation === 'byot') {
            // BYOT (Bring Your Own Token)
            const values = parseBYOTRequest(requestData, dataType);
            const results = await client.byotBatch(values);
            const response = formatTokenizeResponse(results);

            return {
                statusCode: 200,
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(response)
            };

        } else if (operation === 'query') {
            // Query operation
            const queries = parseQueryRequest(requestData);
            const results = await client.executeQueryBatch(queries);
            const response = formatQueryResponse(results);

            return {
                statusCode: 200,
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(response)
            };

        } else {
            // Detokenization
            const tokens = parseDetokenizeRequest(requestData, dataType);
            const results = await client.detokenizeBatch(tokens);
            const response = formatDetokenizeResponse(results);

            return {
                statusCode: 200,
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(response)
            };
        }

    } catch (error) {
        console.error('Lambda error:', error);
        console.error('Stack trace:', error.stack);

        // Return error in Snowflake format
        return {
            statusCode: 500,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                data: [[0, `ERROR: ${error.message}`]]
            })
        };
    }
}

// Export handler
exports.handler = handler;
