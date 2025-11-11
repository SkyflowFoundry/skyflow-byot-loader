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
                    if (configExpired) {
                        console.log('Config cache expired, reloading from Secrets Manager');
                    }

                    config = await loadConfig();
                    configLoadTime = Date.now();
                    skyflowClient = new SkyflowClient(config);
                    console.log('Initialized SkyflowClient', {
                        cacheExpired: configExpired,
                        loadTime: new Date(configLoadTime).toISOString()
                    });
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

    console.log(`Parsed ${tokens.length} tokens from Snowflake detokenize request for ${dataType}`);
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

    console.log(`Parsed ${values.length} values from Snowflake tokenize request for ${dataType}`);
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

    if (operation !== 'tokenize' && operation !== 'detokenize') {
        throw new Error(`Invalid operation: ${operation}. Must be 'tokenize' or 'detokenize'`);
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
    console.log('Lambda invoked', {
        requestId: context.requestId,
        functionName: context.functionName,
        remainingTimeMs: context.getRemainingTimeInMillis()
    });

    try {
        // Extract operation and data type from headers
        const operation = extractOperation(event);
        const dataType = extractDataType(event);
        const caller = extractCallerContext(event);

        console.log(`Operation: ${operation}, Data Type: ${dataType}`, {
            caller: {
                user: caller.user,
                role: caller.role,
                account: caller.account,
                ipAddress: caller.ipAddress
            }
        });

        // Parse request body (API Gateway format)
        let requestData = event;
        if (event.body && typeof event.body === 'string') {
            console.log('Parsing JSON body from API Gateway');
            requestData = JSON.parse(event.body);
        }

        // Handle empty data
        if (!requestData.data || requestData.data.length === 0) {
            console.log('No data to process');
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

            console.log(`Starting tokenization of ${values.length} values for ${dataType}`);
            const startTime = Date.now();

            const results = await client.tokenizeBatch(values);

            const elapsed = Date.now() - startTime;
            const successCount = results.filter(r => !r.error).length;
            const errorCount = results.filter(r => r.error).length;

            console.log(`Tokenization complete in ${elapsed}ms`, {
                totalValues: values.length,
                successCount,
                errorCount,
                throughput: Math.round(values.length / (elapsed / 1000))
            });

            const response = formatTokenizeResponse(results);

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

            console.log(`Starting detokenization of ${tokens.length} tokens`);
            const startTime = Date.now();

            const results = await client.detokenizeBatch(tokens);

            const elapsed = Date.now() - startTime;
            const successCount = results.filter(r => !r.error).length;
            const errorCount = results.filter(r => r.error).length;

            console.log(`Detokenization complete in ${elapsed}ms`, {
                totalTokens: tokens.length,
                successCount,
                errorCount,
                throughput: Math.round(tokens.length / (elapsed / 1000))
            });

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
