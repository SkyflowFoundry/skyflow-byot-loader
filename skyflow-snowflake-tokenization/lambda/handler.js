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
const CONFIG_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

/**
 * Initialize Skyflow client with configuration
 *
 * Reloads config if:
 * - Not yet loaded
 * - Cache expired (older than 5 minutes)
 * - Client not initialized
 *
 * @returns {Promise<SkyflowClient>} Initialized client
 */
async function getSkyflowClient() {
    const now = Date.now();
    const configExpired = configLoadTime && (now - configLoadTime > CONFIG_CACHE_TTL_MS);

    if (!skyflowClient || !config || configExpired) {
        if (configExpired) {
            console.log('Config cache expired, reloading from Secrets Manager');
        }

        config = await loadConfig();
        configLoadTime = now;
        skyflowClient = new SkyflowClient(config);
        console.log('Initialized SkyflowClient', {
            cacheExpired: configExpired,
            loadTime: new Date(configLoadTime).toISOString()
        });
    }
    return skyflowClient;
}

/**
 * Parse Snowflake request and extract tokens for detokenization
 *
 * @param {Object} event - Lambda event from Snowflake
 * @returns {Array} Array of {rowIndex, token, vaultId}
 */
function parseDetokenizeRequest(event) {
    if (!event || !event.data || !Array.isArray(event.data)) {
        throw new Error('Invalid Snowflake request format: missing "data" array');
    }

    const tokens = event.data.map(row => {
        if (!Array.isArray(row) || row.length < 2) {
            throw new Error(`Invalid row format: ${JSON.stringify(row)}`);
        }

        const [rowIndex, token, dataTypeOrVaultId] = row;

        // Resolve vault ID and data type
        let vaultId = dataTypeOrVaultId;
        let dataType = null;

        if (dataTypeOrVaultId && config.vaultsByDataType[dataTypeOrVaultId.toUpperCase()]) {
            const vault = config.vaultsByDataType[dataTypeOrVaultId.toUpperCase()];
            vaultId = vault.vaultId;
            dataType = dataTypeOrVaultId.toUpperCase();
            console.log(`Resolved data type '${dataTypeOrVaultId}' to vault ID: ${vaultId}`);
        }

        return {
            rowIndex,
            token,
            vaultId: vaultId || null,
            dataType: dataType
        };
    });

    console.log(`Parsed ${tokens.length} tokens from Snowflake detokenize request`);
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
 * Extract data type from API Gateway path
 *
 * Examples:
 *   /tokenize/name -> NAME
 *   /detokenize/ssn -> SSN
 *
 * @param {string} path - API Gateway path
 * @returns {string|null} Data type or null
 */
function extractDataTypeFromPath(path) {
    const match = path.match(/\/(tokenize|detokenize)\/(\w+)$/);
    if (match) {
        return match[2].toUpperCase();
    }
    return null;
}

/**
 * Determine operation from API Gateway path
 *
 * @param {string} path - API Gateway path
 * @returns {string} 'tokenize' or 'detokenize'
 */
function determineOperation(path) {
    if (path.includes('/tokenize')) {
        return 'tokenize';
    }
    if (path.includes('/detokenize')) {
        return 'detokenize';
    }
    throw new Error('Invalid path: must include /tokenize or /detokenize');
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
        path: event.path || event.rawPath,
        remainingTimeMs: context.getRemainingTimeInMillis()
    });

    try {
        // Determine operation and data type from path
        const path = event.path || event.rawPath || '';
        const operation = determineOperation(path);
        const dataTypeFromPath = extractDataTypeFromPath(path);

        console.log(`Operation: ${operation}, Data Type: ${dataTypeFromPath || 'not specified'}`);

        // Parse request body (API Gateway format)
        let requestData = event;
        if (event.body && typeof event.body === 'string') {
            console.log('Parsing JSON body from API Gateway');
            requestData = JSON.parse(event.body);
        }

        // If we have a data type from the path, inject it into each row if needed
        if (dataTypeFromPath && requestData.data && Array.isArray(requestData.data)) {
            requestData.data = requestData.data.map(row => {
                // If row only has 2 elements (rowIndex, token/value), add dataType as 3rd element
                if (Array.isArray(row) && row.length === 2) {
                    return [...row, dataTypeFromPath];
                }
                return row;
            });
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
            if (!dataTypeFromPath) {
                throw new Error('Data type not specified in path for tokenization');
            }

            const values = parseTokenizeRequest(requestData, dataTypeFromPath);

            console.log(`Starting tokenization of ${values.length} values for ${dataTypeFromPath}`);
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
            const tokens = parseDetokenizeRequest(requestData);

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
