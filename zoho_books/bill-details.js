const axios = require('axios');
const { Pool } = require('pg');

// Add at the top with other imports
const fs = require('fs');

// Add after other constants
const PROGRESS_FILE = './bill_sync_progress.json';

// Add this function to manage progress
async function loadProgress() {
    try {
        if (fs.existsSync(PROGRESS_FILE)) {
            const data = fs.readFileSync(PROGRESS_FILE, 'utf8');
            return JSON.parse(data);
        }
    } catch (error) {
        console.log('No previous progress found, starting fresh');
    }
    return { lastProcessedIndex: -1 };
}

async function saveProgress(index) {
    fs.writeFileSync(PROGRESS_FILE, JSON.stringify({ lastProcessedIndex: index }));
}

// Constants (reuse the same credentials)
const CLIENT_ID = '1000.4SMTI5UB88N1HHQ37IB14Z8FUSXK2M';
const CLIENT_SECRET = '4c14a5eb4e3b0643c0a515864d05e21def40aad09d';
const REFRESH_TOKEN = '1000.91e0505679d2e67f49d2a592178e57cc.9d679633039bfbc2f90eb84225270590';
const API_BASE_URL = 'https://www.zohoapis.in/books/v3';
const TOKEN_REFRESH_URL = 'https://accounts.zoho.in/oauth/v2/token';

// Database connection (reuse the same connection)
const pool = new Pool({
    host: 'tec-ext-db.clymeiomojq1.ap-southeast-1.rds.amazonaws.com',
    database: 'postgres',
    user: 'postgres',
    password: 'T5hHf2CvhTW4fpT1cmJ9',
    port: 5432,
});

let accessToken = null;

// Reuse the same token refresh function
async function refreshAccessToken() {
    try {
        const response = await axios.post(TOKEN_REFRESH_URL, null, {
            params: {
                refresh_token: REFRESH_TOKEN,
                client_id: CLIENT_ID,
                client_secret: CLIENT_SECRET,
                grant_type: 'refresh_token',
            },
        });
        accessToken = response.data.access_token;
    } catch (error) {
        console.error('Error refreshing access token:', error.response?.data || error.message);
        throw error;
    }
}

// Function to fetch bill details from Zoho Books
async function fetchBillDetails(billId) {
    try {
        const response = await axios.get(`${API_BASE_URL}/bills/${billId}`, {
            headers: {
                Authorization: `Zoho-oauthtoken ${accessToken}`,
            },
        });
        return response.data;
    } catch (error) {
        if (error.response?.status === 401) {
            console.log('Access token expired. Refreshing token...');
            await refreshAccessToken();
            return fetchBillDetails(billId);
        }

        if (error.response?.status === 429) {
            console.log('Rate limit reached. Stopping script. Resume tomorrow.');
            process.exit(1); // This will stop the script immediately
        }

        console.error('Error fetching bill details:', error.response?.data || error.message);
        throw error;
    }
}

// Function to ensure the bill_detail table exists
async function ensureTableExists(client) {
    const createTableQuery = `
    CREATE TABLE IF NOT EXISTS zoho_books.bill_detail (
        bill_id VARCHAR(255) PRIMARY KEY,
        vendor_id VARCHAR,
        vendor_name VARCHAR,
        vat_treatment VARCHAR,
        vat_reg_no VARCHAR,
        source_of_supply VARCHAR,
        destination_of_supply VARCHAR,
        place_of_supply VARCHAR,
        permit_number VARCHAR,
        gst_no VARCHAR,
        gst_treatment VARCHAR,
        tax_treatment VARCHAR,
        is_pre_gst BOOLEAN,
        status VARCHAR,
        bill_number VARCHAR,
        date VARCHAR,
        due_date VARCHAR,
        payment_terms NUMERIC,
        payment_terms_label VARCHAR,
        payment_expected_date VARCHAR,
        reference_number VARCHAR,
        currency_id VARCHAR,
        currency_code VARCHAR,
        currency_symbol VARCHAR,
        documents JSONB,
        price_precision NUMERIC,
        exchange_rate NUMERIC,
        adjustment NUMERIC,
        adjustment_description VARCHAR,
        is_inclusive_tax BOOLEAN,
        sub_total NUMERIC,
        tax_total NUMERIC,
        total NUMERIC,
        payment_made NUMERIC,
        balance NUMERIC,
        billing_address JSONB,
        purchaseorders JSONB,
        line_items JSONB,
        payments JSONB,
        taxes JSONB,
        created_time TIMESTAMP,
        created_by_id VARCHAR,
        last_modified_time VARCHAR,
        notes TEXT,
        terms TEXT,
        attachment_name VARCHAR
    );`;

    await client.query(createTableQuery);
}

async function getLatestSyncTime(client) {
    const result = await client.query(`
        SELECT MAX(created_time) as last_sync
        FROM zoho_books.bill_detail
    `);
    return result.rows[0].last_sync;
}

// Function to process and sync bills
async function syncBills() {
    const client = await pool.connect();
    try {
        await ensureTableExists(client);

        const lastSyncTime = await getLatestSyncTime(client);
        console.log(`Last sync time: ${lastSyncTime}`);

        // Fetch all bills (modify the query based on your needs)
        const billQuery = `
            SELECT bill_id, created_time 
            FROM zoho_books.bills 
            WHERE created_time > $1
            ORDER BY created_time ASC`;

        const { rows: bills } = await client.query(billQuery, [lastSyncTime]);

        // const progress = await loadProgress();
        // const startIndex = progress.lastProcessedIndex + 1;

        const startIndex = 0;

        console.log(`Found ${bills.length} bills to sync`);
        console.log(`Resuming from index ${startIndex}`);

        for (let i = startIndex; i < bills.length; i++) {
            const bill = bills[i];
            const { bill_id, created_time } = bill;

            try {
                const details = await fetchBillDetails(bill_id);

                const values = [
                    bill_id,
                    details.bill?.vendor_id || null,
                    details.bill?.vendor_name || null,
                    details.bill?.vat_treatment || null,
                    details.bill?.vat_reg_no || null,
                    details.bill?.source_of_supply || null,
                    details.bill?.destination_of_supply || null,
                    details.bill?.place_of_supply || null,
                    details.bill?.permit_number || null,
                    details.bill?.gst_no || null,
                    details.bill?.gst_treatment || null,
                    details.bill?.tax_treatment || null,
                    details.bill?.is_pre_gst || null,
                    details.bill?.status || null,
                    details.bill?.bill_number || null,
                    details.bill?.date || null,
                    details.bill?.due_date || null,
                    details.bill?.payment_terms || null,
                    details.bill?.payment_terms_label || null,
                    details.bill?.payment_expected_date || null,
                    details.bill?.reference_number || null,
                    details.bill?.currency_id || null,
                    details.bill?.currency_code || null,
                    details.bill?.currency_symbol || null,
                    details.bill?.documents ? JSON.stringify(details.bill.documents) : null,
                    details.bill?.price_precision || null,
                    details.bill?.exchange_rate || null,
                    details.bill?.adjustment || null,
                    details.bill?.adjustment_description || null,
                    details.bill?.is_inclusive_tax || null,
                    details.bill?.sub_total || null,
                    details.bill?.tax_total || null,
                    details.bill?.total || null,
                    details.bill?.payment_made || null,
                    details.bill?.balance || null,
                    details.bill?.billing_address ? JSON.stringify(details.bill.billing_address) : null,
                    details.bill?.purchaseorders ? JSON.stringify(details.bill.purchaseorders) : null,
                    details.bill?.line_items ? JSON.stringify(details.bill.line_items) : null,
                    details.bill?.payments ? JSON.stringify(details.bill.payments) : null,
                    details.bill?.taxes ? JSON.stringify(details.bill.taxes) : null,
                    created_time,
                    details.bill?.created_by_id || null,
                    details.bill?.last_modified_time || null,
                    details.bill?.notes || null,
                    details.bill?.terms || null,
                    details.bill?.attachment_name || null
                ];

                await client.query(`
                    INSERT INTO zoho_books.bill_detail (
                        bill_id, vendor_id, vendor_name, vat_treatment, vat_reg_no,
                        source_of_supply, destination_of_supply, place_of_supply,
                        permit_number, gst_no, gst_treatment, tax_treatment,
                        is_pre_gst, status, bill_number, date, due_date,
                        payment_terms, payment_terms_label, payment_expected_date,
                        reference_number, currency_id, currency_code, currency_symbol,
                        documents, price_precision, exchange_rate, adjustment,
                        adjustment_description, is_inclusive_tax, sub_total,
                        tax_total, total, payment_made, balance, billing_address,
                        purchaseorders, line_items, payments, taxes, created_time,
                        created_by_id, last_modified_time, notes, terms, attachment_name
                    ) 
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                            $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28,
                            $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41,
                            $42, $43, $44, $45, $46)
                    ON CONFLICT (bill_id) DO UPDATE SET
                        vendor_id = EXCLUDED.vendor_id,
                        vendor_name = EXCLUDED.vendor_name,
                        vat_treatment = EXCLUDED.vat_treatment,
                        vat_reg_no = EXCLUDED.vat_reg_no,
                        source_of_supply = EXCLUDED.source_of_supply,
                        destination_of_supply = EXCLUDED.destination_of_supply,
                        place_of_supply = EXCLUDED.place_of_supply,
                        permit_number = EXCLUDED.permit_number,
                        gst_no = EXCLUDED.gst_no,
                        gst_treatment = EXCLUDED.gst_treatment,
                        tax_treatment = EXCLUDED.tax_treatment,
                        is_pre_gst = EXCLUDED.is_pre_gst,
                        status = EXCLUDED.status,
                        bill_number = EXCLUDED.bill_number,
                        date = EXCLUDED.date,
                        due_date = EXCLUDED.due_date,
                        payment_terms = EXCLUDED.payment_terms,
                        payment_terms_label = EXCLUDED.payment_terms_label,
                        payment_expected_date = EXCLUDED.payment_expected_date,
                        reference_number = EXCLUDED.reference_number,
                        currency_id = EXCLUDED.currency_id,
                        currency_code = EXCLUDED.currency_code,
                        currency_symbol = EXCLUDED.currency_symbol,
                        documents = EXCLUDED.documents,
                        price_precision = EXCLUDED.price_precision,
                        exchange_rate = EXCLUDED.exchange_rate,
                        adjustment = EXCLUDED.adjustment,
                        adjustment_description = EXCLUDED.adjustment_description,
                        is_inclusive_tax = EXCLUDED.is_inclusive_tax,
                        sub_total = EXCLUDED.sub_total,
                        tax_total = EXCLUDED.tax_total,
                        total = EXCLUDED.total,
                        payment_made = EXCLUDED.payment_made,
                        balance = EXCLUDED.balance,
                        billing_address = EXCLUDED.billing_address,
                        purchaseorders = EXCLUDED.purchaseorders,
                        line_items = EXCLUDED.line_items,
                        payments = EXCLUDED.payments,
                        taxes = EXCLUDED.taxes,
                        created_time = EXCLUDED.created_time,
                        created_by_id = EXCLUDED.created_by_id,
                        last_modified_time = EXCLUDED.last_modified_time,
                        notes = EXCLUDED.notes,
                        terms = EXCLUDED.terms,
                        attachment_name = EXCLUDED.attachment_name`,
                    values
                );

                // await saveProgress(i);

                const percentComplete = ((i + 1) / bills.length * 100).toFixed(2);
                console.log(`Synced bill ${bill_id} (${i + 1}/${bills.length} - ${percentComplete}%)`);

            } catch (error) {
                console.error(`Error syncing bill ${bill_id}:`, error.message);
                continue;
            }

            await new Promise(resolve => setTimeout(resolve, 1000));
        }

        console.log('Bill sync completed successfully!');
        // fs.unlinkSync(PROGRESS_FILE);

    } catch (error) {
        console.error('Error during bill sync:', error);
    } finally {
        client.release();
    }
}

// Main script execution
(async () => {
    try {
        console.log('Refreshing access token...');
        await refreshAccessToken();

        console.log('Starting bill sync...');
        await syncBills();
    } catch (error) {
        console.error('Script failed:', error.message);
    } finally {
        await pool.end();
    }
})();
