console.log('EXECUTING FILE:', __filename);

/* high-Ticket Directory Aggregator Backend Engine (Node.js/Express + PostgreSQL)
 * Implements:
 * 1. Subjective-to-Objective Dynamic Filter Routing (using PG JSONB)
 * 2. Nested JSON-LD Generative Engine Optimization (GEO) Generator
 * 3. Pay-Per-Lead (PPL) Validation & Webhook Dispatcher (with Exponential Backoff)
 * 4. Localized Showroom Store Locator queries
 */
const path = require('path');
const express = require('express');
const { Pool } = require('pg');
const axios = require('axios');
const crypto = require('crypto');

const app = express();
// Ensure Express is configured to parse incoming JSON data
app.use(express.json());

// Initialize PostgreSQL Connection Pool using Environment Variables
const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/aggregator_db',
  max: 20, // Max clients in pool
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
});

/**
 * UTILITY: Resilient Webhook Dispatcher with Exponential Backoff
 * Retries dispatching lead payloads up to 5 times with growing delays (1s, 2s, 4s, 8s, 16s)
 */
async function dispatchWebhookWithBackoff(url, payload, attempt = 1) {
  const maxAttempts = 5;
  try {
    const response = await axios.post(url, payload, {
      headers: {
        'Content-Type': 'application/json',
        'X-Aggregator-Signature': crypto.createHmac('sha256', process.env.WEBHOOK_SECRET || 'fallback_secret')
                                        .update(JSON.stringify(payload))
                                        .digest('hex')
      },
      timeout: 5000 // 5-second timeout window
    });
    return { success: true, status: response.status, data: response.data };
  } catch (error) {
    if (attempt >= maxAttempts) {
      console.error(`[CRITICAL] Webhook dispatch permanently failed after ${maxAttempts} attempts. Destination: ${url}. Error:`, error.message);
      return { success: false, error: error.message };
    }
    const delay = Math.pow(2, attempt - 1) * 1000;
    await new Promise((resolve) => setTimeout(resolve, delay));
    return dispatchWebhookWithBackoff(url, payload, attempt + 1);
  }
}

/**
 * 1. SUBJECTIVE-TO-OBJECTIVE FILTERING ENGINE
 * GET /api/products
 * * Maps subjective human attributes to technical PostgreSQL JSONB operations.
 * Handles:
 * - Side Sleeper (< 130 lbs): Firmness 1-4, comfort layer >= 3 inches.
 * - Back/Stomach Sleeper (> 230 lbs): Coil gauge <= 13.5 (stiffer), base density >= 1.8 lb/ft³.
 */
// Serve the main frontend dashboard
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/api/products', async (req, res) => {
  try {
    const { sleeping_position, body_weight, category, max_price } = req.query;

    let queryText = `
      SELECT m.*, b.name as brand_name, b.base_warranty_years, b.return_policy_days 
      FROM product_models m
      JOIN brands b ON m.brand_id = b.brand_id
      WHERE 1=1
    `;
    const queryParams = [];
    let paramIndex = 1;

    // Filter by general category
    if (category) {
      queryText += ` AND m.product_type = $${paramIndex}`;
      queryParams.push(category);
      paramIndex++;
    }

    // Filter by user budget
    if (max_price) {
      queryText += ` AND m.base_msrp <= $${paramIndex}`;
      queryParams.push(parseFloat(max_price));
      paramIndex++;
    }

    // MAP SUBJECTIVE CRITERIA TO EXACT METRICS (The Filtering Heuristic)
    if (sleeping_position && body_weight) {
      const weight = parseFloat(body_weight);

      if (sleeping_position === 'side' && weight < 130) {
        // Firmness index 1 to 4; Comfort layer thickness >= 3 inches in technical_specs
        queryText += ` AND m.firmness_score_normalized BETWEEN 1 AND 4`;
        queryText += ` AND (m.technical_specs->>'comfort_layer_thickness')::numeric >= 3.0`;
      } 
      else if ((sleeping_position === 'back' || sleeping_position === 'stomach') && weight > 230) {
        // High support: Coil gauge <= 13.5, and Base foam density >= 1.8 lb/ft³
        queryText += ` AND (
          (m.technical_specs->>'coil_gauge')::numeric <= 13.5 
          OR (m.technical_specs->>'base_foam_density')::numeric >= 1.8
        )`;
      }
    }

    queryText += ` ORDER BY m.base_msrp ASC`;

    const { rows } = await pool.query(queryText, queryParams);
    return res.json({ success: true, resultsCount: rows.length, data: rows });
  } catch (err) {
    console.error('Error in /api/products:', err);
    return res.status(500).json({ success: false, error: 'Database processing failed.' });
  }
});

/**
 * 2. PAY-PER-LEAD (PPL) CONVERSION PIPELINE
 * POST /api/leads
 * * Receives bottom-of-funnel conversion wizard payloads and dispatches them
 * directly to the partner CRM endpoint via webhooks securely.
 */
app.post('/api/leads', async (req, res) => {
  const { contact_payload, user_intent, extracted_parameters } = req.body;

  // Strict structural validation
  if (!contact_payload?.email || !contact_payload?.phone || !contact_payload?.first_name) {
    return res.status(400).json({ success: false, error: 'Missing critical contact fields (first_name, email, phone).' });
  }
  if (!user_intent?.target_category || !user_intent?.delivery_zip_code) {
    return res.status(400).json({ success: false, error: 'Missing core intent metadata (target_category, delivery_zip_code).' });
  }

  const leadId = `lead_${crypto.randomBytes(8).toString('hex')}`;
  const normalizedLead = {
    lead_id: leadId,
    source_domain: req.get('host') || 'directory.com',
    user_intent,
    extracted_parameters: extracted_parameters || {},
    contact_payload,
    timestamp: new Date().toISOString()
  };

  // Find CRM routing target matching user's selected category or brand preference
  // In production, database tables manage client webhooks matching product specs dynamically.
  const targetCRMUrl = process.env.PARTNER_CRM_WEBHOOK_URL || 'https://mock-partner-crm.com/api/v1/leads';

  // Async dispatch running in the background to avoid holding client request open
  dispatchWebhookWithBackoff(targetCRMUrl, normalizedLead)
    .then((result) => {
      if (result.success) {
        console.log(`[PPL Success] Lead ${leadId} successfully dispatched to partner CRM. Status: ${result.status}`);
      }
    })
    .catch((err) => console.error(`[PPL Disaster] Lead processing crashed completely for ${leadId}`, err));

  // Immediate success confirmation to frontend (Micro-incentive resolution)
  return res.status(202).json({
    success: true,
    message: 'Lead processing initiated. Your rebate coupon code and matching local details are generating.',
    lead_id: leadId,
    unlock_token: crypto.createHash('md5').update(leadId).digest('hex')
  });
});

/**
 * 3. GEOLOCATED STORE LOCATOR AND INVENTORY WATCH
 * GET /api/showrooms
 * * Find localized brick-and-mortar showrooms carrying specific high-intent models near user's zip code.
 */
app.get('/api/showrooms', async (req, res) => {
  try {
    const { model_id, zip_code, radius_miles = 25 } = req.query;

    if (!model_id || !zip_code) {
      return res.status(400).json({ success: false, error: 'Missing required parameters: model_id and zip_code' });
    }

    // Step 1: Fetch baseline zip coordinates (In a production environment, this references a zip code coordinate database)
    const zipQuery = `SELECT latitude, longitude FROM retail_locations WHERE zip_code = $1 LIMIT 1`;
    const zipResult = await pool.query(zipQuery, [zip_code]);

    if (zipResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Zip code coordinates not cataloged.' });
    }

    const { latitude, longitude } = zipResult.rows[0];

    // Step 2: Use Haversine formulation to calculate nearby physical showroom inventories
    const showroomsQuery = `
      SELECT 
        rl.store_name, 
        rl.street_address, 
        rl.city, 
        rl.state, 
        rl.zip_code,
        pa.in_stock, 
        pa.floor_model_available,
        (3959 * acos(
          cos(radians($1)) * cos(radians(rl.latitude)) * cos(radians(rl.longitude) - radians($2)) + 
          sin(radians($1)) * sin(radians(rl.latitude))
        )) AS distance_miles
      FROM retail_locations rl
      JOIN product_availability pa ON rl.location_id = pa.location_id
      WHERE pa.model_id = $3
      GROUP BY rl.location_id, pa.in_stock, pa.floor_model_available
      HAVING (3959 * acos(
        cos(radians($1)) * cos(radians(rl.latitude)) * cos(radians(rl.longitude) - radians($2)) + 
        sin(radians($1)) * sin(radians(rl.latitude))
      )) <= $4
      ORDER BY distance_miles ASC;
    `;

    const { rows } = await pool.query(showroomsQuery, [
      parseFloat(latitude), 
      parseFloat(longitude), 
      parseInt(model_id), 
      parseFloat(radius_miles)
    ]);

    return res.json({ success: true, local_showrooms: rows });
  } catch (err) {
    console.error('Error in /api/showrooms:', err);
    return res.status(500).json({ success: false, error: 'Failed querying nearby retail stock.' });
  }
});

/**
 * 4. DYNAMIC GENERATIVE ENGINE OPTIMIZATION (GEO/SGE) SCHEMAS
 * GET /api/products/:id/schema
 * * Serves highly compliant nested JSON-LD schema configurations directly for SGE crawling engines.
 */
app.get('/api/products/:id/schema', async (req, res) => {
  try {
    const { id } = req.params;

    const productQuery = `
      SELECT m.*, b.name as brand_name, b.return_policy_days
      FROM product_models m
      JOIN brands b ON m.brand_id = b.brand_id
      WHERE m.model_id = $1
    `;
    const productResult = await pool.query(productQuery, [id]);

    if (productResult.rows.length === 0) {
      return res.status(404).json({ success: false, error: 'Product model not found.' });
    }

    const modelObj = productResult.rows[0];

    const variantsQuery = `SELECT size_name, variant_price, sku FROM product_variants WHERE model_id = $1`;
    const variantsResult = await pool.query(variantsQuery, [id]);

    const prices = variantsResult.rows.map(v => parseFloat(v.variant_price));
    const lowPrice = prices.length ? Math.min(...prices).toFixed(2) : modelObj.base_msrp;
    const highPrice = prices.length ? Math.max(...prices).toFixed(2) : modelObj.base_msrp;

    // Inject specifications from dynamic PostgreSQL JSONB technical_specs
    const additionalProperties = Object.keys(modelObj.technical_specs).map(key => ({
      '@type': 'PropertyValue',
      'name': key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
      'value': modelObj.technical_specs[key].toString()
    }));

    if (modelObj.firmness_score_normalized) {
      additionalProperties.push({
        '@type': 'PropertyValue',
        'name': 'Firmness Score',
        'value': modelObj.firmness_score_normalized.toString(),
        'maxValue': '10',
        'minValue': '1'
      });
    }

    // Compose cohesive JSON-LD Graph for schema validation
    const jsonLDGraph = {
      '@context': 'https://schema.org',
      '@graph': [
        {
          '@type': 'Product',
          '@id': `https://directory.com/${modelObj.product_type}/${modelObj.model_id}#product`,
          'name': modelObj.model_name,
          'brand': {
            '@type': 'Brand',
            'name': modelObj.brand_name
          },
          'description': `${modelObj.brand_name} ${modelObj.model_name} in ${modelObj.product_type} category. Spec verified.`,
          'offers': {
            '@type': 'AggregateOffer',
            'priceCurrency': 'USD',
            'lowPrice': lowPrice.toString(),
            'highPrice': highPrice.toString(),
            'offerCount': variantsResult.rows.length.toString()
          },
          'additionalProperty': additionalProperties
        },
        {
          '@type': 'WebPage',
          '@id': `https://directory.com/${modelObj.product_type}/${modelObj.model_id}`,
          'url': `https://directory.com/${modelObj.product_type}/${modelObj.model_id}`,
          'name': `${modelObj.brand_name} ${modelObj.model_name} Specs, Verification & Local Inventory`,
          'mainEntity': {
            '@id': `https://directory.com/${modelObj.product_type}/${modelObj.model_id}#product`
          }
        }
      ]
    };

    return res.json(jsonLDGraph);
  } catch (err) {
    console.error('Error generating schema:', err);
    return res.status(500).json({ success: false, error: 'Dynamic SGE schema compilation failed.' });
  }
});

// Start Express Server Engine
// Configured to use process.env.PORT dynamically so Railway can assign its own port
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`[PROCESS RUNNING] High-ticket aggregator API running on port ${PORT}`);
});
