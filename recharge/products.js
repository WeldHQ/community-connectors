const axios = require('axios');

//1 API URL
const baseProductsUrl = 'https://api.rechargeapps.com/products?limit=250';

//2 API Authentication

//2.1 Recharge API (set key in cloud function env variable)
const api_key = process.env.RECHARGE_API_KEY;

//2.2 Weld API (set key in cloud function env variable)
const checkAuth = (req) => {
  const token = req.headers.authorization.split(" ")[1];
  if (token !== process.env.WELD_API_KEY) {
    const err = new Error("Not authorized");
    err.status = 403;
    throw err;
  }
};

//3: HTTP call
exports.handler = async (req, res) => {
  let result;

  //3.1 Make sure the request is coming from Weld
  checkAuth(req);

  //3.2 If path is schema, return the schema to Weld
  if (req.originalUrl === "/schema") return res.json(getSchema());

  //3.3 Set potential URL parameters, e.g. an incremental pointer
  // Not needed in this case

  //3.4 Call the API
  const resp = await axios.get(req.body.state?.nextUrl||baseProductsUrl, {
    headers: {
      'X-Recharge-Access-Token': api_key,
      'X-Recharge-Version': '2021-11',
      'Content-Type': 'application/json'
    }
    });

  //3.5 Format data to Weld format
  // Not needed in this case

  //3.6 Handle pagination - find next URL, different how APIs handles this
  switch (resp.data.next_cursor) {
    case null:
      result = {
        insert: resp.data.products,          // Data to be inserted into warehouse
        state: {nextUrl: baseProductsUrl},   // Define any variables here. Can be used for e.g. pagination or incremental pointers
        hasMore: false                            // If true Weld will call endpoint again with the updated state to get more rows
      };
      break;
    default:
      result = {
        insert: resp.data.products,                                                            // Data to be inserted into warehouse
        state: {nextUrl: baseProductsUrl.concat('&cursor=').concat(resp.data.next_cursor)},    // Define any variables here. Can be used for e.g. pagination or incremental pointers
        hasMore: true                                                                               // If true Weld will call endpoint again with the updated state to get more rows
      };
      break;
  }

  res.status(200).send(result);
};

// Schema for products table (null, string, int, long, float, double, boolean). If some fields are not defined, Weld will auto-infer types.
const getSchema = () => {
  return {
    schema: {
      next_cursor: { type: ['null', 'string'], default: null },
      previous_cursor: { type: ['null', 'string'], default: null },
      products: {
        type: ['null', 'array'],
        default: null,
        items: {
          type: 'object',
          fields: [
            { name: 'brand', type: ['null', 'string'], default: null },
            { name: 'description', type: ['null', 'string'], default: null },
            { name: 'external_created_at', type: ['null', 'string'], default: null },
            { name: 'external_product_id', type: ['null', 'string'], default: null },
            { name: 'external_updated_at', type: ['null', 'string'], default: null },
            {
              name: 'images',
              type: 'array',
              default: null,
              items: {
                type: 'object',
                fields: [
                  { name: 'large', type: ['null', 'string'], default: null },
                  { name: 'medium', type: ['null', 'string'], default: null },
                  { name: 'original', type: ['null', 'string'], default: null },
                  { name: 'small', type: ['null', 'string'], default: null },
                  { name: 'sort_order', type: ['null', 'integer'], default: null }
                ]
              }
            },
            {
              name: 'options',
              type: 'array',
              default: null,
              items: {
                type: 'object',
                fields: [
                  { name: 'name', type: ['null', 'string'], default: null },
                  { name: 'position', type: ['null', 'integer'], default: null },
                  {
                    name: 'values',
                    type: 'array',
                    default: null,
                    items: {
                      type: 'object',
                      fields: [
                        { name: 'label', type: ['null', 'string'], default: null },
                        { name: 'position', type: ['null', 'integer'], default: null }
                      ]
                    }
                  }
                ]
              }
            },
            { name: 'published_at', type: ['null', 'string'], default: null },
            { name: 'requires_shipping', type: ['null', 'boolean'], default: null },
            { name: 'title', type: ['null', 'string'], default: null },
            {
              name: 'variants',
              type: 'array',
              default: null,
              items: {
                type: 'object',
                fields: [
                  {
                    name: 'dimensions',
                    type: 'object',
                    fields: [
                      { name: 'weight', type: ['null', 'number'], default: null },
                      { name: 'weight_unit', type: ['null', 'string'], default: null }
                    ]
                  },
                  { name: 'external_variant_id', type: ['null', 'string'], default: null },
                  {
                    name: 'image',
                    type: 'object',
                    fields: [
                      { name: 'large', type: ['null', 'string'], default: null },
                      { name: 'medium', type: ['null', 'string'], default: null },
                      { name: 'original', type: ['null', 'string'], default: null },
                      { name: 'small', type: ['null', 'string'], default: null }
                    ]
                  },
                  {
                    name: 'option_values',
                    type: 'array',
                    default: null,
                    items: {
                      type: 'object',
                      fields: [{ name: 'label', type: ['null', 'string'], default: null }]
                    }
                  },
                  {
                    name: 'prices',
                    type: 'object',
                    fields: [
                      { name: 'compare_at_price', type: ['null', 'string'], default: null },
                      { name: 'unit_price', type: ['null', 'string'], default: null }
                    ]
                  },
                  { name: 'sku', type: ['null', 'string'], default: null },
                  { name: 'tax_code', type: ['null', 'string'], default: null },
                  { name: 'taxable', type: ['null', 'boolean'], default: null },
                  { name: 'title', type: ['null', 'string'], default: null },
                  { name: 'requires_shipping', type: ['null', 'boolean'], default: null }
                ]
              }
            },
            { name: 'vendor', type: ['null', 'string'], default: null }
          ]
        }
      }
    }
  };
};