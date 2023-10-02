const axios = require('axios');

//1 API URL
const baseDiscountsUrl = 'https://api.rechargeapps.com/discounts?limit=250';

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
  const resp = await axios.get(req.body.state?.nextUrl||baseDiscountsUrl, {
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
        insert: resp.data.discounts,          // Data to be inserted into warehouse
        state: {nextUrl: baseDiscountsUrl},   // Define any variables here. Can be used for e.g. pagination or incremental pointers
        hasMore: false                            // If true Weld will call endpoint again with the updated state to get more rows
      };
      break;
    default:
      result = {
        insert: resp.data.discounts,                                                            // Data to be inserted into warehouse
        state: {nextUrl: baseDiscountsUrl.concat('&cursor=').concat(resp.data.next_cursor)},    // Define any variables here. Can be used for e.g. pagination or incremental pointers
        hasMore: true                                                                               // If true Weld will call endpoint again with the updated state to get more rows
      };
      break;
  }

  res.status(200).send(result);
};

// Schema for discounts table (null, string, int, long, float, double, boolean). If some fields are not defined, Weld will auto-infer types.
const getSchema = () => {
  return {
    schema: {
      next_cursor: { type: ['null', 'string'], default: null },
      previous_cursor: { type: ['null', 'string'], default: null },
      discounts: {
        type: ['null', 'array'],
        default: null,
        items: {
          type: 'object',
          fields: [
            { name: 'id', type: ['null', 'integer'], default: null },
            {
              name: 'applies_to',
              type: 'object',
              fields: [
                { name: 'ids', type: ['null', 'array'], default: null },
                { name: 'purchase_item_type', type: ['null', 'string'], default: null },
                { name: 'resource', type: ['null', 'string'], default: null }
              ]
            },
            {
              name: 'channel_settings',
              type: 'object',
              fields: [
                {
                  name: 'api',
                  type: 'object',
                  fields: [{ name: 'can_apply', type: ['null', 'boolean'], default: null }]
                },
                {
                  name: 'checkout_page',
                  type: 'object',
                  fields: [{ name: 'can_apply', type: ['null', 'boolean'], default: null }]
                },
                {
                  name: 'customer_portal',
                  type: 'object',
                  fields: [{ name: 'can_apply', type: ['null', 'boolean'], default: null }]
                },
                {
                  name: 'merchant_portal',
                  type: 'object',
                  fields: [{ name: 'can_apply', type: ['null', 'boolean'], default: null }]
                }
              ]
            },
            { name: 'code', type: ['null', 'string'], default: null },
            { name: 'created_at', type: ['null', 'string'], default: null },
            { name: 'ends_at', type: ['null', 'string'], default: null },
            {
              name: 'external_discount_id',
              type: 'object',
              fields: [{ name: 'ecommerce', type: ['null', 'string'], default: null }]
            },
            { name: 'external_discount_source', type: ['null', 'string'], default: null },
            { name: 'prerequisite_subtotal_min', type: ['null', 'string'], default: null },
            { name: 'starts_at', type: ['null', 'string'], default: null },
            { name: 'status', type: ['null', 'string'], default: null },
            { name: 'updated_at', type: ['null', 'string'], default: null },
            {
              name: 'usage_limits',
              type: 'object',
              fields: [
                {
                  name: 'first_time_customer_restriction',
                  type: ['null', 'boolean'],
                  default: null
                },
                { name: 'max_subsequent_redemptions', type: ['null', 'integer'], default: null },
                { name: 'one_application_per_customer', type: ['null', 'boolean'], default: null }
              ]
            },
            { name: 'value', type: ['null', 'string'], default: null },
            { name: 'value_type', type: ['null', 'string'], default: null }
          ]
        }
      }
    }
  };
};