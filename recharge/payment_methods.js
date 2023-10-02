const axios = require('axios');

//1 API URL
const basePaymentMethodsUrl = 'https://api.rechargeapps.com/payment_methods?limit=250';

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
  const resp = await axios.get(req.body.state?.nextUrl||basePaymentMethodsUrl, {
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
        insert: resp.data.payment_methods,          // Data to be inserted into warehouse
        state: {nextUrl: basePaymentMethodsUrl},   // Define any variables here. Can be used for e.g. pagination or incremental pointers
        hasMore: false                            // If true Weld will call endpoint again with the updated state to get more rows
      };
      break;
    default:
      result = {
        insert: resp.data.payment_methods,                                                            // Data to be inserted into warehouse
        state: {nextUrl: basePaymentMethodsUrl.concat('&cursor=').concat(resp.data.next_cursor)},    // Define any variables here. Can be used for e.g. pagination or incremental pointers
        hasMore: true                                                                               // If true Weld will call endpoint again with the updated state to get more rows
      };
      break;
  }

  res.status(200).send(result);
};

// Schema for payment_methods table (null, string, int, long, float, double, boolean). If some fields are not defined, Weld will auto-infer types.
const getSchema = () => {
  return {
    schema: {
      payment_methods: {
        type: ['null', 'array'],
        default: null,
        items: {
          type: 'object',
          fields: [
            { name: 'id', type: ['null', 'integer'], default: null },
            {
              name: 'billing_address',
              type: 'object',
              fields: [
                { name: 'address1', type: ['null', 'string'], default: null },
                { name: 'address2', type: ['null', 'string'], default: null },
                { name: 'city', type: ['null', 'string'], default: null },
                { name: 'company_name', type: ['null', 'string'], default: null },
                { name: 'country_code', type: ['null', 'string'], default: null },
                { name: 'name', type: ['null', 'string'], default: null },
                { name: 'phone', type: ['null', 'string'], default: null },
                { name: 'province', type: ['null', 'string'], default: null },
                { name: 'zip', type: ['null', 'string'], default: null }
              ]
            },
            { name: 'created_at', type: ['null', 'string'], default: null },
            { name: 'customer_id', type: ['null', 'integer'], default: null },
            { name: 'default', type: ['null', 'boolean'], default: null },
            {
              name: 'payment_details',
              type: ['null', 'array'],
              default: null,
              items: {
                type: 'object',
                fields: [
                  { name: 'brand', type: ['null', 'string'], default: null },
                  { name: 'exp_month', type: ['null', 'integer'], default: null },
                  { name: 'exp_year', type: ['null', 'integer'], default: null },
                  { name: 'last4', type: ['null', 'integer'], default: null }
                ]
              }
            },
            { name: 'payment_type', type: ['null', 'string'], default: null },
            { name: 'processor_customer_token', type: ['null', 'string'], default: null },
            { name: 'processor_name', type: ['null', 'string'], default: null },
            { name: 'processor_payment_method_token', type: ['null', 'string'], default: null },
            { name: 'updated_at', type: ['null', 'string'], default: null }
          ]
        }
      }
    }
  };
};