const axios = require('axios');

//1 API URL
const baseChargesUrl = 'https://api.rechargeapps.com/charges?limit=250';

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
  const resp = await axios.get(req.body.state?.nextUrl||baseChargesUrl, {
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
        insert: resp.data.charges,          // Data to be inserted into warehouse
        state: {nextUrl: baseChargesUrl},   // Define any variables here. Can be used for e.g. pagination or incremental pointers
        hasMore: false                            // If true Weld will call endpoint again with the updated state to get more rows
      };
      break;
    default:
      result = {
        insert: resp.data.charges,                                                            // Data to be inserted into warehouse
        state: {nextUrl: baseChargesUrl.concat('&cursor=').concat(resp.data.next_cursor)},    // Define any variables here. Can be used for e.g. pagination or incremental pointers
        hasMore: true                                                                               // If true Weld will call endpoint again with the updated state to get more rows
      };
      break;
  }

  res.status(200).send(result);
};

// Schema for charges table (null, string, int, long, float, double, boolean). If some fields are not defined, Weld will auto-infer types.
const getSchema = () => {
  return {
    schema: {
      charges: {
        primary_key: 'id',
        fields: [
          { name: 'id', type: ['null', 'int'], default: null },
          { name: 'address_id', type: ['null', 'int'], default: null },
          { name: 'analytics_data', type: ['null', {
            type: 'record',
            name: 'analytics_data',
            fields: [
              { name: 'utm_params', type: ['null', {
                type: 'array',
                items: {
                  type: 'record',
                  name: 'utm_params',
                  fields: [
                    { name: 'utm_campaign', type: ['null', 'string'], default: null },
                    { name: 'utm_content', type: ['null', 'string'], default: null },
                    { name: 'utm_data_source', type: ['null', 'string'], default: null },
                    { name: 'utm_medium', type: ['null', 'string'], default: null },
                    { name: 'utm_source', type: ['null', 'string'], default: null },
                    { name: 'utm_term', type: ['null', 'string'], default: null },
                    { name: 'utm_time_stamp', type: ['null', 'string'], default: null },
                  ]
                }
              }], default: null },
            ]
          }], default: null },
          { name: 'charge', type: ['null', {
            type: 'record',
            name: 'charge',
            fields: [
              { name: 'id', type: ['null', 'int'], default: null },
              { name: 'address_id', type: ['null', 'int'], default: null },
              { name: 'analytics_data', type: ['null', {
                type: 'record',
                name: 'analytics_data',
                fields: [
                  { name: 'utm_params', type: ['null', {
                    type: 'array',
                    items: {
                      type: 'record',
                      name: 'utm_params',
                      fields: [
                        { name: 'utm_campaign', type: ['null', 'string'], default: null },
                        { name: 'utm_content', type: ['null', 'string'], default: null },
                        { name: 'utm_data_source', type: ['null', 'string'], default: null },
                        { name: 'utm_medium', type: ['null', 'string'], default: null },
                        { name: 'utm_source', type: ['null', 'string'], default: null },
                        { name: 'utm_term', type: ['null', 'string'], default: null },
                        { name: 'utm_time_stamp', type: ['null', 'string'], default: null },
                      ]
                    }
                  }], default: null },
                ]
              }], default: null },
            ]
          }], default: null },
          { name: 'charge_interval_frequency', type: ['null', 'int'], default: null },
          { name: 'customer_id', type: ['null', 'int'], default: null },
          { name: 'has_queued_charges', type: ['null', 'boolean'], default: null },
          { name: 'is_prepaid', type: ['null', 'boolean'], default: null },
          { name: 'is_skippable', type: ['null', 'boolean'], default: null },
          { name: 'is_swappable', type: ['null', 'boolean'], default: null },
          { name: 'max_retries_reached', type: ['null', 'boolean'], default: null },
          { name: 'order_interval_frequency', type: ['null', 'int'], default: null },
          { name: 'quantity', type: ['null', 'int'], default: null },
          { name: 'sku_override', type: ['null', 'boolean'], default: null },
        ]
      },
    },
  };
};