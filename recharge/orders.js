const axios = require('axios');

//1 API URL
const baseOrdersUrl = 'https://api.rechargeapps.com/orders?limit=250';

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
  const resp = await axios.get(req.body.state?.nextUrl||baseOrdersUrl, {
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
        insert: resp.data.orders,          // Data to be inserted into warehouse
        state: {nextUrl: baseOrdersUrl},   // Define any variables here. Can be used for e.g. pagination or incremental pointers
        hasMore: false                            // If true Weld will call endpoint again with the updated state to get more rows
      };
      break;
    default:
      result = {
        insert: resp.data.orders,                                                            // Data to be inserted into warehouse
        state: {nextUrl: baseOrdersUrl.concat('&cursor=').concat(resp.data.next_cursor)},    // Define any variables here. Can be used for e.g. pagination or incremental pointers
        hasMore: true                                                                               // If true Weld will call endpoint again with the updated state to get more rows
      };
      break;
  }

  res.status(200).send(result);
};

// Schema for orders table (null, string, int, long, float, double, boolean). If some fields are not defined, Weld will auto-infer types.
const getSchema = () => {
  return {
    schema: {
      next: { type: ['null', 'string'], default: null },
      previous: { type: ['null', 'string'], default: null },
      orders: {
        type: ['null', 'array'],
        default: null,
        items: {
          type: 'object',
          fields: [
            { name: 'id', type: ['null', 'integer'], default: null },
            { name: 'address_id', type: ['null', 'integer'], default: null },
            {
              name: 'billing_address',
              type: 'object',
              fields: [
                { name: 'address1', type: ['null', 'string'], default: null },
                { name: 'address2', type: ['null', 'string'], default: null },
                { name: 'city', type: ['null', 'string'], default: null },
                { name: 'company', type: ['null', 'string'], default: null },
                { name: 'country_code', type: ['null', 'string'], default: null },
                { name: 'first_name', type: ['null', 'string'], default: null },
                { name: 'last_name', type: ['null', 'string'], default: null },
                { name: 'phone', type: ['null', 'string'], default: null },
                { name: 'province', type: ['null', 'string'], default: null },
                { name: 'zip', type: ['null', 'string'], default: null }
              ]
            },
            {
              name: 'charge',
              type: 'object',
              fields: [
                { name: 'id', type: ['null', 'integer'], default: null },
                {
                  name: 'external_transaction_id',
                  type: 'object',
                  fields: [{ name: 'payment_processor', type: ['null', 'string'], default: null }]
                }
              ]
            },
            {
              name: 'client_details',
              type: 'object',
              fields: [
                { name: 'browser_ip', type: ['null', 'string'], default: null },
                { name: 'user_agent', type: ['null', 'string'], default: null }
              ]
            },
            { name: 'created_at', type: ['null', 'string'], default: null },
            { name: 'currency', type: ['null', 'string'], default: null },
            {
              name: 'customer',
              type: 'object',
              fields: [
                { name: 'id', type: ['null', 'integer'], default: null },
                { name: 'email', type: ['null', 'string'], default: null },
                {
                  name: 'external_customer_id',
                  type: 'object',
                  fields: [{ name: 'ecommerce', type: ['null', 'string'], default: null }]
                },
                { name: 'hash', type: ['null', 'string'], default: null }
              ]
            },
            {
              name: 'discounts',
              type: ['null', 'array'],
              default: null,
              items: {
                type: 'object',
                fields: [
                  { name: 'id', type: ['null', 'integer'], default: null },
                  { name: 'code', type: ['null', 'string'], default: null },
                  { name: 'value', type: ['null', 'integer'], default: null },
                  { name: 'value_type', type: ['null', 'string'], default: null }
                ]
              }
            },
            { name: 'error', type: ['null', 'string'], default: null },
            { name: 'external_cart_token', type: ['null', 'string'], default: null },
            {
              name: 'external_order_id',
              type: 'object',
              fields: [{ name: 'ecommerce', type: ['null', 'string'], default: null }]
            },
            {
              name: 'external_order_number',
              type: 'object',
              fields: [{ name: 'ecommerce', type: ['null', 'string'], default: null }]
            },
            { name: 'is_prepaid', type: ['null', 'boolean'], default: null },
            {
              name: 'line_items',
              type: ['null', 'array'],
              default: null,
              items: {
                type: 'object',
                fields: [
                  { name: 'purchase_item_id', type: ['null', 'integer'], default: null },
                  {
                    name: 'external_inventory_policy',
                    type: ['null', 'string'],
                    default: null
                  },
                  {
                    name: 'external_product_id',
                    type: 'object',
                    fields: [{ name: 'ecommerce', type: ['null', 'string'], default: null }]
                  },
                  {
                    name: 'external_variant_id',
                    type: 'object',
                    fields: [{ name: 'ecommerce', type: ['null', 'string'], default: null }]
                  },
                  { name: 'grams', type: ['null', 'integer'], default: null },
                  {
                    name: 'images',
                    type: 'object',
                    fields: [
                      { name: 'large', type: ['null', 'string'], default: null },
                      { name: 'medium', type: ['null', 'string'], default: null },
                      { name: 'original', type: ['null', 'string'], default: null },
                      { name: 'small', type: ['null', 'string'], default: null }
                    ]
                  },
                  { name: 'original_price', type: ['null', 'string'], default: null },
                  {
                    name: 'properties',
                    type: ['null', 'array'],
                    default: null,
                    items: {
                      type: 'object',
                      fields: [
                        { name: 'name', type: ['null', 'string'], default: null },
                        { name: 'value', type: ['null', 'string'], default: null }
                      ]
                    }
                  },
                  {
                    name: 'purchase_item_type',
                    type: ['null', 'string'],
                    default: null
                  },
                  { name: 'quantity', type: ['null', 'integer'], default: null },
                  { name: 'sku', type: ['null', 'string'], default: null },
                  {
                    name: 'tax_lines',
                    type: ['null', 'array'],
                    default: null,
                    items: {
                      type: 'object',
                      fields: [
                        { name: 'price', type: ['null', 'string'], default: null },
                        { name: 'rate', type: ['null', 'string'], default: null },
                        { name: 'title', type: ['null', 'string'], default: null },
                        { name: 'unit_price', type: ['null', 'string'], default: null }
                      ]
                    }
                  },
                  { name: 'taxable', type: ['null', 'boolean'], default: null },
                  { name: 'taxable_amount', type: ['null', 'string'], default: null },
                  { name: 'tax_due', type: ['null', 'string'], default: null },
                  { name: 'title', type: ['null', 'string'], default: null },
                  { name: 'total_price', type: ['null', 'string'], default: null },
                  { name: 'unit_price', type: ['null', 'string'], default: null },
                  {
                    name: 'unit_price_includes_tax',
                    type: ['null', 'boolean'],
                    default: null
                  },
                  { name: 'variant_title', type: ['null', 'string'], default: null }
                ]
              }
            },
            { name: 'note', type: ['null', 'string'], default: null },
            {
              name: 'order_attributes',
              type: ['null', 'array'],
              default: null,
              items: {
                type: 'object',
                fields: [
                  { name: 'name', type: ['null', 'string'], default: null },
                  { name: 'value', type: ['null', 'string'], default: null }
                ]
              }
            },
            { name: 'processed_at', type: ['null', 'string'], default: null },
            { name: 'scheduled_at', type: ['null', 'string'], default: null },
            {
              name: 'shipping_address',
              type: 'object',
              fields: [
                { name: 'address1', type: ['null', 'string'], default: null },
                { name: 'address2', type: ['null', 'string'], default: null },
                { name: 'city', type: ['null', 'string'], default: null },
                { name: 'company', type: ['null', 'string'], default: null },
                { name: 'country_code', type: ['null', 'string'], default: null },
                { name: 'first_name', type: ['null', 'string'], default: null },
                { name: 'last_name', type: ['null', 'string'], default: null },
                { name: 'phone', type: ['null', 'string'], default: null },
                { name: 'province', type: ['null', 'string'], default: null },
                { name: 'zip', type: ['null', 'string'], default: null }
              ]
            },
            {
              name: 'shipping_lines',
              type: ['null', 'array'],
              default: null,
              items: {
                type: 'object',
                fields: [
                  { name: 'code', type: ['null', 'string'], default: null },
                  { name: 'price', type: ['null', 'string'], default: null },
                  { name: 'taxable', type: ['null', 'boolean'], default: null },
                  {
                    name: 'tax_lines',
                    type: ['null', 'array'],
                    default: null,
                    items: {
                      type: 'object',
                      fields: [
                        { name: 'price', type: ['null', 'string'], default: null },
                        { name: 'rate', type: ['null', 'string'], default: null },
                        { name: 'title', type: ['null', 'string'], default: null }
                      ]
                    }
                  },
                  { name: 'title', type: ['null', 'string'], default: null }
                ]
              }
            },
            { name: 'status', type: ['null', 'string'], default: null },
            { name: 'subtotal_price', type: ['null', 'string'], default: null },
            { name: 'tags', type: ['null', 'string'], default: null },
            {
              name: 'tax_lines',
              type: ['null', 'array'],
              default: null,
              items: {
                type: 'object',
                fields: [
                  { name: 'price', type: ['null', 'string'], default: null },
                  { name: 'rate', type: ['null', 'string'], default: null },
                  { name: 'title', type: ['null', 'string'], default: null }
                ]
              }
            },
            { name: 'taxable', type: ['null', 'boolean'], default: null },
            { name: 'total_discounts', type: ['null', 'string'], default: null },
            { name: 'total_duties', type: ['null', 'string'], default: null },
            { name: 'total_line_items_price', type: ['null', 'string'], default: null },
            { name: 'total_price', type: ['null', 'string'], default: null },
            { name: 'total_refunds', type: ['null', 'string'], default: null },
            { name: 'total_tax', type: ['null', 'string'], default: null },
            { name: 'total_weight_grams', type: ['null', 'integer'], default: null },
            { name: 'type', type: ['null', 'string'], default: null },
            { name: 'updated_at', type: ['null', 'string'], default: null }
          ]
        }
      }
    }
  };
};