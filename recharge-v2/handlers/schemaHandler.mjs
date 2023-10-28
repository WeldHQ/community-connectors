// Schema for Recharge tables
export const getSchema = () => {
    return {
      schema: {
        charges: {
          primary_key: 'id',
          fields: [ ]
        },
        collections: {
          primary_key: 'id',
          fields: [ ]
        },
        customers: {
          primary_key: 'id',
          fields: [ ]
        },
        discounts: {
          primary_key: 'id',
          fields: [ ]
        },
        onetimes: {
          primary_key: 'id',
          fields: [ ]
        },
        orders: {
          primary_key: 'id',
          fields: [ ]
        },
        payment_methods: {
          primary_key: 'id',
          fields: [ ]
        },
        plans: {
          primary_key: 'id',
          fields: [ ]
        },
        retention_strategies: {
          primary_key: 'id',
          fields: [ ]
        },
        subscriptions: {
          primary_key: 'id',
          fields: [ ]
        },
      },
    };
  };
  