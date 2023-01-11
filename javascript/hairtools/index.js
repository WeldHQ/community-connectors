const axios = require('axios');
const dayjs = require('dayjs')


const API_KEY = process.env.API_KEY



exports.handler = async (req, res) => {

    console.log('starting job')

    if (req.originalUrl === "/schema") {return res.json(getSchema()) }

    const tableName = req.body.name;

    const state = req.body.state

    console.log(`table name: ${tableName}`)

    console.log(`state: ${state?.date}`)

    try {

    const response = await tableFactory(tableName, state);

    console.log('success')

    res.status(200).send(response);



    } catch (e) {
        console.log('error')
        console.log(e)

    }

}

async function tableFactory(tableName, state) {
    switch (tableName) {
        case 'suppliers':
            return await suppliers();
        case 'product_categories':
             return await product_categories();
        case 'bookings':
            return await chain_data('bookings', state);
        case 'customer_groups':
            return await chain_data('customerGroups', state);
        case 'customer_groups_to_customers':
            return await chain_data('customerGroupsToCustomers', state);
        case 'customers':
            return await chain_data('customers', state);
        case 'departments':
            return await chain_data('departments', state);
        case 'employees':
            return await chain_data('employees', state);
        case 'order_books':
            return await chain_data('orderBooks', state);
        case 'products':
            return await chain_data('products', state);
        case 'sales':
            return await chain_data('sales', state);
        case 'types':
            return await chain_data('types', state);
        case 'work_plans':
            return await chain_data('workPlans', state);
    }
}

async function product_categories() {

    const response = await axios.get('https://customerrelationshipapi.hairtools.dk/SharedData/ProductCategories', {headers: {Authorization: `Bearer ${API_KEY}`}})

    const data = response.data

    return {
        insert: data,
        state: {},
        hasMore: false
    }
}

async function suppliers() {
    const response = await  axios.get('https://customerrelationshipapi.hairtools.dk/SharedData/Suppliers', {headers: {Authorization: `Bearer ${API_KEY}`}})

    const data = response.data

    return {
        insert: data,
        state: {},
        hasMore: false
    }
}

async function chain_data(type, state) {

    let more = true;

    const now = dayjs();

    let startDate = state?.date ? dayjs(state.date) : dayjs('01-01-2021');
    let endDate = startDate.add(3, 'month').endOf('month')


    if(endDate.isAfter(now)) {
        endDate = now;
        more = false
        console.log('no more')
    }

    console.log(`start date: ${startDate.format('DD-MM-YYYY')}`)
    console.log(`end date: ${endDate.format('DD-MM-YYYY')}`)

    console.log(`new state start date: ${!more ? '01-01-2021' : endDate.add(1, 'month').startOf('month').format('MM-DD-YYYY')}`)

    console.log('----------------------------------')

    try {

    const response = await axios.get(`https://customerrelationshipapi.hairtools.dk/ChainsData?dateFrom=${startDate.format('DD-MM-YYYY')}&dateTo=${endDate.format('DD-MM-YYYY')}`, {headers: {Authorization: `Bearer ${API_KEY}`}})

    console.log(response.status)

    const data = response.data

    console.log(data.data[type].id)

    return {
        insert: data.data[type],
        state: {
            date: !more ? '01-01-2021' : endDate.add(1, 'month').startOf('month').format('MM-DD-YYYY')
        },
        hasMore: more
    }

} catch(e) {
    console.log('error while trying to fetch data')
    console.log(e)
    throw new Error(e)
}
}


const getSchema = () => {
    return {
        schema: {
            suppliers: {
                primary_key: 'id'
            },
            product_categories: {
                primary_key: 'id'
            },
            bookings: {
                primary_key: 'id'
            },
            customer_groups: {
                primary_key: 'id'
            },
            customer_groups_to_customers: {
                primary_key: 'id'
            },
            customers: {
                primary_key: 'id'
            },
            departments: {
                primary_key: 'id'
            },
            employees: {
                primary_key: 'id'
            },
            order_books: {
                primary_key: 'id'
            }, 
            products: {
                primary_key: 'id'
            }, 
            sales: {
                primary_key: 'id'
            },
            types: {
                primary_key: 'id'
            },
            work_plans: {
                primary_key: 'id'
            }
        }
    };
    
};