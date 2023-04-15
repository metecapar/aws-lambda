const fs = require('../../../../node_modules/fs');
const csv = require('../../../../node_modules/csv-parser');
const moment = require('../../../../node_modules/moment');
const { groupBy, sumBy, uniqBy } = require('../../../../node_modules/lodash');
const amqp = require('../../../../node_modules/amqplib/callback_api');
const { v4: uuidv4 } = require('../../../../node_modules/uuid');

// Get the current time and format the date
const fileDate = moment().format('DDMMYYYY');

// Define file names
const customerFile = `customers_${fileDate}.csv`;
const ordersFile = `orders_${fileDate}.csv`;
const itemsFile = `items_${fileDate}.csv`;



function publishToQueue(queue, messages) {
    amqp.connect('amqp://user:password@host', (error0, connection) => {
      if (error0) {
        throw error0;
      }
      connection.createChannel((error1, channel) => {
        if (error1) {
          throw error1;
        }
  
        channel.assertQueue(queue, {
          durable: true,
        });
  
        messages.forEach((message) => {
          const msg = JSON.stringify(message);
          channel.sendToQueue(queue, Buffer.from(msg));
          console.log(`Sent message: ${msg}`);
        });
      });
    });
  }


// Helper function to read CSV files
const readCSV = (file) =>
    new Promise((resolve, reject) => {
        const rows = [];
        fs.createReadStream(file)
            .pipe(csv())
            .on('data', (row) => rows.push(row))
            .on('end', () => resolve(rows))
            .on('error', (error) => reject(error));
    });

Promise.all([
    readCSV(customerFile),
    readCSV(ordersFile),
    readCSV(itemsFile),
])
    .then(([customers, orders, items]) => {
        // Merge DataFrames
        const ordersItems = items.map((item) => {
            const order = orders.find((order) => order.order_reference === item.order_reference);
            return { ...order, ...item };
        });
        // Filter out rows with undefined customer references
        const ordersItemsWithCustomerRef = ordersItems.filter(
            (orderItem) => orderItem.customer_reference
        );

        const undefinedCustomerRefs = ordersItems.filter(
            (orderItem) => !orderItem.customer_reference
        );

        const undefinedCustomerRefErrors = undefinedCustomerRefs.map((row) => ({
            type: 'error_message',
            customer_reference: null,
            order_reference: row.order_reference,
            message: 'Customer reference is undefined.',
        }));

        const ordersCustomers = ordersItems.map((orderItem) => {
            const customer = customers.find((customer) => customer.customer_reference === orderItem.customer_reference);
            return { ...orderItem, ...customer };
        });

        // Calculate total amount spent and number of orders per customer
        const totalAmountSpent = Object.entries(groupBy(ordersCustomers, 'customer_reference'))
            .map(([customer_reference, group]) => ({
                customer_reference,
                total_price: sumBy(group, (item) => parseFloat(item.total_price) || 0),
            }));

        const numberOfOrders = Object.entries(groupBy(ordersCustomers, 'customer_reference'))
            .map(([customer_reference, group]) => ({
                customer_reference,
                order_count: uniqBy(group, 'order_reference').length,
            }));

        // Create customer summary
        const customerSummary = numberOfOrders.map((orderData) => {
            const totalSpentData = totalAmountSpent.find(
                (amountData) => amountData.customer_reference === orderData.customer_reference,
            );
            return {
                ...orderData,
                ...totalSpentData,
            };
        });

        // Convert customer summary to a list of dictionaries
        const customerMessages = customerSummary.map((row) => ({
            ...row,
            type: 'customer_message',
        }));

        // Check for negative total prices in items
        const errorMessages = items
            .filter((row) => parseFloat(row.total_price) < 0)
            .map((row) => ({
                type: 'error_message',
                customer_reference: null,
                order_reference: row.order_reference,
                message: 'Total price should not be negative.',
            }));

        // Print results
        console.log(JSON.stringify(customerMessages));
        console.log([...errorMessages, ...undefinedCustomerRefErrors]);

        const queue1 = 'mete1';
        publishToQueue(queue1, customerMessages);
      
        const queue2 = 'mete2';
        publishToQueue(queue2, [...errorMessages, ...undefinedCustomerRefErrors]);
    })
    .catch((error) => console.error(`Error: ${error.message}`));
