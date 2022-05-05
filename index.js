/**
 * This exercise has you implement a synchronize() method that will
 * copy all records from the sourceDb into the targetDb() then start
 * polling for changes. Places where you need to add code have been
 * mostly marked and the goal is to get the runTest() to complete
 * successfully.
 * 
 *
 * Requirements:
 *
 * Try to solve the following pieces for a production system. Note,
 * doing one well is better than doing all poorly. If you are unable to
 * complete certain requirements, please comment your approach to the 
 * solution. Comments on the "why" you chose to implement the code the
 * way you did is highly preferred.
 *
 * 1. syncAllNoLimit(): Make sure what is in the source database is 
 *    in our target database for all data in one sync. Think of a naive
 *    solution.
 * 2. syncAllSafely(): Make sure what is in the source database is in
 *    our target database for all data in batches during multiple
 *    syncs. Think of a pagination solution.
 * 3. syncNewChanges(): Make sure updates in the source database are in
 *    our target database without syncing all data for all time. Think
 *    of a delta change solution using operators.
 * 4. synchronize(): Create polling for the sync. If haven't synced before,
 *    full sync all records from the source database. Otherwise, a cadence
 *    where we check the source database for updates to sync.
 *
 * Assumptions:
 *
 * Assume that you only have read-only access to the sourceDb and write-only 
 * access to the targetDb. Of course, for tests, you can go outside these 
 * assumptions to properly test; however, for the general logic, assume this
 * level of access.
 *
 * Feel free to use any libraries that you are comfortable with and
 * change the parameters and returns to solve for the requirements.
 *
 * Reach out to rspoone@mediafly.com for clarity and general questions.
 *
 * You will need to reference the following API documentation:
 * 
 * Required: https://www.npmjs.com/package/nedb
 * Recommended: https://lodash.com/docs/4.17.15
 * Recommended: https://www.npmjs.com/package/await-the#api-reference
 */

const Datastore = require('nedb-promises');
const _ = require('lodash');
const the = require('await-the');
const faker = require('faker');

// The source database to sync updates from.
const sourceDb = new Datastore({
    inMemoryOnly: true,
    timestampData: true
});

// The target database that sendEvents() will write too.
const targetDb = new Datastore({
    inMemoryOnly: true,
    timestampData: true
});

let TOTAL_RECORDS;
let EVENTS_SENT = 0;
const REGISTERS_UPDATED = [];

/**
 * Mock function to load data into the sourceDb.
 */
const load = async () => {
    // Add some documents to the collection.
    // TODO: Maybe dynamically do this? `faker` might be a good library here.

    const records = [
        new Promise((res) => { sourceDb.insert({ name: 'GE', owner: 'test', amount: 1000000 }).then((data) => { sendEvent(data); res(data); })}),
        new Promise((res) => { sourceDb.insert({ name: 'Exxon', owner: 'test2', amount: 5000000 }).then((data) => { sendEvent(data); res(data); })}),
        new Promise((res) => { sourceDb.insert({ name: 'Google', owner: 'test3', amount: 5000001 }).then((data) => { sendEvent(data); res(data); })}),
    ];

    for (let i = 0; i < 10; ++i) {
        const reg = {
            name: faker.company.companyName(),
            owner: faker.name.firstName(),
            amount: faker.finance.amount()
        };
        records.push(new Promise((res) => { sourceDb.insert(reg).then((data) => { sendEvent(data); res(data); })}));
    }
    
    await Promise.all(records);
    TOTAL_RECORDS = records.length;
}


/**
 * Mock function to find and update an existing document in the
 * sourceDb.
 */
const touch = async name => {
    await sourceDb.update({ name }, { $set: { owner: 'test4' } });
    REGISTERS_UPDATED.push(name);
};


/**
 * API to send each document to in order to sync.
 */
const sendEvent = (data, name = '') => {
    EVENTS_SENT += 1;
    console.log(`event being sent: ${name}`);
    console.log(data);

    // TODO: Write data to targetDb
    // await targetDb.insert(data);
};


/**
 * Utility to log one record to the console for debugging.
 */
const read = async name => {
    const record = await sourceDb.findOne({ name });
    console.log(record);
};


/**
 * Get all records out of the database and send them to the targetDb.
 */
const syncAllNoLimit = async () => {
    // TODO
    console.log("[START]: Syncing All No Limit....");
    const allRecords = await sourceDb.find({});
    for (let i = 0; i < allRecords.length; i++) {
        await targetDb.insert(allRecords[i]);
        sendEvent(allRecords[i]);
    }
    
    console.log(`[FINISH]: ${allRecords.length} records synced to Target DB.`);
}


/**
 * Sync up to the provided limit of records.
 */
const syncWithLimit = async (limit, data) => {
    // TODO
    console.log(`[START]: Syncing with Limit of ${limit} records.`);
    if (data.lastResultSize === -1) {
        data.lastResultSize = 0;
    }

    const records = await sourceDb.find({}).skip(data.lastResultSize).limit(limit);

    for (let i = 0; i < records.length; i++) {
        try {
            await targetDb.insert(records[i]);
            sendEvent(records[i]);
        } catch (e) {
            console.error(e.message);
        }
    }
    data.lastResultSize += limit;
    console.log(`[FINISH]: ${limit} records synced to Target DB.`);
    return data;
}


/**
 * Synchronize all records in batches.
 */
const syncAllSafely = async (batchSize, data) => {
    // FIXME: Example implementation.
    console.log(`[START]: Syncing All Safely with batchSize of ${batchSize}.`);
    if (_.isNil(data)) {
        data = {}
    }

    data.lastResultSize = -1;
    await the.while(
        () => data.lastResultSize < TOTAL_RECORDS,
        async () => await syncWithLimit(batchSize, data)
    );
    console.log(`[FINISH]: Synced All Safely with batchSize of ${batchSize}.`);
    return data;
}


/**
 * Sync changes since the last time the function was called with
 * with the passed in data.
 */
const syncNewChanges = async () => {
    // TODO
    console.log(`[START]: Syncing New Changes.`);
    const promsForUpdate = REGISTERS_UPDATED.map(name => {
        return new Promise(resolve => {
            sourceDb.find({ name }).then(data => {
                targetDb.update({ name }, { $set: { ...data } });
                sendEvent(data, 'SYNC NEW CHANGE:');
                resolve(data);
            });
        });
    });
    
    await Promise.all(promsForUpdate);
    REGISTERS_UPDATED.splice(0, REGISTERS_UPDATED.length);
    console.log(`[FINISH]: Syncing New Changes.`);
}


/**
 * Implement function to fully sync of the database and then 
 * keep polling for changes.
 */
const synchronize = async () => {
    // TODO
}


/**
 * Simple test construct to use while building up the functions
 * that will be needed for synchronize().
 */
const runTest = async () => {
    await load();

    console.log({ TOTAL_RECORDS });

    // Check what the saved data looks like.
    await read('GE');

    EVENTS_SENT = 0;
    await syncAllNoLimit();

    // TODO: Maybe use something other than logs to validate use cases?
    // Something like `mocha` with `assert` or `chai` might be good libraries here.
    if (EVENTS_SENT === TOTAL_RECORDS) {
        console.log('1. synchronized correct number of events')
    }

    EVENTS_SENT = 0;
    const data = await syncAllSafely(1);

    if (EVENTS_SENT === TOTAL_RECORDS) {
        console.log('2. synchronized correct number of events')
    }

    // Make some updates and then sync just the changed files.
    EVENTS_SENT = 0;
    await the.wait(300);
    await touch('GE');
    await syncNewChanges();

    if (EVENTS_SENT === 1) {
        console.log('3. synchronized correct number of events')
    }
}

// TODO:
// Call synchronize() instead of runTest() when you have synchronize working
// or add it to runTest().
runTest();
// synchronize();