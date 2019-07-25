import * as admin from "firebase-admin";

const config =
{
    apiKey: "XXXX",
    authDomain: "XXXX",
    databaseURL: "https://XXXX.firebaseio.com",
    projectId: "XXXX",
    storageBucket: "XXXX.appspot.com",
    messagingSenderId: "XXXX"
};

admin.initializeApp(config);

const firestore = admin.firestore();

const NUM_DOCS = 10000;

async function deleteData() {

    const collectionRef = firestore.collection("test_");
    const query = collectionRef.limit(400);
    return await new Promise((resolve, reject) => {
        deleteQueryBatch(firestore, query, resolve, reject);
    });
}

function deleteQueryBatch(firestore, query, resolve, reject) {
    query.get()
        .then(async (snapshot) => {
            const batch = firestore.batch();
            if (snapshot.size === 0) {
                return 0;
            }

            snapshot.docs.forEach((doc) => {
                batch.delete(doc.ref);
            });

            console.log('delete data');
            return await batch.commit().then(() => {
                return snapshot.size;
            });
        }).then((numDeleted) => {
            if (numDeleted === 0) {
                resolve();
                return;
            }

            process.nextTick(() => {
                deleteQueryBatch(firestore, query, resolve, reject);
            });
        })
        .catch(reject);
}


function setData(actions) {
    for (let i = 0; i < NUM_DOCS; i++) {
        console.log("---------- setData:" + i + ' --- time: ' + new Date().getHours() + ':' + new Date().getMinutes() + ':' + new Date().getSeconds());

        const docRef = firestore.collection("test_").doc();
        let guid = uuidv4();
        let data;

        data = hundleUndefined({
            GUID: guid,
            field_1: 'field_1_' + guid,
            field_2: 'field_2_' + guid,
            field_3: 'field_3_' + guid,
            field_4: 'field_4_' + guid,
            field_5: 'field_5_' + guid,
            field_6: new Object(),
            field_7: new Object(),
            field_8: new Object()
        });
        for (let i = 0; i < 10; i++) {
            guid = uuidv4();
            data.field_6['field_6' + guid] = hundleUndefined({
                field_1: 'field_1_' + guid,
                field_2: 'field_2_' + guid,
                field_3: 'field_3_' + guid,
            });
        }
        for (let i = 0; i < 10; i++) {
            guid = uuidv4();
            data.field_7['field_7' + guid] = hundleUndefined({
                field_1: 'field_1_' + guid,
                field_2: 'field_2_' + guid,
                field_3: 'field_3_' + guid,
            });
        }
        for (let i = 0; i < 5; i++) {
            var props = new Object();
            for (let i = 0; i < 5; i++) {
                guid = uuidv4();
                props['field_8' + guid] = hundleUndefined({
                    field_1: 'field_1_' + guid,
                    field_2: 'field_2_' + guid,
                    field_3: 'field_3_' + guid,
                });
            }
            data.field_8['field_8' + uuidv4()] = props;
        }
        actions.push({ type: 'set', args: [docRef, data] });
    }
}

function setActionsInBatchs(batches, actions) {
    let chunk = 500;
    let ind = -1;
    for (let i = 0; i < actions.length; i += chunk) {
        let sliced = actions.slice(i, i + chunk);
        ind++;
        batches[ind] = firestore.batch();
        for (let ref of sliced) {
            console.log('Push Action [' + ref.args[0].id + '] in Batch[' + ind + ']' + ' --- time: ' + new Date().getHours() + ':' + new Date().getMinutes() + ':' + new Date().getSeconds());
            batches[ind][ref.type](...ref.args)
        }
    }
}

async function commitPromises(batches) {

    let promisses = [];

    for (var i = 0; i < batches.length; i++) {
        if (batches[i]._writes.length > 0) {
            if (promisses.length < 15) {
                console.log('Push Batch ' + i + ' --- time: ' + new Date().getHours() + ':' + new Date().getMinutes() + ':' + new Date().getSeconds());
                promisses.push(batches[i].commit());
            } else {
                console.log('Processing promises with ' + promisses.length + ' batchs ' + ' --- time: ' + new Date().getHours() + ':' + new Date().getMinutes() + ':' + new Date().getSeconds());
                await Promise.all(promisses).catch((err) => { console.log(`batchCommit error: ${err}` + ' --- time: ' + new Date().getHours() + ':' + new Date().getMinutes() + ':' + new Date().getSeconds()) });
                promisses = [];
                promisses.push(batches[i].commit());
            }
        }
    }

    if (promisses.length > 0) {
        console.log('Processing promises with ' + promisses.length + ' batchs ' + ' --- time: ' + new Date().getHours() + ':' + new Date().getMinutes() + ':' + new Date().getSeconds());
        await Promise.all(promisses).catch((err) => { console.log(`batchCommit error: ${err}` + ' --- time: ' + new Date().getHours() + ':' + new Date().getMinutes() + ':' + new Date().getSeconds()) });
        promisses = [];
    }
}


async function start() {
    let actions = new Array;

    await deleteData();

    setData(actions);

    console.log("---------- Total Actions:" + actions.length + ' --- time: ' + new Date().getHours() + ':' + new Date().getMinutes() + ':' + new Date().getSeconds());

    let batches: FirebaseFirestore.WriteBatch[] = [];
    setActionsInBatchs(batches, actions);

    console.log("---------- Total Batches:" + batches.length + ' --- time: ' + new Date().getHours() + ':' + new Date().getMinutes() + ':' + new Date().getSeconds());

    await commitPromises(batches)
        .then(() => {
            console.log("all done" + ' --- time: ' + new Date().getHours() + ':' + new Date().getMinutes() + ':' + new Date().getSeconds());
        }).catch(err => {
            console.log(err + ' --- time: ' + new Date().getHours() + ':' + new Date().getMinutes() + ':' + new Date().getSeconds());
        });
}

function uuidv4() {
    return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
        var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
        return v.toString(16);
    });
}

function hundleUndefined(obj): any {
    const obj2 = obj;
    for (const prop in obj) {
        if (obj[prop] === undefined) {
            obj2[prop] = "";
        }
    }
    return obj2;
}


start();
