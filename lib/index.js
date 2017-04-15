'use strict';

// Load modules

const Hoek = require('hoek');
const DummyDB = require('./db');


// Declare internals

const internals = {};


internals.defaults = {
    base: '',
    ext: 'nedb'
};

exports = module.exports = internals.Connection = function (options) {

    Hoek.assert(this instanceof internals.Connection, 'NeDB cache client must be instantiated using new');

    this.settings = this.getSettings(options);

    this.db = null;
    this.isConnectionStarted = false;
    this.isConnected = false;
    this.collections = {};
    this.startPending = null;           // Set to an array of callbacks if start pending
    return this;
};

internals.Connection.prototype.getSettings = function (options) {

    // merge with defaults
    const settings = Hoek.applyToDefaults(internals.defaults, options);

    return settings;
};

internals.Connection.prototype.start = function (callback) {

    // Check if already connected

    if (this.isConnected) {
        return callback();
    }

    // Check if start already pending

    if (this.startPending) {
        this.startPending.push(callback);
        return;
    }

    // Set start pending state

    this.startPending = [callback];

    const connected = (err) => {

        this.isConnected = !err;

        for (let i = 0; i < this.startPending.length; ++i) {
            this.startPending[i](err);
        }

        this.startPending = null;
    };

    // Connection started flag
    this.isConnectionStarted = true;

    // Open connection

    this.db = new DummyDB(this.settings);

    connected();
};


internals.Connection.prototype.stop = function () {

    if (this.db) {
        this.db.close();
        this.db = null;
        this.collections = {};
        this.isConnected = false;
        this.isConnectionStarted = false;
    }
};


internals.Connection.prototype.isReady = function () {

    return this.isConnected;
};


internals.Connection.prototype.validateSegmentName = function (name) {

    /*
     Collection names:

     - empty string is not valid
     - cannot contain "\0"
     */

    if (!name) {
        return new Error('Empty string');
    }

    if (name.indexOf('\0') !== -1) {
        return new Error('Includes null character');
    }

    return null;
};


internals.Connection.prototype.getCollection = function (name, callback) {

    if (!this.isConnected) {
        return callback(new Error('Connection not ready'));
    }

    if (!name) {
        return callback(new Error('Empty string'));
    }

    if (this.collections[name]) {
        return callback(null, this.collections[name]);
    }

    // Fetch collection


    this.db.collection(name, (err, collection) => {

        if (err) {
            return callback(err);
        }

        // Found
        collection.ensureIndex({ fieldName: 'expiresAt', expireAfterSeconds: 0 }, (err) => {

            if (err) {
                return callback(err);
            }

            this.collections[name] = collection;
            return callback(null, collection);
        });
    });
};


internals.Connection.prototype.get = function (key, callback) {

    if (!this.isConnectionStarted) {
        return callback(new Error('Connection not started'));
    }

    this.getCollection(key.segment, (err, collection) => {

        if (err) {
            return callback(err);
        }

        const criteria = { _id: key.id };
        collection.findOne(criteria, (err, record) => {

            if (err) {
                return callback(err);
            }

            if (!record) {
                return callback(null, null);
            }

            if (!record.value ||
                !record.stored) {

                return callback(new Error('Incorrect record structure'));
            }

            const envelope = {
                item: record.value,
                stored: record.stored.getTime(),
                ttl: record.ttl
            };

            return callback(null, envelope);
        });
    });
};


internals.Connection.prototype.set = function (key, value, ttl, callback) {

    if (!this.isConnectionStarted) {
        return callback(new Error('Connection not started'));
    }

    this.getCollection(key.segment, (err, collection) => {

        if (err) {
            return callback(err);
        }

        const expiresAt = new Date();
        expiresAt.setMilliseconds(expiresAt.getMilliseconds() + ttl);
        const record = {
            _id: key.id,
            value: value,
            stored: new Date(),
            ttl: ttl,
            expiresAt: expiresAt
        };

        const criteria = { _id: key.id };
        collection.update(criteria, record, { upsert: true }, (err, count) => {

            if (err) {
                return callback(err);
            }

            return callback();
        });
    });
};


internals.Connection.prototype.drop = function (key, callback) {

    if (!this.isConnectionStarted) {
        return callback(new Error('Connection not started'));
    }

    this.getCollection(key.segment, (err, collection) => {

        if (err) {
            return callback(err);
        }

        const criteria = { _id: key.id };
        collection.remove(criteria, {}, (err, count) => {

            if (err) {
                return callback(err);
            }

            return callback();
        });
    });
};
