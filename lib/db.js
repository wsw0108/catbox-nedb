'use strict';

const Path = require('path');
const DataStore = require('nedb');
const Hoek = require('hoek');


// Declare internals

const internals = {};


exports = module.exports = internals.DummyDB =  function (options) {

    Hoek.assert(this instanceof internals.DummyDB, 'Dummy DB must be instantiated using new');

    this.options = options || {};
    this.base = this.options.base;
    this.ext = this.options.ext || 'nedb';
    return this;
};


internals.DummyDB.prototype.collection = function (name, callback) {

    const options = Hoek.applyToDefaults(this.options, {
        autoload: false
    });

    if (this.base) {
        const base = this.base;
        const subdir = this.options.partition;
        const filename = name + '.' + this.ext;
        options.filename = Path.join(base, subdir, filename);
    }

    const store = new DataStore(options);

    store.loadDatabase((err) => {

        if (err) {
            return callback(err);
        }

        callback(null, store);
    });
};


internals.DummyDB.prototype.close = function () {
};
