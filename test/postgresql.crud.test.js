/**
 * Created by germanamz on 2/22/17.
 */
'use strict';
var juggler = require('loopback-datasource-juggler');
var CreateDS = juggler.DataSource;
require('loopback-datasource-juggler/test/common.batch.js');
require('loopback-datasource-juggler/test/include.test.js');

require('./init');
var should = require('should');

var Post, db, created;

describe('lazyConnect', function () {
  it('should skip connect phase (lazyConnect = true)', function (done) {
    var dsConfig = {
      host: '127.0.0.1',
      port: 4,
      lazyConnect: true,
      debug: false,
    };
    var ds = getDS(dsConfig);

    var errTimeout = setTimeout(function () {
      done();
    }, 2000);
    ds.on('error', function (err) {
      clearTimeout(errTimeout);
      done(err);
    });
  });

  it('should report connection error (lazyConnect = false)', function (done) {
    var dsConfig = {
      host: '127.0.0.1',
      port: 4,
      lazyConnect: false,
      debug: false,
    };
    var ds = getDS(dsConfig);

    ds.on('error', function (err) {
      err.message.should.containEql('ECONNREFUSED');
      done();
    });
  });
});

var getDS = function (config) {
  var db = new CreateDS(require('../'), config);
  return db;
};

describe('crud postgresql connector', function () {
  before(function () {
    db = getDataSource();

    Post = db.define('Post', {
      title: {type: String, length: 255, index: true},
      content: {type: String},
      loc: 'GeoPoint',
      created: Date,
      approved: Boolean,
      data: {
        type: Object
      },
      files: [
        'String'
      ]
    });
    created = new Date();
  });

  it('should run migration', function (done) {
    db.automigrate('Post', function () {
      done();
    });
  });

  var post;

  it('should support create instances as jsonb', function (done) {
    Post.create({
      title: 'Yolotl',
      content: 'jjajajajakskjflkadf',
      created: new Date(),
      approved: true,
      data: {
        name: 'yolo'
      },
      files: ['hhh', 'kkk', 'lll']
    }, function (err, p) {
      if (err) console.log(err);
      should.not.exists(err);
      done();
    });
  });

  it('should support nested properties query in find', function (done) {
    Post.find({
      where: {
        'data.name': 'yolo',
        title: 'Yolotl'
      },
    }, function (err, posts) {
      if (err) return done(err);
      posts.length.should.eql(1);
      done();
    });
  });

  it('should support in array item query in find', function (done) {
    Post.find({
      where: {
        files: 'kkk'
      },
    }, function (err, posts) {
      if (err) return done(err);
      posts.length.should.eql(1);
      done();
    });
  });

  it('should save instance', function (done) {
    Post.find({
      where: {
        'data.name': 'yolo',
        title: 'Yolotl'
      },
    }, function (err, posts) {
      posts[0].save(function (err) {
        should.not.exists(err);
        if(err) return done(err);
        done();
      });

    });
  });

});
