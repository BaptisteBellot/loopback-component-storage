// Copyright IBM Corp. 2013,2019. All Rights Reserved.
// Node module: loopback-component-storage
// This file is licensed under the Artistic License 2.0.
// License text available at https://opensource.org/licenses/Artistic-2.0

'use strict';

// Globalization
var g = require('strong-globalize')();

/**
 * File system based on storage provider
 */

var fs = require('fs'),
  path = require('path'),
  stream = require('stream'),
  async = require('async'),
  File = require('./file').File,
  Container = require('./container').Container;
var AWS = require('aws-sdk');
var uuid = require('uuid/v4');
var Readable = stream.Readable;

var utils = require('../../utils');

module.exports.storage = module.exports; // To make it consistent with pkgcloud

module.exports.File = File;
module.exports.Container = Container;
module.exports.Client = AmazonProvider;
module.exports.createClient = function(options) {
  return new AmazonProvider(options);
};

function AmazonProvider(options) {
  options = options || {};

  AWS.config.update({credentials: new AWS.Credentials(options.keyId,options.key)});
  AWS.config.update({region: options.region});


  this.tmpFolder = path.join(__dirname,'tmp');
  this.S3 = new AWS.S3({apiVersion: '2006-03-01'});
  this.S3.listBuckets((err, data) => {
    if (err) {
      throw new Error("Error", err);
    } else {
      console.log("Success", data.Buckets);
      this.BaseBucket = data.Buckets.filter(item => item.Name == options.bucket).pop();
      if(!this.BaseBucket) {
        // Create the parameters for calling createBucket
        let bucketParams = {
          Bucket : options.bucket || "loopback-default-storage",
          ACL : 'public-read'
        };

        // call S3 to create the bucket
        this.S3.createBucket(bucketParams, (err, data) => {
          if (err) {
            console.log("Error", err);
          } else {
            console.log("Success", data);
            this.BaseBucket = data;
          }
        });
      } else {
        console.log("Success", this.BaseBucket);
      }

    }
  });


}

var namePattern = new RegExp('[^' + path.sep + '/]+');
// To detect any file/directory containing dotdot paths
var containsDotDotPaths = /(^|[\\\/])\.\.([\\\/]|$)/;

function validateName(name, cb) {
  if (!name || containsDotDotPaths.test(name)) {
    cb && process.nextTick(cb.bind(null, new Error(g.f('Invalid name: %s', name))));
    if (!cb) {
      console.error(g.f('{{AmazonProvider}}: Invalid name: %s', name));
    }
    return false;
  }
  var match = namePattern.exec(name);
  if (match && match.index === 0 && match[0].length === name.length) {
    return true;
  } else {
    cb && process.nextTick(cb.bind(null,
      new Error(g.f('{{AmazonProvider}}: Invalid name: %s', name))));
    if (!cb) {
      console.error(g.f('{{AmazonProvider}}: Invalid name: %s', name));
    }
    return false;
  }
}

function streamError(errStream, err, cb) {
  process.nextTick(function() {
    errStream.emit('error', err);
    cb && cb(null, err);
  });
  return errStream;
}

var writeStreamError = streamError.bind(null, new stream.Writable());
var readStreamError = streamError.bind(null, new stream.Readable());

/*!
 * Populate the metadata from file stat into props
 * @param {fs.Stats} stat The file stat instance
 * @param {Object} props The metadata object
 */
function populateMetadata(stat, props) {
  for (var p in stat) {
    switch (p) {
      case 'Size':
        props["size"] = stat[p];
        break;
      case 'LastModified':
        props["mtime"] = stat[p];
        break;
      
    }
  }
}

AmazonProvider.prototype.getContainers = function(cb) {
  cb = cb || utils.createPromiseCallback();

  var self = this;
  this.BaseBucket
  this.S3.listObjects({Bucket: self.BaseBucket.Name, Delimiter: "/"}, (err, data) => {
    let containers = data.CommonPrefixes.map(item => new Container(self, {name: item.Prefix.slice(0,-1)}));
    cb && cb(err, containers);
  });


  

  return cb.promise;
};

AmazonProvider.prototype.createContainer = function(options, cb) {
  cb = cb || utils.createPromiseCallback();

  var self = this;
  var name = options.name;
  if(validateName(name, cb)) {
    // call S3 to retrieve upload file to specified bucket
    var uploadParams = {Bucket: self.BaseBucket.Name, Key: '', Body: ''};
    var file = Readable.from('');
    
    // Configure the file stream and obtain the upload parameters
    var fileStream = Readable.from('');
    fileStream.on('error', (err) => {
      console.log('File Error', err);
      cb && cb(err);
        return;
    });
    uploadParams.Body = fileStream;
    uploadParams.Key = name+'/.ignore';
    
    // call S3 to retrieve upload file to specified bucket
    self.S3.upload (uploadParams,  (err, data) => {
      if (err) {
        console.log("Error", err);
        cb && cb(err);
        return;
      } 
      if (data) {
        console.log("Upload Success", data.Location);
        var props = {name: name};
        var container = new Container(self, props);
      
        cb && cb(err, container);
      }
    });
  }


 
  return cb.promise;
};

AmazonProvider.prototype.destroyContainer = function(containerName, cb) {
  cb = cb || utils.createPromiseCallback();

  var self = this;
  if (!validateName(containerName, cb)) return;

  this.S3.listObjects({Bucket: self.BaseBucket.Name, Prefix: containerName+"/"}, (err, data) => {
    if(err) return cb && cb(err);
    let objects = [];
    data.Contents.forEach(item => {
      objects.push({Key: item.Key});
    });
    self.S3.listObjects({Bucket: self.BaseBucket.Name, Delete: {Objects: objects, Quiet: false}}, (err, data) => {
      if(err) return cb && cb(err);
      cb && cb(err, data.Deleted);
    });
    
  });


  return cb.promise;
};

AmazonProvider.prototype.getContainer = function(containerName, cb) {
  cb = cb || utils.createPromiseCallback();

  var self = this;
  if (!validateName(containerName, cb)) return;
  this.S3.listObjects({Bucket: self.BaseBucket.Name, Prefix: containerName+"/"}, (err, data) => {
    if(err) return cb && cb(err);
    if(data.Contents.length > 0){
      var props = {name: name};
        var container = new Container(self, props);
      
        cb && cb(err, container);
    } else {
      cb && cb(new Error(g.f('{{AmazonProvider}}: Invalid name: %s', container)))
    }
    
    
  });
  return cb.promise;
};

// File related functions
AmazonProvider.prototype.upload = function(options, cb) {
  
  var self = this;
  var container = options.container;
  if (!validateName(container)) {
    return writeStreamError(
      new Error(g.f('{{AmazonProvider}}: Invalid name: %s', container)),
      cb
    );
  }
  var file = options.remote;
  if (!validateName(file)) {
    return writeStreamError(
      new Error(g.f('{{AmazonProvider}}: Invalid name: %s', file)),
      cb
    );
  }
  var filePath = path.join(self.tmpFolder, uuid()+"_"+file);

  var fileOpts = {flags: options.flags || 'w+',
    encoding: options.encoding || null,
    mode: options.mode || parseInt('0666', 8),
  };
  var uploadParams = {Bucket:self.BaseBucket.Name, Key: '', Body: ''};

  try {
    // simulate the success event in filesystem provider
    // fixes: https://github.com/strongloop/loopback-component-storage/issues/58
    // & #23 & #67
    var stream = fs.createWriteStream(filePath, fileOpts);
    stream.on('finish', function() {
      var fileStream = fs.createReadStream(filePath);
      fileStream.on('error', function(err) {
        console.log('File Error', err);
      });
      uploadParams.Body = fileStream;
      uploadParams.Key = container+"/"+file;

      // call S3 to retrieve upload file to specified bucket
      self.S3.upload (uploadParams, function (err, data) {
        if (err) {
          console.log("Error", err);
        } if (data) {
          console.log("Upload Success", data.Location);
          stream.emit('success');
          fs.unlinkSync(filePath);
        }
      });
      
    });
    return stream;
  } catch (e) {
    return writeStreamError(e, cb);
  }
};

AmazonProvider.prototype.download = function(options, cb) {
  var self = this;
  cb = cb || utils.createPromiseCallback();
  var container = options.container;
  if (!validateName(container, cb)) {
    return readStreamError(
      new Error(g.f('{{AmazonProvider}}: Invalid name: %s', container)),
      cb
    );
  }
  var file = options.remote;
  if (!validateName(file, cb)) {
    return readStreamError(
      new Error(g.f('{{AmazonProvider}}: Invalid name: %s', file)),
      cb
    );
  }


  var fileOpts = {flags: 'r',
    autoClose: true};

  if (options.start) {
    fileOpts.start = options.start;
    fileOpts.end = options.end;

  }
  this.S3.getObject({Bucket: self.BaseBucket.Name, Key: container+"/"+file}, (err, data) => {
    if (err) {
      return readStreamError(
        new Error(g.f('{{AmazonProvider}}: Invalid file: %s', file)),
        cb
      );
    }
    console.log('file data', data);
    var filePath = path.join(self.tmpFolder, uuid()+"_"+file);
    try {
      fs.writeFileSync(filePath, data.Body);
      let stream = fs.createReadStream(filePath, fileOpts);
      cb && cb(err, stream);
      fs.unlinkSync(filePath);
    } catch(e) {
      cb(e);
    }
  });

  return cb.promise;
};

AmazonProvider.prototype.getFiles = function(container, options, cb) {
  if (typeof options === 'function' && !(options instanceof RegExp)) {
    cb = options;
    options = false;
  }

  cb = cb || utils.createPromiseCallback();

  var self = this;
  if (!validateName(container, cb)) return;
  this.S3.listObjects({Bucket: self.BaseBucket.Name, Prefix: container+"/"}, (err, data) => {
    let files = [];
    data.Contents.forEach(item => {
      let name = item.Key.slice(String(container+'/').length);
      if (name == ".ignore") return;
      var props = {container: container, name: name};
      populateMetadata(item, props);
      files.push(new File(self, props));
    });
    cb && cb(err, files);
  });
  
  
 
  return cb.promise;
};

AmazonProvider.prototype.getFile = function(container, file, cb) {
  cb = cb || utils.createPromiseCallback();

  var self = this;
  if (!validateName(container, cb)) return;
  if (!validateName(file, cb)) return;
  this.S3.getObject({Bucket: self.BaseBucket.Name, Key: container+"/"+file, Range: "bytes=0-9"}, (err, data) => {
    var f = null;
    if (!err) {
      var props = {container: container, name: file};
      populateMetadata(data, props);
      console.log('file data', data);
      f = new File(self, props);
    }
    cb && cb(err, f);
  });

  return cb.promise;
};

AmazonProvider.prototype.getUrl = function(options) {
  options = options || {};
  var filePath = path.join(options.container, options.path);
  return filePath;
};

AmazonProvider.prototype.removeFile = function(container, file, cb) {
  cb = cb || utils.createPromiseCallback();

  if (!validateName(container, cb)) return;
  if (!validateName(file, cb)) return;

  var self = this;
  this.S3.deleteObject({Bucket: self.BaseBucket.Name, Key: container+"/"+file}, (err, data) => {
    if (!err) {
      var props = {container: container, name: file};
      populateMetadata(data, props);
      f = new File(self, props);
    }
    cb && cb(err, f);
  });

  return cb.promise;
};
