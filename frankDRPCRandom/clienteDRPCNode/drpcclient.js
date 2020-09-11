var NodeDRPCClient = require('node-drpc');
ObjectID = require('mongodb').ObjectID
 assert = require('assert')

var hostName = "10.10.11.119";
//var hostName = "127.0.0.1";
var portNo = 3772;

var timeout = null;
var stormFunctionName = 'FrankManualDRPC';

var MSIDN = "34633998875";
var IMEI = "355828063300862";
var IMSI = "214040101801031";
var TOKEN = "db8e1a907d861cc132b4fc26d8f7b207a43bc6b192761756d48ef00326b53ee2";

// Create a new ObjectID
var objectId = new ObjectID();
// Verify that the hex string is 24 characters long
assert.equal(24, objectId.toHexString().length);


var userjson= { "id": objectId, "TOKEN" : TOKEN, "MSISDN": MSIDN ,"IMEI": IMEI, "IMSI": IMSI, "SCENARY": "MO" }
var senduser= JSON.stringify(userjson)
var nodeDrpcClient = new NodeDRPCClient(hostName, portNo, timeout);

        console.time(senduser);
        nodeDrpcClient.execute(stormFunctionName,senduser , function(err, response) {
        if (err) {
            console.error(err);
        } else {
            console.log("Storm DRPC Response : ", response);
            console.timeEnd(senduser);
        }
       });
