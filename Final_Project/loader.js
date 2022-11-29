/* This code loads some test data into a DynamoDB table. You _could_ modify this
   to upload test data for HW4 (which has different tables and different data),
   but you don't have to; we won't be grading this part. If you prefer, you can
   just stick your data into DynamoDB tables manually, using the AWS web console. */

var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();
var async = require('async');

/* We begin by defining the name of the table and the data we want to upload */

var wordDBname = "users";
var users = [ ["mickey", "mouse", "Mickey Mouse"]];

/* The function below checks whether a table with the above name exists, and if not,
   it creates such a table with a hashkey called 'keyword', which is a string. 
   Notice that we don't have to specify the additional columns in the schema; 
   we can just add them later. (DynamoDB is not a relational database!) */

var initTable = function(tableName, callback) {
  db.listTables(function(err, data) {
    if (err)  {
      console.log(err, err.stack);
      callback('Error when listing tables: '+err, null);
    } else {
      console.log("Connected to AWS DynamoDB");
          
      var tables = data.TableNames.toString().split(",");
      console.log("Tables in DynamoDB: " + tables);
      if (tables.indexOf(tableName) == -1) {
        console.log("Creating new table '"+tableName+"'");

        var params = {
            AttributeDefinitions: 
              [ 
                {
                  AttributeName: 'username',
                  AttributeType: 'S'
                }
              ],
            KeySchema: 
              [ 
                {
                  AttributeName: 'username',
                  KeyType: 'HASH'
                }
              ],
            ProvisionedThroughput: { 
              ReadCapacityUnits: 20,       // DANGER: Don't increase this too much; stay within the free tier!
              WriteCapacityUnits: 20       // DANGER: Don't increase this too much; stay within the free tier!
            },
            TableName: tableName /* required */
        };

        db.createTable(params, function(err, data) {
          if (err) {
            console.log(err)
            callback('Error while creating table '+tableName+': '+err, null);
          }
          else {
            console.log("Table is being created; waiting for 20 seconds...");
            setTimeout(function() {
              console.log("Success");
              callback(null, 'Success');
            }, 20000);
          }
        });
      } else {
        console.log("Table "+tableName+" already exists");
        callback(null, 'Success');
      }
    }
  });
}

/* This function puts an item into the table. Notice that the column is a parameter;
   hence the unusual [column] syntax. This function might be a good template for other
   API calls, if you need them during the project. */

var putIntoTable = function(tableName, keyword, password, fullname, callback) {
  var params = {
      Item: {
        "username": {
          S: keyword
        },
        "password": { 
          S: password
        },
        "fullname": { 
          S: fullname
        },
       
      },
      TableName: tableName,
      ReturnValues: 'NONE'
  };

  db.putItem(params, function(err, data){
	console.log(data);
    if (err)
      callback(err)
    else
      callback(null)
  });
}

/* This is the code that actually runs first when you run this file with Node.
   It calls initTable and then, once that finishes, it uploads all the words
   in parallel and waits for all the uploads to complete (async.forEach). */

initTable("users", function(err, data) {
  if (err)
    console.log("Error while initializing table: "+err);
  else {
    async.forEach(users, function (user, callback) {
      console.log("uploading username: " + user[0]);
      putIntoTable("users", user[0], user[1], user[2], function(err) {
        if (err)
          console.log("Oops, error when adding "+user[0]+": " + err);
      });
    }, function() { console.log("Upload complete")});
  }
});