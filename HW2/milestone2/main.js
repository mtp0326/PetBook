/**
 * 
 */

var port = 8081;

const express = require('express');
const app = new express();
const path = require("path");
const JSON5 = require("json5")
const stemmer = require("stemmer")

const AWS = require("aws-sdk");

const {DynamoDB, QueryCommand} = require('@aws-sdk/client-dynamodb-v2-node');

AWS.config.update({region:'us-east-1'});

const client = new AWS.DynamoDB();

app.set("view engine", "pug");
app.set("views", path.join(__dirname, "views"))

app.get('/', function(request, response) {
    response.sendFile('html/index.html', { root: __dirname });
});

// TODO Add a route for /talk here

app.get('/talks', function(request, response) {
	var docClient = new AWS.DynamoDB.DocumentClient();
	
  // TODO Copy your /talks route from HW1 and expand it as described in the handout

});

app.listen(port, () => {
  console.log(`HW2 app listening at http://localhost:${port}`)
})
