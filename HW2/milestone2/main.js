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

app.get('/talk', function(request, response) {
 var search = parseInt(request.query.id);
 var docClient = new AWS.DynamoDB.DocumentClient();
 var params={
	TableName: "ted_talks",
	KeyConditionExpression: "talk_id = :talk_id",
	
	ExpressionAttributeValues:{
		":talk_id": search
	}
	
};

results = new Array;


//query with my talk_id from ted_talks table
docClient.query(params, function(err,data){
	if(err){
		console.log(err); 
	}else{
		//parsing my topics and related talks to a better formate
		results.push(data.Items[0])
		results[0].topics = JSON5.parse(results[0].topics);
		results[0].related_talks = JSON5.parse(results[0].related_talks.replace(/([0-9]+)/g, "\"$&\""));
		//render the results 
		response.render("results",{ "search": parseInt(request.query.id), "results": results});	
		
	
	}
});
});


app.get('/talks', function(request, response) {
 keywords = new Array;
 promisearr = new Array;
 set = new Set;
 keywords = stemmer((request.query.keyword).toLowerCase().split(" "));
 var docClient = new AWS.DynamoDB.DocumentClient();
 // for every keyword create a new params
 for( let i = 0; i < keywords.length; i++){
	var params={
	TableName: "inverted",
	KeyConditionExpression: "keyword = :keyword",
	
	ExpressionAttributeValues:{
		":keyword": keywords[i]
	}
	};
	//store the promises into an array
	promisearr.push(docClient.query(params).promise());
	
	
}
results = new Array;
Promise.all(promisearr).then(
	success =>{
		var tedTalksArr = new Set;
		//take the first 15 items
		for(let i = 0; i < success.length; i++){
			for(let j = 0; j < success[i].Items.length; j++){
				if (set.size == 15){
					break;
				}
				if(!set.has(success[i].Items[j].inxid)){
					set.add(success[i].Items[j].inxid);
				}
			}	
		}
		
		for(let s of set){
			 var params={
				TableName: "ted_talks",
				KeyConditionExpression: "talk_id = :talk_id",
	
				ExpressionAttributeValues:{
				":talk_id": s
				}
	
		};
			tedTalksArr.add(docClient.query(params).promise());
			
		}
		//get all the ted talks
		Promise.all(tedTalksArr).then(
			success => {
				finalResult = new Array;
				for(let i = 0; i < success.length; i++){
					var s = success[i];
					// parsing the result
					s.Items[0].topics = JSON5.parse(s.Items[0].topics);
					s.Items[0].related_talks = JSON5.parse(s.Items[0].related_talks.replace(/([0-9]+)/g, "\"$&\""));
					finalResult.push(s.Items[0]);
					console.log(finalResult);
			
				}
				response.render("results",{ "search": request.query.keyword, "results": finalResult});	
			},
			error =>{
				for(let i = 0 ; i < error.length; i++){
					console.log(error[i]);
				}
			}
		)
	},
	error =>{
		for(let i = 0 ; i < error.length; i++){
			console.log(error[i]);
		}
		
		
	}
		
		
);
 
 
		

  // TODO look up the word (or, in the case of EC2, words) in 'terms' in DynamoDB and hand them over as the variable 'results' below

 
});





app.listen(port, () => {
  console.log(`HW2 app listening at http://localhost:${port}`)
})




	
		



