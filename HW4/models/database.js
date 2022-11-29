var AWS = require('aws-sdk');
AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();

/* The function below is an example of a database method. Whenever you need to 
   access your database, you should define a function (myDB_addUser, myDB_getPassword, ...)
   and call that function from your routes - don't just call DynamoDB directly!
   This makes it much easier to make changes to your database schema. */

var login = function(searchTerm, callback) {
  console.log('Login: ' + searchTerm); 

  var params = {
	TableName : "users",	
     KeyConditions: {
        username: {
          ComparisonOperator: 'EQ',
          AttributeValueList: [ { S: searchTerm } ]
        }
      },
      AttributesToGet: [ 'password' ]
  };

  db.query(params, function(err, data) {
	
    if (err || data.Items.length == 0) {
      callback(err, null);
    } else {
      callback(err, data.Items[0].password.S);
    }
  });
};

var login_add = function(keyword, password, fullname) {
	var params={
		TableName : "users",	
		Item: {
			"username" :{
				S: keyword
			},
			"password" :{
				S: password
			},
			"fullname" :{
				S: fullname
			}
			
		}
	};
	db.putItem(params, function(err,data){
		if(err){
			console.log(err);
		}
		
	});
	
		
}

var restaurants = function(callback) {
  var params = {
	TableName : "restaurants",	
	Select: "ALL_ATTRIBUTES"
};
  

  

  db.scan(params, function(err, data) {
   callback(err, data.Items);
  })
};

var delete_item = function(name, username, latitude, longitude, description){
	var params = {
		TableName : "restaurants",	
			Key :{
				"name" :{
					S: name
				} 
				
			}
	  };
	  db.deleteItem(params, function(err,data){
		console.log(data);
		if(err){
			console.log(err);
		}
	});
};

var restaurant_add = function(name, username, latitude, longitude, description) {
	var params = {
	TableName : "restaurants",	
    	Item :{
			"name":{
				S: name
			},
			"creator":{
				S: username
			},
			"latitude":{
				S: latitude
			},
			"longitude":{
				S: longitude
			},
			"description":{
				S: description
			}
			
		}
  };
	db.putItem(params, function(err,data){
		console.log(data);
		if(err){
			console.log(err);
		}
	});
	
		
};

// TODO Your own functions for accessing the DynamoDB tables should go here

/* We define an object with one field for each method. For instance, below we have
   a 'lookup' field, which is set to the myDB_lookup function. In routes.js, we can
   then invoke db.lookup(...), and that call will be routed to myDB_lookup(...). */

// TODO Don't forget to add any new functions to this class, so app.js can call them. (The name before the colon is the name you'd use for the function in app.js; the name after the colon is the name the method has here, in this file.)

var database = { 
	login : login,
	login_add: login_add,
	restaurants: restaurants,
	delete_item: delete_item,
	restaurant_add: restaurant_add
};

module.exports = database;
                                        