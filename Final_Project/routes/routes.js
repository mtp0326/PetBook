var db = require('../models/database.js');
var session = require('express-session');

// TODO The code for your own routes should go here

var getMain = function(req, res) {
	
  	res.render('main.ejs', {});
};

var checklogin = function(req, res) {
	console.log(req);
	req.session.password = req.body.password;
	req.session.username = req.body.username;
  db.login(req.session.username, function(err, data) {
    if (err || data != req.session.password) {
      req.session.loggedIn=false;
      res.render("checklogin.ejs", {"check" : false});
    } else  {
    req.session.loggedIn=true;
     res.render("checklogin.ejs", {"check" : true});
    }
  });
};

var signup = function(req, res){
	res.render("signup.ejs", {});
};

var createaccount = function(req,res){
	req.session.password = req.body.password;
	req.session.username = req.body.username;
	req.session.fullname = req.body.fullname;
	db.login(session.username,function(err,data){
		if(err || data == null){
			req.session.loggedIn = true; 
			db.login_add(req.session.username, req.session.password, req.session.fullname, function(err,data) {});
			res.render("createaccount.ejs", {"check" : true});
			
		} else{
			res.render("createaccount.ejs", {"check" : false});
			req.session.loggedIn = false; 
		}
	});
};
var homepage = function(req, res) {
	db.restaurants(function(err, data){
		// 
		console.log(data);
		
		res.render("homepage.ejs",{"check" : req.session.loggedIn, "list" : data} );
		
	});
};

var chat = function(req,res){
	res.render("chat.ejs");
}

var getrestaurants = function(req, res){
	db.restaurants(function(err, data){
		res.send(JSON.stringify(data));
	});
};

var getUser = function(req, res){
	res.send(JSON.stringify(req.session.username));
}
var createrestaurant = function(req, res){
	var response;
	console.log(req);
	if(req.body.name.length == 0 || 
		req.body.latitude.length == 0 || 
		req.body.longitude.length == 0 || 
		req.body.description.length == 0 ){
			response = "error"
			res.send(JSON.stringify(response));
		}
	else{
		db.restaurant_add(req.body.name, req.session.username, req.body.latitude, req.body.longitude, req.body.description);
		response = 
		{
			"name" : req.body.name,
			"creator" : req.session.username,
			"longitude" :req.body.longitude,
			"latitude" : req.body.latitude,
			"description" : req.body.description

		};

		res.send(JSON.stringify(response) );
	}

};

var deleteres = function(req,res){
	db.delete_item(req.body.name, req.session.username, req.body.latitude, req.body.longitude, req.body.description);
	var response = 
	{
		"name" : req.body.name,
		"creator" : req.session.username,
		"longitude" :req.body.longitude,
		"latitude" : req.body.latitude,
		"description" : req.body.description

	};
	res.send(JSON.stringify(response));
}	

// var addrestaurant = function(req, res){
// 	if(req.body.name.length == 0){
// 		res.render("addrestaurant.ejs", {"error":0});
// 	}
// 	else if(req.body.latitude.length == 0){
// 		res.render("addrestaurant.ejs", {"error":1});
// 	}
// 	else if(req.body.longitude.length == 0){
// 		res.render("addrestaurant.ejs", {"error":2});
// 	}
// 	else if(req.body.description.length == 0){
// 		res.render("addrestaurant.ejs", {"error":3});
// 	}
// 	else{
// 		db.restaurant_add(req.body.name, session.username, req.body.latitude, req.body.longitude, req.body.description);
// 		res.render("addrestaurant.ejs", {"error":4});
// 	}
// };





var logout = function(req,res){
	req.session.loggedin = false;
	res.render("logout.ejs", {});
};


// TODO Don't forget to add any new functions to this class, so app.js can call them. (The name before the colon is the name you'd use for the function in app.js; the name after the colon is the name the method has here, in this file.)

var routes = { 
  get_main: getMain,
  signup: signup,
  checklogin: checklogin,
  createaccount: createaccount,
  chat: chat,
 
  homepage: homepage,
//   addrestaurant: addrestaurant,
getrestaurants: getrestaurants,
  createrestaurant: createrestaurant,
  getUser: getUser,
  logout: logout,
  deleteres : deleteres
};

module.exports = routes;
