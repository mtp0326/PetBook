var db = require('../models/database.js');
var isCreated = true;
var isVerified = false;
// TODO The code for your own routes should go here
//gets the main page (USED POP-UP box instead of the red failed sign)
var getMain = function(req, res) {
  isCreated = true;
  isVerified = false;
  res.render('main.ejs');
};

//for results if the username and password are correct
var postResultsUser = function(req, res) {
  var usernameCheck = req.body.username;
  var passwordCheck = req.body.password;
  db.passwordLookup(usernameCheck, "username", function(err, data) {
    if (data == passwordCheck && !err) {
      req.session.username = req.body.username;
      req.session.password = req.body.password;
      res.render('checklogin.ejs', {"verify" : true});
      isVerified = true;
    } else {
      res.render('checklogin.ejs', {"verify" : false});
      isVerified = false;
    }
  });
};

//gets signup page
var getSignup = function(req, res) {
	res.render('signup.ejs', {"isCreated" : isCreated});
}

//gets logout page
var getLogout = function(req, res) {
	req.session.username = null;
  req.session.destroy();
	res.render('logout.ejs', {});
}

//check if new account can be created by receiving null (which means that username in db is empty)
//and create the new account and go to restaurants or fail and go back to signup.
var postNewAccount = function(req, res) {
  var usernameNewCheck = req.body.newUsername;
  db.usernameLookup(usernameNewCheck, "username", function(err, data) {
	if(data == null || err) {
    req.session.username = req.body.newUsername;
    req.session.password = req.body.newPassword;
    req.session.fullname = req.body.newFullname;
		db.createAccount(req.session.username, req.session.password, req.session.fullname, function(err, data){});
		isVerified = true;
		isCreated = true;
		res.render('createaccount.ejs', {"created" : true});
	} else {
		isVerified = false;
		isCreated = false;
		res.render('createaccount.ejs', {"created" : false});
	}
  });
};

//render homepage
//NEW: getHomepage, homepage.ejs
var getHomepage = function(req, res) {
	res.render('homepage.ejs', {"isVerified" : isVerified})
};

//ajax: query posts of friend's userid
//Also renders comments if exists
//NEW: getHomepagePostList, getAllPosts
var getHomepagePostListAjax = function(req, res) {
  var friendsList = [];
  var postsList = [];
  db.getFriends(req.session.username, function(err, data){
    friendsList.push(data);
  });
  for(const friend of friendsList) {
    db.getAllPosts(friend, function(err, data){
      postsList.push(data);
    });
  }
  postsList.sort((a, b) => a.timestamp.S.localeCompare(b.timestamp.S)).reverse();
	res.send(JSON.stringify(postsList))
};






//get all restaurants and login verification and put in restaurants
var getRestaurants = function(req, res) {
	res.render('restaurants.ejs', {"isVerified" : isVerified})
};

//ajax: get all data of restaurants
var getRestaurantList = function(req, res) {
	db.getAllRestaurants(function(err, data){
	  res.send(JSON.stringify(data))
  });
};

//ajax: get the creator information
var getCreator = function(req, res) {
	  res.send(JSON.stringify(req.session.username));
};

//create new restaurant in the db when all inputs exist
var postNewRestaurantAjax = function(req, res) {
  var latitude = req.body.latitude;
  var longitude = req.body.longitude;
  var resName = req.body.name;
  var description = req.body.description;
  if(latitude.length != 0 && longitude.length != 0 && resName.length != 0 && description.length != 0) {
	  db.createRestaurant(resName, latitude, longitude, description, req.session.username, function(err, data){});
    
    var response = {
      "name": resName,
      "latitude" : latitude,
      "longitude": longitude,
      "description": description,
      "creator": req.session.username
    };

    res.send(response);
  } else {
	  res.send(null);
  }
};

//ajax: deletes the restaurant data in db
var postDeleteRestaurantAjax = function(req, res) {
  var resName = req.body.name;
  db.deleteRestaurant(resName, function(err,data){});
  res.send(resName);
};

//create new restaurant in the db when all inputs exist
//var postNewRestaurant = function(req, res) {
//  var latitude = req.body.latitude;
//  var longitude = req.body.longitude;
//  var resName = req.body.restaurantName;
//  var description = req.body.description;
//  if(latitude.length != 0 && longitude.length != 0 && resName.length != 0 && description.length != 0) {
//	db.createRestaurant(resName, latitude, longitude, description, session.username, function(err, data){});
//	
//  	res.render('addrestaurant.ejs', {"allFields" : true}); //send? addrestaurant.ejs is unnecessary
//  } else {
//	res.render('addrestaurant.ejs', {"allFields" : false});
//  }
//};

// TODO Don't forget to add any new functions to this class, so app.js can call them. (The name before the colon is the name you'd use for the function in app.js; the name after the colon is the name the method has here, in this file.)

var routes = { 
  get_main: getMain,
  verifyUser: postResultsUser,
  get_restaurants : getRestaurants,
  get_restaurantList : getRestaurantList,
  get_signup : getSignup,
  get_logout : getLogout,
  get_creator : getCreator,

  //NEW
  get_homepage : getHomepage,
  get_homepagePostListAjax : getHomepagePostListAjax,

  post_newAccount : postNewAccount,
  post_newRestaurantAjax : postNewRestaurantAjax,
  post_deleteRestaurantAjax : postDeleteRestaurantAjax

  //post_newRestaurant : postNewRestaurant
};

module.exports = routes;
