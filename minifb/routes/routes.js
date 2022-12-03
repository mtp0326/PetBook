var db = require('../models/database.js');

var isVerified = false;
// TODO The code for your own routes should go here
var getMain = function(req, res) {
  isVerified = false;
  res.render('main.ejs');
};

var getWall = function(req, res) {
  isVerified = false;
  res.render('wall.ejs', {"check" : true});
};

//for results if the username and password are correct
var postResultsUser = function(req, res) {
  var usernameCheck = req.body.username;
  var passwordCheck = req.body.password;
  db.passwordLookup(usernameCheck, function(err, data) {
    if (data == passwordCheck && !err) {
      req.session.username = req.body.username;
      req.session.password = req.body.password;
      isVerified = true;
      res.render('checklogin.ejs', {"check" : true});
    } else {
      isVerified = false;
      res.render('checklogin.ejs', {"check" : false});
    }
  });
};

//gets signup page
var getSignup = function(req, res) {
	res.render('signup.ejs', {"check" : isVerified});
}

//gets logout page
var getLogout = function(req, res) {
	req.session.username = null;
  req.session.destroy();
	res.render('logout.ejs', {});
}

var getChat = function(req, res) {
	res.render('chat.ejs', {"check" : isVerified})
};

//check if new account can be created by receiving null (which means that username in db is empty)
//and create the new account and go to restaurants or fail and go back to signup.
var postNewAccount = function(req, res) {
  var usernameNewCheck = req.body.username;
  db.usernameLookup(usernameNewCheck, "username", function(err, data) {
	if(data == null || err) {
    req.session.username = req.body.username;
    req.session.password = req.body.password;
    req.session.fullname = req.body.firstname + " " + req.body.lastname;
    req.session.affiliation = req.body.affiliation;
    req.session.email = req.body.email;
    req.session.birthday = req.body.birthday;
    
    var interestList = (req.body.interest.toLowerCase()).split(", ");
    req.session.interest = interestList;
    req.session.pfpURL = req.body.pfpURL;
		db.createAccount(req.session.username, req.session.password, req.session.fullname, req.session.affiliation, req.session.email, req.session.birthday,
      req.session.interest, req.session.pfpURL, function(err, data){});
		isVerified = true;
    res.render('createaccount.ejs', {"check" : true});
	} else {
		isVerified = false;
    res.render('createaccount.ejs', {"check" : false});
	}
    
  });
};

//render homepage
//NEW: getHomepage, homepage.ejs
var getHomepage = function(req, res) {
	res.render('homepage.ejs', {"check" : isVerified})
};

//ajax: query posts of friend's userid
//Also renders comments if exists
//NEW: getHomepagePostList, getAllPosts
var getHomepagePostListAjax = function(req, res) {
  
  db.getFriends(req.session.username, function(err, data){
    var friendsList = data.map(obj => obj.S);
    console.log("friend: " + friendsList);

    var tempList = [];
    db.getAllPosts(req.session.username, function(err, data){
      var contentArr = data.map(obj => obj.content.S);
      var commentsArr = data.map(obj => obj.comments.L);
      var likesArr = data.map(obj => obj.likes.L);
      var userIDArr = data.map(obj => obj.userID.S);
      var timestampArr = data.map(obj => obj.timestamp.S);
      
      for(let i = 0; i < userIDArr.length; i++) {
        var pointer =  {
          "content": contentArr[i],
          "comments": commentsArr[i],
          "likes": likesArr[i],
          "userID" : userIDArr[i],
          "timestamp" : timestampArr[i]
        };
        tempList.push(pointer);
      }
      recGetAllPosts(friendsList, tempList, 0, function(postsList) {
        postsList.sort((a, b) => (a.timestamp).localeCompare(b.timestamp)).reverse();
        console.log(postsList)
        res.send(JSON.stringify(postsList));
      });
    });
  });
};

var recGetAllPosts = function(recFriendsList, recPostsList, counter, callback) {
  if (counter >= recFriendsList.length) {
    console.log("recFriendsList: " + recPostsList);
    callback(recPostsList);
  } else {
    db.getAllPosts(recFriendsList[counter], function(err, data){
      var contentArr = data.map(obj => obj.content.S);
      var commentsArr = data.map(obj => obj.comments.L);
      var likesArr = data.map(obj => obj.likes.L);
      var userIDArr = data.map(obj => obj.userID.S);
      var timestampArr = data.map(obj => obj.timestamp.S);
      
      console.log(contentArr);
      for(let i = 0; i < userIDArr.length; i++) {
        var pointer =  {
          "content": contentArr[i],
          "comments": commentsArr[i],
          "likes": likesArr[i],
          "userID" : userIDArr[i],
          "timestamp" : timestampArr[i]
        };
        recPostsList.push(pointer);
      }
      counter++;
      recGetAllPosts(recFriendsList, recPostsList, counter, callback);
    });
  }
}

//ajax: get the creator information
var getCreator = function(req, res) {
  res.send(JSON.stringify(req.session.username));
};

//create new post in the db when all inputs exist in posts
var postNewPostAjax = function(req, res) {
  var content = req.body.content;
  var timestamp = req.body.timestamp;
  if(content.length != 0 && timestamp.length != 0) {
	  db.createPost(req.session.username, content, timestamp, function(err, data){});
    
    var response = {
      "userID": req.session.username,
      "content" : content,
      "timestamp": timestamp
    };

    res.send(response);
  } else {
	  res.send(null);
  }
};

//ajax: add comment in post data in posts
var postNewCommentAjax = function(req, res) {
  var userID = req.body.userID;
  var timestamp = req.body.timestamp;
  var comment = req.body.comment;
  console.log(userID);
  console.log(timestamp);
  console.log(comment);

  if(userID.length != 0 && timestamp.length != 0 && comment.length != 0) {
    db.addComment(userID, timestamp, comment, function(err,data){});
    
    var response = {
      "userID": userID,
      "timestamp": timestamp,
      "comment" : comment
    };
    console.log(response);

    res.send(response);
  } else {
    res.send(null);
  }
};
//***************************************************** */

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
  get_chat: getChat,
  get_wall: getWall,
  //NEW
  get_homepage : getHomepage,
  get_homepagePostListAjax : getHomepagePostListAjax,

  post_newPostAjax : postNewPostAjax,
  post_newCommentAjax : postNewCommentAjax,

  post_newAccount : postNewAccount,
  post_newRestaurantAjax : postNewRestaurantAjax,
  post_deleteRestaurantAjax : postDeleteRestaurantAjax

  //post_newRestaurant : postNewRestaurant
};

module.exports = routes;