var db = require('../models/database.js');

// TODO The code for your own routes should go here

var getMain = function(req, res) {
  res.render('main.ejs', {});
};

var postResults = function(req, res) {
  var userInput = req.body.myInputField;
  db.lookup(userInput, "german", function(err, data) {
    if (err) {
      res.render('results.ejs', {theInput: userInput, message: err, result: null});
    } else if (data) {
      res.render('results.ejs', {theInput: userInput, message: null, result: data});
    } else {
      res.render('results.ejs', {theInput: userInput, result: null, message: 'We did not find anything'});
    }
  });
};

// TODO Don't forget to add any new functions to this class, so app.js can call them. (The name before the colon is the name you'd use for the function in app.js; the name after the colon is the name the method has here, in this file.)

var routes = { 
  get_main: getMain,
  post_results: postResults
};

module.exports = routes;
