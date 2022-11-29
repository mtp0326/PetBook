/* Some initialization boilerplate. Also, we include the code from
   routes/routes.js, so we can have access to the routes. Note that
   we get back the object that is defined at the end of routes.js,
   and that we use the fields of that object (e.g., routes.get_main)
   to access the routes. */

var express = require('express');
var routes = require('./routes/routes.js');
var app = express();
app.use(express.urlencoded());


/* Below we install the routes. The first argument is the URL that we
   are routing, and the second argument is the handler function that
   should be invoked when someone opens that URL. Note the difference
   between app.get and app.post; normal web requests are GETs, but
   POST is often used when submitting web forms ('method="post"'). */

var session1 = require('express-session');
app.use(session1({secret: "secret"}));

app.get('/', routes.get_main);
app.get('/signup', routes.signup);
app.post('/checklogin', routes.checklogin);
app.post('/createaccount', routes.createaccount);
app.get('/restaurants', routes.restaurants);
app.get('/getCreator', routes.getUser);
app.post('/deleteitem', routes.deleteres);
app.get('/getList', routes.getrestaurants, function(req, res){
   console.log("got getlist")
});
app.post('/createrestaurant', routes.createrestaurant, function(req, res){
   console.log("got createrestaurant")
});
// app.post('/addrestaurant', routes.addrestaurant);
app.get('/logout', routes.logout);

// TODO You will need to replace these routes with the ones specified in the handout

/* Run the server */

console.log('Author: Cindy (weicindy)');
app.listen(8080);
console.log('HTTP server started on port 8080');
