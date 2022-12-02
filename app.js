/* Some initialization boilerplate. Also, we include the code from
   routes/routes.js, so we can have access to the routes. Note that
   we get back the object that is defined at the end of routes.js,
   and that we use the fields of that object (e.g., routes.get_main)
   to access the routes. */

var express = require('express');
var routes = require('./routes/routes.js');
var app = express();
app.use(express.urlencoded());

var session = require('express-session');
app.use(session({
   secret: 'loginSecret',
   resave : false,
   saveUnitialized: true,
   cookie: { secure: false }
}));

/* Below we install the routes. The first argument is the URL that we
   are routing, and the second argument is the handler function that
   should be invoked when someone opens that URL. Note the difference
   between app.get and app.post; normal web requests are GETs, but
   POST is often used when submitting web forms ('method="post"'). */

app.get('/', routes.get_main);
app.get('/restaurants', routes.get_restaurants);
app.get('/signup', routes.get_signup);
app.get('/logout', routes.get_logout);
app.get('/getList', routes.get_restaurantList);
app.get('/getCreator', routes.get_creator);

//NEW
app.get('/homepage', routes.get_homepage);
app.get('/getPostAjax', routes.get_homepagePostListAjax);

app.post('/createpost', routes.post_newPostAjax);
app.post('/createcomment', routes.post_newCommentAjax);

app.post('/checklogin', routes.verifyUser);
app.post('/createaccount', routes.post_newAccount);
app.post('/addList', routes.post_newRestaurantAjax);
app.post('/deleteList', routes.post_deleteRestaurantAjax);



// TODO You will need to replace these routes with the ones specified in the handout

/* Run the server */

console.log('Author: Jiwoong Matt Park (mtp0326)');
app.listen(8080);
console.log('HTTP server started on port 8080');
