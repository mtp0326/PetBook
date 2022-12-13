/* Some initialization boilerplate. Also, we include the code from
   routes/routes.js, so we can have access to the routes. Note that
   we get back the object that is defined at the end of routes.js,
   and that we use the fields of that object (e.g., routes.get_main)
   to access the routes. */

   var express = require('express');
   var routes = require('./routes/routes.js');
   var chats = require('./routes/chatRoutes.js');
   var app = express();
   app.use(express.urlencoded());
   
   var http = require('http').Server(app);
   var io = require('socket.io')(http);
   
   var session = require('express-session');
   app.use(session({
      secret: 'loginSecret',
      resave: false,
      saveUnitialized: true,
      cookie: { secure: false }
   }));
   
   
   
   io.on("connection", function (socket) {
      console.log("connected!");
      socket.on("chat message", function (obj) {
         console.log("chat message");
         console.log(obj);
         if (obj.room == undefined) {
            console.log("null room");
         }
         else {
            console.log(obj.room);
            
            io.sockets.in(obj.room).emit("chat message", obj);
            console.log("emitted");
         }
   
   
      });
   
      socket.on("join room", function (obj) {
         console.log("join");
         console.log(obj.room);
         socket.join(obj.room);
      });
   
      socket.on("leave room", function (obj) {
         console.log("leave");
         console.log(obj.room)
         socket.leave(obj.room);
      });
   });
   
   
   /* Below we install the routes. The first argument is the URL that we
      are routing, and the second argument is the handler function that
      should be invoked when someone opens that URL. Note the difference
      between app.get and app.post; normal web requests are GETs, but
      POST is often used when submitting web forms ('method="post"'). */
   
   
   
   
      app.get('/', routes.get_main);
      app.get('/restaurants', routes.get_restaurants);
      app.get('/signup', routes.get_signup);
      app.get('/logout', routes.get_logout);
      app.get('/chat', routes.get_chat);
      app.get('/wall', routes.get_wall);
      app.get('/edit', routes.get_edit);
      app.get('/getList', routes.get_restaurantList);
      app.get('/getCreator', routes.get_creator);
      
      //NEW
      app.get('/homepage', routes.get_homepage);
      app.get('/getPostAjax', routes.get_homepagePostListAjax);
      app.get('/getWallAjax', routes.get_wallListAjax);
      app.get('/getEditUserInfoAjax', routes.get_editUserInfoAjax);
      app.get('/getAllUsername', routes.get_allUsername);
      
      app.post('/createpost', routes.post_newPostAjax);
      app.post('/createcomment', routes.post_newCommentAjax);
      app.post('/createwall', routes.post_newWallAjax);
      app.post('/postUpdateUser', routes.post_updateUser);
      
      app.post('/checklogin', routes.verifyUser);
      
      app.post('/createaccount', routes.post_newAccount);
      app.post('/addList', routes.post_newRestaurantAjax);
      app.post('/deleteList', routes.post_deleteRestaurantAjax);
      app.post('/editaccount', routes.post_updateUser);
      app.post('/postOtherWallPageAjax', routes.post_otherWallPageAjax);
      app.get('/getDetermineWallOwner', routes.get_determineWallOwner),
      
      //chat
      app.get('/getonlineusers', chats.get_online_users);
      app.get('/getchat', chats.get_chat);
      app.post('/addchatroom', chats.add_chatroom);
      app.post('/addonlineuser', chats.add_online_user);
      app.post('/addmessage', chats.add_message);
      app.get('/getchatrooms', chats.get_chatrooms);
      app.post('/logoutchat', chats.log_out);
      
      //visualizer
      app.get('/visualizer', routes.get_friend_visualizer);
      
      /* Run the server */
      
      console.log('Author: Jiwoong Matt Park (mtp0326)');
      // app.listen(8080);
      http.listen(8080, () => {
         console.log('listening on 8080');
      });
      console.log('HTTP server started on port 8080');