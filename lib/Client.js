var Consumer = require('Consumer');
var Produver = require('Producer');

var Client = function(){
	options = {host : '10.75.15.235', topic : 'topic', partition : 0, offset : 0};
	var consumer = new Consumer(options);
	consumer.connect(function(err){
	  if (err){
	    console.log("Error: " + err);
	    throw "could not connect to Kafka";
	  }
	  console.log("connected!!");  
	  console.log("consuming: " + consumer.topic);
	  
	  consumer.on('message', function(messages){
	    messages.forEach(function(m){
	      console.log(m.payload.toString());
	    });
	    setTimeout(consumer.consume(),2000);
	  });
	  consumer.on('error', function(error){
	    console.log(error);
	  });

	  consumer.consume();
	    
	});


};


exports.module = Client;