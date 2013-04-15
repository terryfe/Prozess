var Consumer = require('./lib/Consumer');


options = {host : '10.75.15.234', topic : 'topic', partition : 0, offset : 0};
var consumer = new Consumer(options);
consumer.connect(function(err){
  if (err){
    console.log("Error: " + err);
    throw "could not connect to Kafka";
  }
  console.log("connected!!");

  console.log("===================================================================");
  console.log(new Date());
  console.log("consuming: " + consumer.topic);
  consumer.on('message', function(messages){
    messages.forEach(function(m){
      console.log(m);
    });
    setTimeout(consumer.consume(),2000);
  });
  consumer.consume();
    
});


