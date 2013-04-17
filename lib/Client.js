var Consumer = require('Consumer');
var Produver = require('Producer');

var Client = function (options) {
	this.options.producer = options.producer || null;

    this.options.consumer = options.consumer || null;

    if(this.options.consumer) {
		this.consumer = new Consumer(options.consumer);   
    };

    if(this.options.producer) {
    	this.producer = new Producer(options.producer);
    };


};

Client.prototype.consume = function(callback) {
	this.consumer.connect(function (err) {
        if (err) {            
            throw "could not connect to Kafka";
        }               

        this.consumer.on('message', function (messages) {
            callback(null, messages);
            setTimeout(this.consumer.consume(), options.consumer.interval);
        });
        
        this.consumer.on('error', function (error) {
        	callback(error, null);
            setTimeout(this.consumer.consume(), options.consumer.interval);
        });

        consumer.consume();

    });
};

Client.prototype.produce = function(messages, callback) {
	
	producer.on('error', function(err){
		callback(err, null);
	});
	
	producer.on('connect')

	producer.send(messages, function(err, res){	  
	  
	});

	producer.connect();
		
};


exports.module = Client;