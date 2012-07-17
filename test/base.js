
var db = require('../rcluster.js').connect(9001, '127.0.0.1', {
	detect_buffers: true,
	resharding: true
});

db.on('error', function(err, node) {
	console.log('error:');
	console.log(arguments);
});

db.on('cluster ready', function() {
	console.log('cluster is ready: ' + db.nodes.length + ' nodes.');
	
	//db.node('abc').get('abc', function(err, dt) {
	db.get('abc', function(err, dt) {
		console.log(arguments);
		
		db.quit();
	});
});
