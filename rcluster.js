
var redis = require('redis');
var events = require('events');
var util = require('util');
var crc16 = require('./crc16');
var safecmds = require('./safecmds');

var RCluster = function(host, port, opts) {

	var _this = this;
	_this.nodes = [];
	_this.nodes_by_addr = new Object(null);
	_this.slots = Array(4096);
	_this.opts = opts;
	var initialNode = _this._setupNode(host, port);
	events.EventEmitter.call(_this);
	
	_this.on('cluster ready', function() {
		for(var i = 0, empty = 0; i < _this.slots.length; i++)
			if(!_this.slots[i])
				empty++;
		if(empty)
			_this.emit('error', 'cluster error: ' + empty + ' slots not allocated');
	});
	
	initialNode.once('ready', function() {
		initialNode.send_command('cluster', [ 'nodes' ], function(err, res) {
			var entries = _this._parseClusterNodesOutput(res);
			for(var i = 0; i < entries.length; i++) {
				var entry = entries[i];
				var node;
				
				if(entry.type === 'myself') {
					node = initialNode;
				}
				else {
					var addr = entry.address.split(':');
					node = _this._setupNode(addr[1], addr[0]);
				}
				
				for(var j = 0; j < entry.slots.length; j++)
					for(var k = entry.slots[j][0]; k <= entry.slots[j][1]; k++)
						_this.slots[k] = node;
			}
		});
	});
};

util.inherits(RCluster, events.EventEmitter);
module.exports.RCluster = RCluster;

RCluster.prototype._reshardingWrapper = function(command, cmd_args, cb) {
	var _this = this;
	return function(err) {
		if(err && (m = /^Error: (MOVED|ASK) (\d+) (.+)$/.exec(err))) {
			
			var redir = m[1];
			var ask = redir === 'ASK';
			var slot = Number(m[2]);
			var addr = m[3];
			var node = _this.nodes_by_addr[addr];
			
			if(!node)
				return cb('received ' + redir + ' to unknown node ' + addr);
			
			if(ask) {
				node.send_command('asking', function(err) {
					if(err)
						return cb('asking command failed after redirection to ' + addr);
					node.send_command.apply(node, [ command, cmd_args ]);
				});
			}
			else {
				// update the slot registry based on the -MOVE info:
				_this.nodes[slot] = node;
				// resend the command to the proper node
				node.send_command.apply(node, [ command, cmd_args ]);
			}
		}
		else {
			return cb.apply(null, Array.prototype.slice.call(arguments));
		}
	};
};

safecmds.forEach(function(command) {
	var cmd = function() {
		var args = Array.prototype.slice.call(arguments);
		var key = args[0];
		if(Array.isArray(key))
			key = key[0];
		var node = this.node(key);
		
		if(this.opts.resharding) {
			var cb = args[args.length - 1];
			if(typeof(cb) === 'function') {
				args[args.length - 1] = this._reshardingWrapper(command, args, cb);
			}
		}
		
		return node.send_command.apply(node, [ command, args ]);
	};
	RCluster.prototype[command] = cmd;
	RCluster.prototype[command.toUpperCase()] = cmd;
});

module.exports.connect = function(host, port, opts) {
	return new RCluster(host, port, opts);
};

RCluster.prototype.node = function(key) {
	return this.slots[crc16(key)];
};

RCluster.prototype.end = function() {
	for(var i = 0; i < this.nodes.length; i++)
		this.nodes[i].end();
};

RCluster.prototype.quit = function() {
	for(var i = 0; i < this.nodes.length; i++)
		this.nodes[i].quit();
};

RCluster.prototype._parseClusterNodesOutput = function(output) {
	var nodes = [];
	var m, re = /^(.+?) (.+?) (.+?) (.+?) (\d+) (\d+) (\w+)( [\d\- ]+)?(.+)?$/gm;

	while(m = re.exec(output)) {
		var node = {
			name: m[1],
			address: m[2],
			type: m[3],
			slaveof: m[4],
			ping: m[5],
			pong: m[6],
			status: m[7],
			slots: m[8] || '',
			migration: m[9]
		};
	
		var m2, re2 = /\s*(\d+)(?:-(\d+))?\s*/g, ss = [];
	
		while(m2 = re2.exec(node.slots)) {
			var n1 = Number(m2[1]);
			var n2 = m2[2] ? Number(m2[2]) : n1;
			ss.push([ n1, n2 ]);
		}
		if(node.slots.trim().length > 0 && ss.length === 0)
			throw new Error('unexpected slot format: ' + node.slots);
		
		node.slots = ss;
		nodes.push(node);
	}
	return nodes;
};

RCluster.prototype._setupNode = function(port, host) {

	var _this = this;
	var node = redis.createClient(port, host, _this.opts);

	node.on('connect', function() {
		_this.emit('connect', node);
	});

	node.on('ready', function() {
		_this.emit('ready', node);
		var ln = _this.nodes.length;
		if(ln > 1) {
			for(var i = 0, r = 0; i < ln; i++)
				if(_this.nodes[i].ready)
					r++;
			if(ln === r)
				_this.emit('cluster ready');
		}
	});

	node.on('error', function(err) {
		_this.emit('error', err, node);
	});

	node.on('end', function() {
		_this.emit('end', node);
	});

	node.on('drain', function() {
		_this.emit('drain', node);
	});

	node.on('idle', function() {
		_this.emit('idle', node);
	});
	
	_this.nodes.push(node);
	_this.nodes_by_addr[host + ':' + port] = node;
	return node;
};
