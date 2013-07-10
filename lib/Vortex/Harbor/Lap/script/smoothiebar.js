
/*
 * Smoothie Bar
 * depends on d3.js, nothing else.

 * INA-DLWeb 2013
 *
 */

;SmoothieBar = (function() {

//var debug_id = "box6";

SmoothieBar.getFormat = function(suffix) {
	var f = d3.format(".3s");
	/*return function(value) {
		return f.call(undefined, value) + suffix;
	};*/
	return function(d) {
		var prefix = d3.formatPrefix(d);
		var s = prefix.symbol;
		return f.call(undefined, prefix.scale(d)) + (suffix == "" && s == "" ? "" : " ") + s + suffix;
	};
};

SmoothieBar.clone = function(obj) {
	var clone = {};
	for (var key in obj) {
		if(typeof(obj[key])=="object") {
			clone[key] = SmoothieBar.clone(obj[key]);
		} else {
			clone[key] = obj[key];
		}
	}
	return clone;
};

SmoothieBar.merge = function(org, o) {
	var n = {};
	for (var key in org) {
		if (o[key] === undefined) {
			n[key] = org[key];
		} else if (typeof(org[key]) == "object") {
			n[key] = SmoothieBar.merge(org[key], o[key]);
		} else {
			n[key] = o[key];
		}
	}
	return n;
};

var DefaultParams = {
	// 10 bars are displayed
	wantedItems: 10,

	// the total height of the svg element
	totalHeight: 200,
	
	// the paddings around the chart 
	padding: {top: 15, right: 10, bottom: 17, left: 35},
	
	// the total width of the svg element (ratio of the containing element)
	totalWidth: 1.0,
	
	// bar/column width ratio
	barColumnRatio: 0.8,
	
	// maximum value of the stacked items (values above will be ouside the chart)
	stackScaleMax: undefined,
	
	// minimum value of the stacked scale (max axis value is never bellow this)
	stackScaleMin: 0,
	
	// frame duration / refresh period (every 1000 milliseconds)
	frameDuration: 1000,
	
	// transition duration
	transitionDuration: 1000,
	
	// tranbsition type
	transitionEase: "linear",
	
	// color range
	colorRange: ["#051", "#0f3"],
	
	// axis lines & text colors
	axisColor: "#0f3",
	axisFontSize: 12,

	// displayed time delay in milliseconds
	displayDelay: 0,

	// legend line/box height
	legendLineHeight: 11,

	// time format function
	timeFormat: function(d) {
		return (d.getSeconds() === 0)
			? d3.time.format("%H:%M").call(undefined, d)
			: (d.getSeconds() + '"')
			;
	},

	// y axis format
	yAxisFormat: SmoothieBar.getFormat(""),
	yAxisRight: false,
	
	// datasets loading function (default is random data)
	dataLoader: function(wantedItems, callback) {
		var datasetNames = ["ds1", "ds2", 'ds3'];
		var ds = {};
		for (var i=0; i<datasetNames.length; ++i) {
			ds[datasetNames[i]] = this.randomDataset(Math.ceil(0.8 * wantedItems), 2);
		}
		callback.call(undefined, ds);
	},
};	

SmoothieBar.usedDefaultParams = SmoothieBar.clone(SmoothieBar.DefaultParams);

SmoothieBar.setDefaultParams = function(params) {
	SmoothieBar.usedDefaultParams = SmoothieBar.merge(DefaultParams, params);
};

SmoothieBar.resetDefaultParams = function() {
	SmoothieBar.usedDefaultParams = SmoothieBar.clone(DefaultParams);
};

function SmoothieBar(targetId, params) {
	this.targetId = targetId;

	// merge constructor params with default params
	this.params = SmoothieBar.merge(SmoothieBar.usedDefaultParams, params);
};


SmoothieBar.prototype.init = function() {
	/* when a value is added to the chart, it represents the value for 
	 * the time unit that has just elapsed.
	 * we shorten this delay by an additional 2 seconds, because it seems to be more accurate :/
	 */
	this.params.displayDelay = this.params.displayDelay + this.params.frameDuration - 2000;

	// make sur the transition is shorted than the frame
	this.params.transitionDuration = Math.min(this.params.frameDuration, this.params.transitionDuration);

	// find target element, error
	var targetElement = document.getElementById(this.targetId);
	if (!targetElement) {
		throw new Error("element with id '" + this.targetId + "' was not found");
	}

	// compute width & height of the chart
	this.chartWidth = 
		targetElement.clientWidth * this.params.totalWidth
		- this.params.padding.left
		- this.params.padding.right;

	this.chartHeight = this.params.totalHeight
		- this.params.padding.top
		- this.params.padding.bottom;

	// compute columns & bar width
	this.columnWidth = this.chartWidth / this.params.wantedItems;
	this.barWidth = this.params.barColumnRatio * this.columnWidth;

	// initialize X and Y axis (with ranges)
	this.x = d3.time.scale().range([0, this.chartWidth]);
	this.y = d3.scale.linear().range([this.chartHeight, 0]);

	// initialize datasets
	this.datasets = {};
	this.currentDataKey = 0;
	this.means = {};
	this.mins = {};
	this.maxs = {};
}

SmoothieBar.prototype.nextDataKey = function() {
	return ++(this.currentDataKey);
};

SmoothieBar.prototype.randomDataset = function(items, o) {
	var sb = this;
	if (o === undefined) o = 3;
	function bump(a) {
		var x = 1 / (.1 + Math.random()),
			y = 2 * Math.random() - .5,
			z = 10 / (.1 + Math.random());
		for (var i = 0; i < items; i++) {
			var w = (i / items - y) * z;
			a[i] += x * Math.exp(-w * w);
			a[i] = Math.round(a[i]);
		}
	}
	var a = [], i;
	for (i = 0; i < items; i++) a[i] = o + o * Math.random();
	for (i = 0; i < 5; i++) bump(a);
	return a.map(function (d, i) {
		return Math.max(0, d);
	});
};

SmoothieBar.prototype.log = function(s) {
	/*if (this.targetId == debug_id) {
		console.log(s);
	}*/
};

SmoothieBar.prototype.loadData = function(itemCount, callback) {
	this.params.dataLoader.call(this, itemCount, callback);
};

SmoothieBar.prototype.mergeNewData = function(newDatasets, now) {
	now = Math.floor(now/1000)*1000;
	var stackHeights = [], d = this.params.frameDuration, 
		delay = this.params.displayDelay, v;
	for (var datasetName in newDatasets) {
		var newDataset = newDatasets[datasetName];

		var currentDataset = this.datasets[datasetName];
		if (currentDataset === undefined) {
			currentDataset = [];
			this.datasets[datasetName] = currentDataset;
		}

		for (var x=0, xl=newDataset.length; x<xl; ++x) {
			// initialize y0 for the first stack 
			if (stackHeights[x] === undefined) stackHeights[x] = 0;
			
			// merge the data
			v = {
				value: Math.max(0, newDataset[x]),
				key:   this.nextDataKey(),
				y0:	stackHeights[x],
				x: now - (xl - x)*d - delay
			};
			//this.log("value:" + JSON.stringify(v));
			currentDataset.push(v);
			stackHeights[x] += newDataset[x];
		}

		// remove values if dataset is tool long
		while (currentDataset.length > this.params.wantedItems) {
			currentDataset.shift();
		}

		// recompute average/min/max
		if (currentDataset.length > 0) {
			this.means[datasetName] = d3.mean(currentDataset, function(d) { return d.value });
			this.mins[datasetName] = d3.min(currentDataset, function(d) { return d.value });
			this.maxs[datasetName] = d3.max(currentDataset, function(d) { return d.value });
		} else {
			this.means[datasetName] = 0;
			this.mins[datasetName] = 0;
			this.maxs[datasetName] = 0;
		}
		
		// padd dataset with 0 values if incomplete
		while (currentDataset.length < this.params.wantedItems) {
			currentDataset.unshift({
				value: 0,
				key:   this.nextDataKey(),
				y0:	0,
				x: 0
			});
		}
	}

	// add the current max stack height and the min stack height to the list
	if (this.maxStack !== undefined) stackHeights.push(this.maxStack);
	stackHeights.push(this.params.stackScaleMin);

	// compute the max height of a stack
	this.maxStack = this.params.stackScaleMax !== undefined && this.params.stackScaleMax > 0
		? this.params.stackScaleMax 
		: d3.max(stackHeights);
	
	// update X & Y axis ranges
	var ds = now - this.params.wantedItems*this.params.frameDuration - this.params.displayDelay;
	var de = now - this.params.displayDelay;
	this.x.domain([new Date(ds), new Date(de)]);
	this.y.domain([0, this.maxStack]);
};

SmoothieBar.prototype.drawInitialChart = function(now) {
	var sb = this;
	var axisCorrectY = 0;

	// chart
	sb.chart = d3.select("#" + sb.targetId).append("svg")
		.attr("class", "chart")
		.attr("width", function() { 
			return sb.chartWidth + sb.params.padding.left + sb.params.padding.right 
		})
		.attr("height", sb.chartHeight + sb.params.padding.top + sb.params.padding.bottom)
		.append("g")
		.attr("transform", "translate(" + sb.params.padding.left + "," + sb.params.padding.top + ")")
	;

	// X axis
	var xShift = 2;
	sb.x.axis = d3.svg.axis()
		.scale(sb.x)
		.tickSize(4)
		.tickPadding(2)
		.orient("bottom")
		.tickFormat(sb.params.timeFormat)
	;
	sb.xAxis = sb.chart.append("g")
		.attr("class", "x axis")
		.attr("transform", "translate(" + (sb.params.yAxisRight ? 1 : -1)*xShift + "," + (sb.chartHeight+axisCorrectY) + ")")
		.style("opacity", 1.0)
		.call(sb.x.axis)
	;

	// Y axis
	sb.y.axis = d3.svg.axis()
		.scale(sb.y)
		.ticks(Math.max(Math.floor(sb.chartHeight / (sb.params.axisFontSize * 1.25)), 1))
		.tickSize(4)
		.tickPadding(2)
		.orient(sb.params.yAxisRight ? "right" : "left")
		.tickFormat(sb.params.yAxisFormat)
	;
	sb.yAxis = sb.chart.append("g")
		.attr("class", "y axis")
		.attr("transform", "translate(" 
			+ (sb.params.yAxisRight ? sb.chartWidth + xShift : -1*xShift)
			+ "," + axisCorrectY + ")"
		)
		.call(sb.y.axis)
	;

	var bars = [], i = 0, delay = sb.params.displayDelay, 
		period = sb.params.frameDuration,
		datasetLength = sb.params.wantedItems;

	// datasets
	for (var datasetName in sb.datasets) {
		var chartClassName = "ds_chart_" + i;
		var legendClassName = "ds_legend_" + i;

		// legend
		var llh = sb.params.legendLineHeight;
		var g = sb.chart
			.append("g")
			.attr("transform", "translate(" + 
				(sb.chartWidth - llh)
				+ "," + -1 * Math.ceil(1.3*llh*(i + 1.3)) +
				// sb.params.padding.top - llh*(0.6 + i*1.3)
			")")
			;
		g.append("rect")
			.attr("x", 0).attr("y", 0)
			.attr("width", llh).attr("height", llh)
			.style("fill", sb.color(i))
			;
		g.append("text")
			.attr("class", "legendText " + legendClassName)
			.attr("x", -0.5 * llh).attr("y", llh)
			.text(sb.legend(datasetName))
			.attr("text-anchor", "end")
			.style("fill", sb.params.axisColor)
			.style("font-size", llh + "px")
			;

		// bars
		sb.chart.selectAll("." + chartClassName)
			.data(
				function() { return sb.datasets[datasetName] },
				function(d) { return d.key }
			)
			.enter()
			.insert("rect", ":first-child")
			//.append("rect")
			.style("fill", sb.color(i))
			.style("opacity", 1.0)
			.attr("class", chartClassName)
			.attr("x", function(d, x) { 
				return sb.x(d.x);
				//return sb.x(now - (datasetLength - x)*period - delay) //+ 3
			})
			.attr("y", function(d) { return sb.y(d.y0 + d.value); })
			.attr("height", function(d) { return sb.chartHeight - sb.y(d.value); })
			.attr("width", sb.barWidth)
		;

		i++;
	}

	sb.chart
		.selectAll(".axis")
		.style("fill", sb.params.axisColor)
		.style("font-size", sb.params.axisFontSize + "px")
	;

	sb.chart
		.selectAll(".axis path, .axis line")
		.style("fill", "none")
		.style("shape-rendering", "crispEdges")
		.style("stroke", sb.params.axisColor)
	;
};

SmoothieBar.prototype.legend = function(datasetName) {
	return datasetName + " (" 
		+ this.params.yAxisFormat.call(undefined, this.mins[datasetName]) + " / " 
		+ this.params.yAxisFormat.call(undefined, this.means[datasetName]) + " / " 
		+ this.params.yAxisFormat.call(undefined, this.maxs[datasetName]) + ")"
	;
};

SmoothieBar.prototype.stop = function() {
	this.wantStop = true;
};

SmoothieBar.prototype.start = function() {
	var sb = this;
	sb.wantStop = false;

	// initialize
	sb.init();
	
	var doStart = function() {
		// load initial data
		sb.loadData(sb.params.wantedItems, function(newDatasets) {

			sb.datasetCount = 0;
			for (var k in newDatasets) ++(sb.datasetCount);

			// create color scale
			sb.color = d3.scale.linear()
				.domain([0, sb.datasetCount - 1])
				.range(sb.params.colorRange);

			var now = Date.now();
			var rnow = Math.floor(now/1000)*1000;
			
			// merge data and compute average/max/min
			sb.mergeNewData(newDatasets, rnow);

			// draw chart
			sb.drawInitialChart(rnow);

			// Compute how long to wait to start loop:
			// - should be well aligned with frame duration 
			// - should be as close to 500ms as possible (to avoid second shift)
			now = Date.now();
			var ut = sb.params.frameDuration - (now % sb.params.frameDuration) + 500;
			/*if (sb.targetId == debug_id) {
				var n = new Date(now);
				sb.log("NOW:" + n.getSeconds() + "s " + n.getMilliseconds() + "ms__FRAME:" + sb.params.frameDuration + "__UT:" + ut);
			}*/
			setTimeout(function() { 
				sb.startTime = Date.now();
				sb.log("start-time:" + new Date(sb.startTime).getSeconds() + "s " + new Date(sb.startTime).getMilliseconds() + "ms");
				sb.loop(); 
			}, ut);
		});
	};
	
	var delay = sb.durationToMs(500);
	setTimeout(doStart, delay);
};

SmoothieBar.prototype.durationToMs = function(offset) {
	if (offset == undefined) offset = 0;
	var d = new Date();
	var ms = d.getMilliseconds();
	var t = offset - d.getMilliseconds();
	if (t < 0) t += 1000;
	return t;
};

SmoothieBar.prototype.loop = function() {
	var sb = this;

	if (sb.wantStop) return;
	
	sb.loadData(1, function(newDatasets) {
		var now = Date.now();
		
		var rnow = Math.floor(now/1000)*1000;
		var n = new Date(now);
		// var nowRound = Math.floor(now/1000)*1000;
		var lateness = (now - sb.startTime) % sb.params.frameDuration;
		sb.log(n.getSeconds() + "s " + n.getMilliseconds() + "ms__" + lateness);
		
		// merge data and compute average/max/min
		sb.mergeNewData(newDatasets, rnow);

		// update chart (loops)
		sb.updateChart(rnow, lateness);
	});
};

SmoothieBar.prototype.updateChart = function(now, lateness) {
	var sb = this;

	var smooth = sb.params.transitionDuration,
		ease = sb.params.transitionEase,
		period = sb.params.frameDuration, 
		delay = sb.params.displayDelay;
	var frame = period - Math.max(lateness, 0);

	// refresh X axis
	sb.xAxis
		.transition()
		.duration(smooth)
		.ease(ease)
		.call(sb.x.axis);

	// refresh chart
	var i = 0;
	for (var datasetName in sb.datasets) {
		var chartClassName = "ds_chart_" + i;
		var legendClassName = "ds_legend_" + i;

		var legend = sb.chart.selectAll("." + legendClassName)
			.transition().duration(smooth).ease(ease)
			.text(sb.legend(datasetName))
		;

		var rect = sb.chart.selectAll("." + chartClassName)
			.data(
				function() { return sb.datasets[datasetName] },
				function(d) { return d.key; }
			)
		;

		rect
			.enter()
			.insert("rect", ":first-child")
			.style("fill", sb.color(i))
			.style("opacity", 0.05)
			.attr("class", chartClassName)
			.attr("x", function(d, x) { 
				return sb.x(d.x) + sb.columnWidth;
				//return sb.x(now - (sb.params.wantedItems - x)*period - delay) + sb.columnWidth
			})
			.attr("y", function(d) { return sb.y(d.y0 + d.value); })
			.attr("height", function(d) { return sb.chartHeight - sb.y(d.value); })
			.attr("width", 0.1)
			;

		rect
			.transition().duration(smooth).ease(ease)
			.style("opacity", 1.0)
			//.style("fill", sb.color(i))
			.attr("y", function(d, x) { return sb.y(d.y0 + d.value); })
			.attr("height", function(d) { return sb.chartHeight - sb.y(d.value); })
			.attr("x", function(d, x) { 
				return sb.x(d.x);
				//return sb.x(now - (sb.params.wantedItems - x)*period - delay)
			})
			.attr("width", sb.barWidth)
		;

		rect
			.exit()
			.transition().duration(smooth).ease(ease)
			//.attr("fill", "red")
			.attr("x", function(d, x) { 
				return sb.x(d.x) + sb.columnWidth;
				//return sb.x(now - sb.params.wantedItems*period - delay) - (sb.columnWidth - sb.barWidth)
			})
			.attr("width", 0.1)
			.style("opacity", 0.05)
			.remove()
		;

		i++;
	}
	
	// if smooth duration is significantly shorter than frame duration
	if (smooth < frame*1.3) {
		setTimeout(function(d) { sb.loop() }, frame);
		sb.yAxis
			.transition().duration(smooth).ease(ease)
			.call(sb.y.axis)
		;
	} else {
		sb.yAxis
			.transition().duration(frame).ease(ease)
			.each("end", function(d) { sb.loop() })
			.call(sb.y.axis)
		;
	}
	
	sb.chart
		.selectAll(".axis line")
		.style("fill", "none")
		.style("shape-rendering", "crispEdges")
		.style("stroke", sb.params.axisColor)
	;
};

return SmoothieBar;
})();