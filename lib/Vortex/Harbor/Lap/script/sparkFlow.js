// require d3.js
// require jquery.js

var sparkFlow = function(xResolution, gWidth, gHeight, dataUrl, dataProperty, delay, lineWidth) {
	delay = delay || 1000;
	lineWidth = lineWidth || 1.5;
	
	var drawYAxis = function(y) {
	  return d3.svg.axis().scale(y).ticks(5).orient("left");
	}
	
	var fixYAxisCss = function() {
	  $(".axis path, .axis line")
	  .css("fill", "none")
	  .css("shape-rendering", "crispedges")
	  .css("stroke", "#000000")
	  ;
	}
	
	//var random = d3.random.normal(0, .2);
	var chart = function(xDomain, yDomain, interpolation, tick) {
	  var data = d3.range(xResolution).map(function() { return 0 });

	  var margin = {top: 6, right: 0, bottom: 6, left: 35},
		  width = gWidth - margin.right,
		  height = gHeight - margin.top - margin.bottom;

	  var x = d3.scale.linear()
		  .domain(xDomain)
		  .range([0, width]);

	  var y = d3.scale.linear()
		  .domain(yDomain)
		  .range([height, 0]);

	  var line = d3.svg.line()
		  .interpolate(interpolation)
		  .x(function(d, i) { return x(i); })
		  .y(function(d, i) { return y(d); });

	  var svg = d3.select("body").append("p").append("svg")
		  .attr("width", width + margin.left + margin.right)
		  .attr("height", height + margin.top + margin.bottom)
		  //.style("margin-left", margin.left + "px")
		  .style("border", "1px solid #000000")
		  .style("font", "10px sans-serif")
		.append("g")
		  .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

	  svg.append("defs").append("clipPath")
		  .attr("id", "clip")
		.append("rect")
		  .attr("width", width)
		  .attr("height", height);

	  svg.append("g")
		  .attr("class", "y axis")
		  .call(drawYAxis(y))
		  ;
	  fixYAxisCss();

	  var path = svg.append("g")
		  .attr("clip-path", "url(#clip)")
		.append("path")
		  .data([data])
		  .attr("class", "line")
		  .attr("d", line)
		  .style("fill", "none")
		  .style("stroke", "#000000")
		  .style("stroke-width", lineWidth + "px")
		  ;

	  tick(svg, path, line, data, x, y);
	}

	var currentValue = 0;

	var loadData = function() {
	  $.ajax({
		url: dataUrl,
		success: function(data) {
		  currentValue = data[dataProperty];
		  // never poll the server more than every 500 ms
		  setTimeout(loadData, Math.max(delay, 500));
		}
	  });
	};
	loadData(); 

	chart([1, xResolution - 2], [0, 1], "basis", function tick(svg, path, line, data, x, y) {

	  // push a new data point onto the back
	  data.push(currentValue);
	  
	  var dispMax = Math.ceil(d3.max(data)*1.1) || 1;
	  var dist = y.domain()[1]/dispMax;
	  //console.log("curVal:" + currentValue + " dist:" + dist + " dispMax:" + dispMax);
	  if (dist < 1 || dist > 1.1) {
		y.domain([0 , dispMax]);
		//console.log("updated : " + dispMax);
		svg.select(".y.axis").call(drawYAxis(y));
		fixYAxisCss();
	  }
	  
	  
	  // redraw the line, and then slide it to the left
	  path
		  .attr("d", line)
		  .attr("transform", null)
		.transition()
		  .duration(delay)
		  .ease("linear")
		  .attr("transform", "translate(" + x(0) + ")")
		  .each("end", function() { tick(svg, path, line, data, x, y); });

	  // pop the old data point off the front
	  data.shift();

	});
};