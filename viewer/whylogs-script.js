"use strict";

(function () {
  // NOTE: Update file path with you JSON data path
  var JSON_URL = "./example/example-profile.json";

  // HTML Elements
  var $selectedProfile = $(".wl__selected-profile");
  var $featureCount = $(".wl__feature-count");
  var $sidebarFeatureNameList = $(".wl__sidebar-feature-name-list");
  var $featureCountDiscrete = $(".wl__feature-count--discrete");
  var $featureCountNonDiscrete = $(".wl__feature-count--non-discrete");
  var $featureCountUnknown = $(".wl__feature-count--unknown");
  var $tableBody = $(".wl__table-body");
  var $featureSearch = $("#wl__feature-search");
  var $featureFilterInput = $(".wl__feature-filter-input");

  // Constants and variables
  var batchArray = [];
  var featureList = [];
  var inferredFeatureType = {
    discrete: [],
    nonDiscrete: [],
    unknown: [],
  };
  var featureSearchValue = "";
  var isActiveInferredType = {};

  // Util functions
  function debounce(func, wait, immediate) {
    var timeout;
    return function () {
      var context = this,
        args = arguments;
      var later = function () {
        timeout = null;
        if (!immediate) func.apply(context, args);
      };
      var callNow = immediate && !timeout;
      clearTimeout(timeout);
      timeout = setTimeout(later, wait);
      if (callNow) func.apply(context, args);
    };
  }

  function handleSearch() {
    var tableBodyChildrens = $tableBody.children();

    for (var i = 0; i < tableBodyChildrens.length; i++) {
      var name = tableBodyChildrens[i].dataset.featureName.toLowerCase();
      var type = tableBodyChildrens[i].dataset.inferredType.toLowerCase();

      if (isActiveInferredType[type] && name.startsWith(featureSearchValue)) {
        tableBodyChildrens[i].style.display = "";
      } else {
        tableBodyChildrens[i].style.display = "none";
      }
    }
  }

  function fixNumberTo(number, decimals = 3) {
    return parseFloat(number).toFixed(decimals);
  }

  function getQuantileValues(data) {
    return {
      min: fixNumberTo(data[0]),
      firstQuartile: fixNumberTo(data[3]),
      median: fixNumberTo(data[4]),
      thirdQuartile: fixNumberTo(data[5]),
      max: fixNumberTo(data[8]),
    };
  }

  // Override and populate HTML element values
  function updateHtmlElementValues(data) {
    var properties = data.properties;
    var columns = data.columns;
    batchArray = Object.entries(columns);

    var totalCount = "";
    var inferredType = "";
    var nullRatio = "";
    var estUniqueVal = "";
    var dataType = "";
    var dataTypeCount = "";
    var quantiles = {
      min: "",
      firstQuartile: "",
      median: "",
      thirdQuartile: "",
      max: "",
    };
    var mean = "";
    var stddev = "";

    for (var i = 0; i < batchArray.length; i++) {
      var featureName = batchArray[i][0];
      var featureNameValues = batchArray[i][1];
      var frequentItemsElemString = "";
      var chartData = [];

      // Collect all feature names into one array
      featureList.push(featureName);

      if (featureNameValues.numberSummary) {
        // Collect all discrete and non-discrete features
        if (featureNameValues.numberSummary.isDiscrete) {
          inferredType = "Discrete";
          inferredFeatureType.discrete.push(featureNameValues);
        } else {
          inferredType = "Non-discrete";
          inferredFeatureType.nonDiscrete.push(featureNameValues);
        }

        // Update other values
        totalCount = featureNameValues.numberSummary.count;
        quantiles = getQuantileValues(featureNameValues.numberSummary.quantiles.quantileValues);
        mean = fixNumberTo(featureNameValues.numberSummary.mean);
        stddev = fixNumberTo(featureNameValues.numberSummary.stddev);
      } else {
        inferredFeatureType.unknown.push(featureNameValues);
        inferredType = "Unknown";
        totalCount = "-";
        quantiles = {
          min: "-",
          firstQuartile: "-",
          median: "-",
          thirdQuartile: "-",
          max: "-",
        };
        mean = "-";
        stddev = "-";
      }

      // Update other values
      nullRatio = featureNameValues.nullRatio;
      estUniqueVal = featureNameValues.nullRatio;
      dataType = featureNameValues.schema.inferredType.type;
      dataTypeCount = featureNameValues.schema.typeCounts[dataType];

      // Collect frequent items
      if (
        featureNameValues.frequentItems &&
        featureNameValues.frequentItems.items.length &&
        inferredType === "Discrete"
      ) {
        // Chart
        featureNameValues.frequentItems.items.forEach(function (item, index) {
          chartData.push({
            axisY: item.estimate,
            axisX: index,
          });
        });

        // Frequent item chips / bedge
        var frequentItems = featureNameValues.frequentItems.items.slice(0, 5);
        for (var fi = 0; fi < frequentItems.length; fi++) {
          frequentItemsElemString += '<span class="wl-table-cell__bedge">' + frequentItems[fi].jsonValue + "</span>";
        }
      } else {
        if (inferredType === "Non-discrete") {
          // Chart
          if (featureSearchValue.numberSummary) {
            featureNameValues.numberSummary.histogram.counts.slice(0, 30).forEach(function (count, index) {
              chartData.push({
                axisY: count,
                axisX: index,
              });
            });
          }

          // Frequent item chips / bedge
          frequentItemsElemString = "No data shown";
        } else {
          frequentItemsElemString = "-";
          chartData = [];
        }
      }

      // Update sidebar HTML feature name list
      $sidebarFeatureNameList.append(
        '<li class="list-group-item js-list-group-item" data-feature-name-id="' +
          featureName +
          '"><a href="#' +
          featureName +
          '">' +
          featureName +
          "</a></li>",
      );

      function getGraphHtml(data) {
        if (!data.length) return "";

        // var axis = data.reduce(
        //   function (acc, item) {
        //     acc.x.push(parseFloat(item.axisX));
        //     acc.y.push(parseFloat(item.axisY));

        //     return acc;
        //   },
        //   { y: [], x: [] },
        // );

        var MARGIN = {
          TOP: 0,
          RIGHT: 0,
          BOTTOM: 20,
          LEFT: 40,
        };
        var SVG_WIDTH = 250;
        var SVG_HEIGHT = 140;
        var CHART_WIDTH = SVG_WIDTH - MARGIN.LEFT - MARGIN.RIGHT;
        var CHART_HEIGHT = SVG_HEIGHT - MARGIN.TOP - MARGIN.BOTTOM;
        var BORDER_WIDTH = 3;
        var PRIMARY_COLOR_HEX = "#0e7384";
        var SECONDARY_COLOR_HEX = "#ebf2f3";

        var svgEl = d3.create("svg").attr("width", SVG_WIDTH).attr("height", SVG_HEIGHT);

        // SVG FRAME
        svgEl
          .append("rect")
          .attr("x", 0)
          .attr("y", 0)
          .attr("height", SVG_HEIGHT)
          .attr("width", SVG_WIDTH)
          .style("stroke", SECONDARY_COLOR_HEX)
          .style("fill", "none")
          .style("stroke-width", BORDER_WIDTH);
        // CHART FRAME
        svgEl
          .append("rect")
          .attr("x", 0)
          .attr("y", 0)
          .attr("height", CHART_HEIGHT)
          .attr("width", CHART_WIDTH)
          .attr("transform", "translate(" + MARGIN.LEFT + ", 0)")
          .style("stroke", "red")
          .style("fill", "none")
          .style("stroke-width", 3);

        var xScale = d3
          .scaleBand()
          .domain(data.map((d) => d.axisX))
          .range([MARGIN.LEFT, MARGIN.LEFT + CHART_WIDTH]);
        var yScale = d3
          .scaleLinear()
          .domain([0, d3.max(data, (d) => d.axisY)])
          .range([CHART_HEIGHT, 0]);

        // Add the x Axis
        svgEl
          .append("g")
          .attr("transform", "translate(" + 0 + ", " + CHART_HEIGHT + ")")
          .call(d3.axisBottom(xScale));

        // Add the y Axis
        svgEl
          .append("g")
          .attr("transform", "translate(" + MARGIN.LEFT + ", 0)")
          .call(d3.axisLeft(yScale));

        var gChart = svgEl.append("g");
        gChart
          .selectAll(".bar")
          .data(data)
          .enter()
          .append("rect")
          .attr("class", "bar")
          .attr("fill", PRIMARY_COLOR_HEX)
          .attr("x", (d) => xScale(d.axisX))
          .attr("y", CHART_HEIGHT)
          .attr("transform", (d) => `rotate(180, ${xScale(d.axisX) + (xScale.bandwidth() - 1) / 2}, ${CHART_HEIGHT})`)
          .attr("width", xScale.bandwidth() - 1)
          .attr("height", (d) => {
            // NEGATIVE VALUE err
            return Math.abs(yScale(d.axisY));
          });

        return svgEl._groups[0][0].outerHTML;

        // var leftMargin = 40; // Space to the left of first bar; accomodates y-axis labels
        // var rightMargin = 0; // Space to the right of last bar
        // var margin = { left: leftMargin, right: rightMargin, top: 10, bottom: 10 };
        // var barWidth = 20; // Width of the bars
        // var chartHeight = 80; // Height of chart, from x-axis (ie. y=0)
        // var chartWidth = margin.left + newData.length * barWidth + margin.right;

        // /* This scale produces negative output for negatve input */
        // var yScale = d3
        //   .scaleLinear()
        //   .domain([0, d3.max(newData)])
        //   .range([0, chartHeight]);

        // /*
        //  * We need a different scale for drawing the y-axis. It needs
        //  * a reversed range, and a larger domain to accomodate negaive values.
        //  */
        // var yAxisScale = d3
        //   .scaleLinear()
        //   .domain([d3.min(newData), d3.max(newData)])
        //   .range([chartHeight - yScale(d3.min(newData)), 0]);

        // var svg = d3.create("svg");
        // // var svg = d3.select("svg");
        // svg
        //   .attr("height", chartHeight + 100)
        //   .attr("width", chartWidth)
        //   .style("border", "1px solid");

        // svg
        //   .selectAll("rect")
        //   .data(newData)
        //   .enter()
        //   .append("rect")
        //   .attr("x", function (d, i) {
        //     return margin.left + i * barWidth;
        //   })
        //   .attr("y", function (d, i) {
        //     return chartHeight - Math.max(0, yScale(d));
        //   })
        //   .attr("height", function (d) {
        //     return Math.abs(yScale(d));
        //   })
        //   .attr("width", barWidth)
        //   .style("fill", "grey")
        //   .style("stroke", "black")
        //   .style("stroke-width", "1px")
        //   .style("opacity", function (d, i) {
        //     return 1; /*- (i * (1/data.length)); */
        //   });

        // var yAxis = d3.axisLeft(yAxisScale);

        // svg
        //   .append("g")
        //   .attr("transform", function (d) {
        //     return "translate(" + margin.left + ", 0)";
        //   })
        //   .call(yAxis);

        // return svg._groups[0][0].outerHTML;
      }

      // Update data table rows/columns
      // if (chartData.length) {
      $tableBody.append(`
          <li class="wl-table-row" data-feature-name="${featureName}" data-inferred-type="${inferredType}" style="display: none;">
            <div class="wl-table-cell">
              <h4 class="wl-table-cell__title">${featureName}</h4>
              <div class="wl-table-cell__graph-wrap">${getGraphHtml(chartData)}</div>
            </div>
            <div class="wl-table-cell wl-table-cell--top-spacing align-middle" style="max-width: 270px">
              <div class="wl-table-cell__bedge-wrap">
                ${frequentItemsElemString}
              </div>
            </div>
            <div class="wl-table-cell wl-table-cell--top-spacing align-middle">${inferredType}</div>
            <div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">${totalCount}</div>
            <div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">${nullRatio}</div>
            <div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">${estUniqueVal}</div>
            <div class="wl-table-cell wl-table-cell--top-spacing align-middle">${dataType}</div>
            <div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">${dataTypeCount}</div>
            <div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">${mean}</div>
            <div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">${stddev}</div>
            <div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">${quantiles.min}</div>
            <div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">${quantiles.median}</div>
            <div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">${quantiles.max}</div>
          </li>
        `);
      // }
    }

    $featureCountDiscrete.html(inferredFeatureType.discrete.length);
    $featureCountNonDiscrete.html(inferredFeatureType.nonDiscrete.length);
    $featureCountUnknown.html(inferredFeatureType.unknown.length);
    $selectedProfile.html(properties.dataTimestamp);
    $featureCount.html(featureList.length);
  }

  function renderList() {
    var tableBodyChildrens = $tableBody.children();

    $.each($featureFilterInput, function (_, filterInput) {
      isActiveInferredType[filterInput.value.toLowerCase()] = filterInput.checked;
    });

    for (var i = 0; i < tableBodyChildrens.length; i++) {
      var type = tableBodyChildrens[i].dataset.inferredType.toLowerCase();

      if (isActiveInferredType[type]) {
        tableBodyChildrens[i].style.display = "";
      }
    }
  }

  // Load data from JSON file
  $.getJSON(JSON_URL, updateHtmlElementValues).then(function () {
    renderList();
  });

  // Bind event listeners
  $featureSearch.on(
    "input",
    debounce(function (event) {
      featureSearchValue = event.target.value.toLowerCase();
      handleSearch();
    }, 300),
  );
  $featureFilterInput.on("change", function (event) {
    var filterType = event.target.value.toLowerCase();
    var isChecked = event.target.checked;

    isActiveInferredType[filterType] = isChecked;
    handleSearch();
  });
})();
