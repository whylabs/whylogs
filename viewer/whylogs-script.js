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
  var $jsonForm = $("#json-form");
  var $fileInput = $("#file-input");
  var $tableContent = $("#table-content");
  var $tableMessage = $("#table-message");
  var $sidebarContent = $("#sidebar-content");

  // Constants and variables
  var batchArray = [];
  var featureSearchValue = "";
  var isActiveInferredType = {};
  var frequentItems = [];

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
      firstQuantile: fixNumberTo(data[3]),
      median: fixNumberTo(data[4]),
      thirdQuantile: fixNumberTo(data[5]),
      max: fixNumberTo(data[8]),
    };
  }

  function formatLabelDate(timestamp) {
    const date = new Date(timestamp);
    const format = d3.timeFormat("%Y-%m-%d %I:%M:%S %p %Z");
    return format(date);
  }

  function scrollToFeatureName(event) {
    var TABLE_HEADER_OFFSET = 90;
    var $tableWrap = $(".wl-table-wrap");
    var featureNameId = event.currentTarget.dataset.featureNameId;
    var currentOffsetTop = $tableWrap.scrollTop();
    var offsetTop = $('[data-feature-name="' + featureNameId + '"]').offset().top;

    // $tableWrap.scrollTop(currentOffsetTop + offsetTop - TABLE_HEADER_OFFSET);
    $tableWrap.animate(
      {
        scrollTop: currentOffsetTop + offsetTop - TABLE_HEADER_OFFSET,
      },
      500,
    );
  }

  function openPropertyPanel(chips) {
    if (chips.length && chips !== "undefined") {
      var chipString = "";

      chips.forEach((item) => {
        chipString += '<span class="wl-table-cell__bedge">' + item + "</span>";
      });
      $(".wl-property-panel__frequent-items").html(chipString);
      $(".wl-property-panel").addClass("wl-property-panel--open");
      $(".wl-table-wrap").addClass("wl-table-wrap--narrow");
    }
  }

  function handleClosePropertyPanel() {
    $(".wl-property-panel").removeClass("wl-property-panel--open");
    $(".wl-table-wrap").removeClass("wl-table-wrap--narrow");
    $(".wl-property-panel__frequent-items").html("");
  }

  // Override and populate HTML element values
  function updateHtmlElementValues(data) {
    var properties = data.properties;
    var columns = data.columns;
    batchArray = Object.entries(columns);
    $tableBody.html("");
    $sidebarFeatureNameList.html("");

    var featureList = [];
    var inferredFeatureType = {
      discrete: [],
      nonDiscrete: [],
      unknown: [],
    };
    var totalCount = "";
    var inferredType = "";
    var nullRatio = "";
    var estUniqueVal = "";
    var dataType = "";
    var dataTypeCount = "";
    var quantiles = {
      min: "",
      firstQuantile: "",
      median: "",
      thirdQuantile: "",
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
          firstQuantile: "-",
          median: "-",
          thirdQuantile: "-",
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
        frequentItems = featureNameValues.frequentItems.items.reduce((acc, item) => {
          acc.push(item.jsonValue);
          return acc;
        }, []);

        var slicedFrequentItems = featureNameValues.frequentItems.items.slice(0, 5);
        for (var fi = 0; fi < slicedFrequentItems.length; fi++) {
          frequentItemsElemString +=
            '<span class="wl-table-cell__bedge">' + slicedFrequentItems[fi].jsonValue + "</span>";
        }
      } else {
        if (inferredType === "Non-discrete") {
          // Chart
          if (featureNameValues.numberSummary) {
            featureNameValues.numberSummary.histogram.counts.slice(0, 30).forEach(function (count, index) {
              chartData.push({
                axisY: count,
                axisX: index,
              });
            });
          }

          // Frequent item chips / bedge
          frequentItemsElemString = "No data shown";
          frequentItems = [];
        } else {
          frequentItemsElemString = "-";
          chartData = [];
          frequentItems = [];
        }
      }

      // Update sidebar HTML feature name list
      $sidebarFeatureNameList.append(
        '<li class="list-group-item js-list-group-item"><span data-feature-name-id="' +
          featureName +
          '">' +
          featureName +
          "</span></li>",
      );

      function getGraphHtml(data) {
        if (!data.length) return "";

        var MARGIN = {
          TOP: 5,
          RIGHT: 5,
          BOTTOM: 5,
          LEFT: 55,
        };
        var SVG_WIDTH = 250;
        var SVG_HEIGHT = 140;
        var CHART_WIDTH = SVG_WIDTH - MARGIN.LEFT - MARGIN.RIGHT;
        var CHART_HEIGHT = SVG_HEIGHT - MARGIN.TOP - MARGIN.BOTTOM;
        var PRIMARY_COLOR_HEX = "#0e7384";
        var SECONDARY_COLOR_HEX = "#ebf2f3";
        var BORDER_WIDTH = 3;

        var svgEl = d3.create("svg").attr("width", SVG_WIDTH).attr("height", SVG_HEIGHT);
        var tooltip = d3.select("body").append("div").attr("class", "wl-tooltip");

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

        var maxYValue = d3.max(data, (d) => Math.abs(d.axisY));

        var xScale = d3
          .scaleBand()
          .domain(data.map((d) => d.axisX))
          .range([MARGIN.LEFT, MARGIN.LEFT + CHART_WIDTH]);
        var yScale = d3
          .scaleLinear()
          .domain([0, maxYValue * 1.02]) // so that chart's height has 10§2% height of the maximum value
          .range([CHART_HEIGHT, 0]);

        // Add the x Axis
        // svgEl
        //   .append("g")
        //   .attr("transform", "translate(" + 0 + ", " + (CHART_HEIGHT + MARGIN.TOP) + ")")
        //   .call(d3.axisBottom(xScale));

        // Add the y Axis
        svgEl
          .append("g")
          .attr("transform", "translate(" + MARGIN.LEFT + ", " + MARGIN.TOP + ")")
          .call(d3.axisLeft(yScale).ticks(5));

        var gChart = svgEl.append("g");
        gChart
          .selectAll(".bar")
          .data(data)
          .enter()
          .append("rect")
          .classed("bar", true)
          .attr("width", xScale.bandwidth() - 1)
          .attr("height", (d) => CHART_HEIGHT - yScale(d.axisY))
          .attr("x", (d) => xScale(d.axisX))
          .attr("y", (d) => yScale(d.axisY) + MARGIN.TOP)
          .attr("fill", PRIMARY_COLOR_HEX);

        return svgEl._groups[0][0].outerHTML;
      }

      var $tableRow = $(`
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
        <div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">${quantiles.firstQuantile}</div>
        <div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">${quantiles.median}</div>
        <div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">${quantiles.thirdQuantile}</div>
        <div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">${quantiles.max}</div>
      </li>
    `);

      $tableRow.on("click", openPropertyPanel.bind(this, frequentItems));
      // Update data table rows/columns
      $tableBody.append($tableRow);
    }

    $featureCountDiscrete.html(inferredFeatureType.discrete.length);
    $featureCountNonDiscrete.html(inferredFeatureType.nonDiscrete.length);
    $featureCountUnknown.html(inferredFeatureType.unknown.length);
    $selectedProfile.html(formatLabelDate(+properties.dataTimestamp));
    $featureCount.html(featureList.length);
  }

  function renderList() {
    var tableBodyChildrens = $tableBody.children();

    $.each($featureFilterInput, function (_, filterInput) {
      isActiveInferredType[filterInput.value.toLowerCase()] = filterInput.checked;
    });

    for (var i = 0; i < tableBodyChildrens.length; i++) {
      var name = tableBodyChildrens[i].dataset.featureName.toLowerCase();
      var type = tableBodyChildrens[i].dataset.inferredType.toLowerCase();

      if (isActiveInferredType[type] && name.startsWith(featureSearchValue)) {
        tableBodyChildrens[i].style.display = "";
      }
    }
  }

  function loadFile(e) {
    var input, file, fr;

    if (typeof window.FileReader !== "function") {
      alert("The file API isn't supported on this browser yet.");
      return;
    }

    input = document.getElementById("file-input");
    if (!input) {
      alert("Um, couldn't find the file input element.");
    } else if (!input.files) {
      alert("This browser doesn't seem to support the `files` property of file inputs.");
    } else if (!input.files[0]) {
      alert("Please select a file before clicking the 'Load file' button.");
    } else {
      file = input.files[0];
      fr = new FileReader();
      fr.onload = receivedText;
      fr.readAsText(file);
    }
  }

  function receivedText(e) {
    var lines = e.target.result;
    var newArr = JSON.parse(lines);

    updateHtmlElementValues(newArr);
    $tableMessage.addClass("d-none");
    $sidebarContent.removeClass("d-none");
    $tableContent.removeClass("d-none");
    renderList();
    $jsonForm.trigger("reset");
  }
  // Load data from JSON file
  // $.getJSON(JSON_URL, updateHtmlElementValues).then(function () {
  //   renderList();
  // });

  // Bind event listeners
  $fileInput.on("change", loadFile);
  // $(document).on("click", ".wl-table-row", openPropertyPanel);
  $(document).on("click", ".js-list-group-item span", scrollToFeatureName);
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
  $(".wl-property-panel__button").on("click", handleClosePropertyPanel);
})();
