"use strict";

(function () {
  var MESSAGES = {
    error: {
      noInputElementFound: "It seems we could not find the file input element.",
      noBrowserSupport: "This browser does not seem to support the `files` property of file inputs.",
      noFileSelected: "Please select a file.",
      fileAPINotSupported: "The file API is not supported on this browser yet.",
      invalidJSONFile:
        "It seems the JSON file you are trying to load is not valid due because it does not match whylogs format. Please check the file validity and try again.",
    },
  };

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
  var $singleProfileWrap = $("#sidebar-content-single-profile");
  var $multiProfileWrap = $("#sidebar-content-multi-profile");
  var $profileDropdown = $("#sidebar-content-multi-profile-dropdown");

  // Constants and variables
  var batchArray = [];
  var featureSearchValue = "";
  var isActiveInferredType = {};
  var frequentItems = [];
  var profiles = [];
  var jsonData = {};

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
      estUniqueVal = featureNameValues.uniqueCount ? fixNumberTo(featureNameValues.uniqueCount.estimate) : "-";
      nullRatio = featureNameValues.schema.typeCounts.NULL ? featureNameValues.schema.typeCounts.NULL : "0";
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

  function renderProfileDropdown() {
    $profileDropdown.html("");

    for (var i = 0; i < profiles.length; i++) {
      var option = `<option value="${profiles[i].value}"${i === 0 ? "selected" : ""}>${profiles[i].label}</option>`;
      $profileDropdown.append(option);
    }
  }

  function handleProfileChange(event) {
    var value = event.target.value;

    updateHtmlElementValues(jsonData[value]);
    renderList();
  }

  function updateTableMessage(message) {
    $tableMessage.find("p").html(message);
  }

  function collectProfilesFromJSON(data) {
    return Object.keys(data).map(function (profile) {
      return {
        label: profile,
        value: profile,
      };
    });
  }

  function compareArrays(array, target) {
    if (array.length !== target.length) return false;

    array.sort();
    target.sort();

    for (var i = 0; i < array.length; i++) {
      if (array[i] !== target[i]) {
        return false;
      }
    }

    return true;
  }

  function checkJSONValidityForSingleProfile(data) {
    var VALID_KEY_NAMES = ["properties", "columns"];
    var keys = Object.keys(data);

    return keys.length === 2 && compareArrays(VALID_KEY_NAMES, keys);
  }

  function hasFalseValue(array) {
    return array.some(function (obj) {
      return checkJSONValidityForSingleProfile(obj) === false;
    });
  }

  function checkJSONValidityForMultiProfile(data) {
    var values = Object.values(data);

    if (!values && !values.length) return false;
    if (hasFalseValue(values)) return false;

    return true;
  }

  function showDataVisibility() {
    $tableMessage.addClass("d-none");
    $sidebarContent.removeClass("d-none");
    $tableContent.removeClass("d-none");
  }

  function hideDataVisibility() {
    $tableMessage.removeClass("d-none");
    $sidebarContent.addClass("d-none");
    $tableContent.addClass("d-none");
  }

  function loadFile() {
    var input, file, fr;

    if (typeof window.FileReader !== "function") {
      updateTableMessage(MESSAGES.error.fileAPINotSupported);
      return;
    }

    input = document.getElementById("file-input");
    if (!input) {
      updateTableMessage(MESSAGES.error.noInputElementFound);
    } else if (!input.files) {
      updateTableMessage(MESSAGES.error.noBrowserSupport);
    } else if (!input.files[0]) {
      updateTableMessage(MESSAGES.error.noFileSelected);
    } else {
      file = input.files[0];
      fr = new FileReader();
      fr.onload = receivedText;
      fr.readAsText(file);
    }
  }

  function receivedText(e) {
    var lines = e.target.result;
    jsonData = JSON.parse(lines);

    if (checkJSONValidityForMultiProfile(jsonData)) {
      profiles = collectProfilesFromJSON(jsonData);
      renderProfileDropdown();
      updateHtmlElementValues(Object.values(jsonData)[0]);
      $multiProfileWrap.removeClass("d-none");
      $singleProfileWrap.addClass("d-none");
    } else if (checkJSONValidityForSingleProfile(jsonData)) {
      $multiProfileWrap.addClass("d-none");
      $singleProfileWrap.removeClass("d-none");
      updateHtmlElementValues(jsonData);
    } else {
      updateTableMessage(MESSAGES.error.invalidJSONFile);
      hideDataVisibility();
      return;
    }

    renderList();
    showDataVisibility();
    $jsonForm.trigger("reset");
  }

  // Bind event listeners
  $fileInput.on("change", loadFile);
  $profileDropdown.on("change", handleProfileChange);
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
