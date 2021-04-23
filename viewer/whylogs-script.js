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
        var frequentItems = featureNameValues.frequentItems.items.slice(0, 5);
        for (var fi = 0; fi < frequentItems.length; fi++) {
          frequentItemsElemString +=
            // '<span class="badge bg-light text-dark">' + frequentItems[fi].jsonValue + "</span>";
            '<span class="wl-table-cell__bedge">' + frequentItems[fi].jsonValue + "</span>";
        }
      } else {
        if (inferredType === "Non-discrete") {
          frequentItemsElemString = "No data available";
        } else {
          frequentItemsElemString = "-";
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

      // Update data table rows/columns
      $tableBody.append(`
        <li class="wl-table-row" data-feature-name="${featureName}" data-inferred-type="${inferredType}" style="display: none;">
          <div class="wl-table-cell">
            <h4 class="wl-table-cell__title">${featureName}</h4>
            <img src="images/placement-chart.png" alt="For placement only" width="265px" />
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
