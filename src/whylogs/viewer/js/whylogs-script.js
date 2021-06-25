"use strict";

(function () {
  const MESSAGES = {
    error: {
      noInputElementFound: "It seems we could not find the file input element.",
      noBrowserSupport: "This browser does not seem to support the `files` property of file inputs.",
      noFileSelected: "Please select a file.",
      fileAPINotSupported: "The file API is not supported on this browser yet.",
      invalidJSONFile:
        "The JSON file you are trying to load does not match the expected whylogs format. Please check the file and try again.",
    },
  };

  // HTML Elements
  const $selectedProfile = $(".wl__selected-profile");
  const $featureCount = $(".wl__feature-count");
  const $sidebarFeatureNameList = $(".wl__sidebar-feature-name-list");
  const $featureCountDiscrete = $(".wl__feature-count--discrete");
  const $featureCountNonDiscrete = $(".wl__feature-count--non-discrete");
  const $featureCountUnknown = $(".wl__feature-count--unknown");
  const $tableBody = $(".wl__table-body");
  const $featureSearch = $("#wl__feature-search");
  const $featureFilterInput = $(".wl__feature-filter-input");
  const $jsonForm = $("#json-form");
  const $fileInput = $("#file-input");
  const $tableContent = $("#table-content");
  const $tableMessage = $("#table-message");
  const $sidebarContent = $("#sidebar-content");
  const $singleProfileWrap = $("#sidebar-content-single-profile");
  const $multiProfileWrap = $("#sidebar-content-multi-profile");
  const $profileDropdown = $("#sidebar-content-multi-profile-dropdown");
  const $propertyPanelTitle = $(".wl-property-panel__title");
  const $propertyPanelProfileName = $(".wl-property-panel__table-th-profile");

  // Constants and variables
  let batchArray = [];
  let featureSearchValue = "";
  const isActiveInferredType = {};
  let propertyPanelData = [];
  let profiles = [];
  let jsonData = {};

  // Util functions
  function debounce(func, wait, immediate) {
    let timeout;

    return function () {
      const context = this;
      const args = arguments;
      const later = function () {
        timeout = null;
        if (!immediate) func.apply(context, args);
      };

      const callNow = immediate && !timeout;
      clearTimeout(timeout);
      timeout = setTimeout(later, wait);
      if (callNow) func.apply(context, args);
    };
  }

  function handleSearch() {
    const tableBodyChildrens = $tableBody.children();
    const featureListChildren = $sidebarFeatureNameList.children();
    let featureCount = 0;
    for (let i = 0; i < tableBodyChildrens.length; i++) {
      const name = tableBodyChildrens[i].dataset.featureName.toLowerCase();
      const type = tableBodyChildrens[i].dataset.inferredType.toLowerCase();

      if (isActiveInferredType[type] && name.startsWith(featureSearchValue)) {
        tableBodyChildrens[i].style.display = "";
        featureCount++;
      } else {
        tableBodyChildrens[i].style.display = "none";
      }
    }

    for (let i = 0; i < featureListChildren.length; i++) {
      const name = featureListChildren[i].dataset.featureName.toLowerCase();
      const type = featureListChildren[i].dataset.inferredType.toLowerCase();

      if (isActiveInferredType[type] && name.startsWith(featureSearchValue)) {
        featureListChildren[i].style.display = "";
      } else {
        featureListChildren[i].style.display = "none";
      }
    }
    $featureCount.html(featureCount);
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
    const TABLE_HEADER_OFFSET = 90;
    const $tableWrap = $(".wl-table-wrap");
    const featureNameId = event.currentTarget.dataset.featureNameId;
    const currentOffsetTop = $tableWrap.scrollTop();
    const offsetTop = $('[data-scroll-to-feature-name="' + featureNameId + '"]').offset().top;

    $tableWrap.animate(
      {
        scrollTop: currentOffsetTop + offsetTop - TABLE_HEADER_OFFSET,
      },
      500,
    );
  }

  function openPropertyPanel(items, infType) {
    if (items.length > 0 && items !== "undefined") {
      let chipString = "";
      const chipElement = (chip) => `<span class="wl-table-cell__bedge">${chip}</span>`;
      const chipElementTableData = (value) => `<td class="wl-property-panel__table-td" >${chipElement(value)}</td>`;
      const chipElementEstimation = (count) =>
        `<td class="wl-property-panel__table-td wl-property-panel__table-td-profile" >${count}</td>`;

      items.forEach((item) => {
        chipString += `
        <tr class="wl-property-panel__table-tr">
          ${chipElementTableData(item.value)}
          ${chipElementEstimation(item.count)}
        </tr>
        `;
      });
      $(".wl-property-panel__frequent-items").html(chipString);
      if (infType === "non-discrete") {
        $propertyPanelTitle.html("Histogram data:");
        $propertyPanelProfileName.html("Bin values");
      } else if (infType === "discrete") {
        $propertyPanelTitle.html("Frequent items:");
        $propertyPanelProfileName.html("Counts");
      }

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
    const properties = data.properties;
    const columns = data.columns;
    batchArray = Object.entries(columns);
    $tableBody.html("");
    $sidebarFeatureNameList.html("");

    const featureList = [];
    const inferredFeatureType = {
      discrete: [],
      nonDiscrete: [],
      unknown: [],
    };
    let totalCount = "";
    let inferredType = "";
    let nullRatio = "";
    let estUniqueVal = "";
    let dataType = "";
    let dataTypeCount = "";
    let quantiles = {
      min: "",
      firstQuantile: "",
      median: "",
      thirdQuantile: "",
      max: "",
    };
    let mean = "";
    let stddev = "";

    for (let i = 0; i < batchArray.length; i++) {
      const featureName = batchArray[i][0];
      const featureNameValues = batchArray[i][1];
      let frequentItemsElemString = "";
      let chartData = [];

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
        inferredType === "Discrete" &&
        featureNameValues.frequentItems &&
        featureNameValues.frequentItems.items.length
      ) {
        // Chart
        featureNameValues.frequentItems.items.forEach((item, index) => {
          chartData.push({
            axisY: item.estimate,
            axisX: index,
          });
        });

        // Frequent item chips / bedge
        propertyPanelData = featureNameValues.frequentItems.items.reduce((acc, item) => {
          acc.push({
            value: item.jsonValue,
            count: item.estimate,
          });
          return acc;
        }, []);

        const slicedFrequentItems = featureNameValues.frequentItems.items.slice(0, 5);
        for (let fi = 0; fi < slicedFrequentItems.length; fi++) {
          frequentItemsElemString +=
            '<span class="wl-table-cell__bedge">' + slicedFrequentItems[fi].jsonValue + "</span>";
        }
      } else {
        if (inferredType === "Non-discrete") {
          // Chart
          if (featureNameValues.numberSummary) {
            // Histogram chips / bedge
            propertyPanelData = featureNameValues.numberSummary.histogram.counts.reduce((acc, value, index) => {
              acc.push({
                value: value,
                count: featureNameValues.numberSummary.histogram.bins[index],
              });
              return acc;
            }, []);

            featureNameValues.numberSummary.histogram.counts.slice(0, 30).forEach((count, index) => {
              chartData.push({
                axisY: count,
                axisX: index,
              });
            });
          }

          // Frequent item chips / bedge
          frequentItemsElemString = "No data to show";
        } else {
          frequentItemsElemString = "-";
          chartData = [];
          propertyPanelData = [];
        }
      }

      // Update sidebar HTML feature name list
      $sidebarFeatureNameList.append(
        `<li class="list-group-item js-list-group-item" data-feature-name="${featureName}" data-inferred-type="${inferredType}" style="display: none"><span data-feature-name-id="${featureName}" >${featureName}</span></li>`,
      );

      function getGraphHtml(data) {
        if (!data.length) return "";

        const MARGIN = {
          TOP: 5,
          RIGHT: 5,
          BOTTOM: 5,
          LEFT: 55,
        };
        const SVG_WIDTH = 350;
        const SVG_HEIGHT = 140;
        const CHART_WIDTH = SVG_WIDTH - MARGIN.LEFT - MARGIN.RIGHT;
        const CHART_HEIGHT = SVG_HEIGHT - MARGIN.TOP - MARGIN.BOTTOM;
        const PRIMARY_COLOR_HEX = "#0e7384";

        const svgEl = d3.create("svg").attr("width", SVG_WIDTH).attr("height", SVG_HEIGHT);

        const maxYValue = d3.max(data, (d) => Math.abs(d.axisY));

        const xScale = d3
          .scaleBand()
          .domain(data.map((d) => d.axisX))
          .range([MARGIN.LEFT, MARGIN.LEFT + CHART_WIDTH]);
        const yScale = d3
          .scaleLinear()
          .domain([0, maxYValue * 1.02]) // so that chart's height has 10§2% height of the maximum value
          .range([CHART_HEIGHT, 0]);

        // Add the y Axis
        svgEl
          .append("g")
          .attr("transform", "translate(" + MARGIN.LEFT + ", " + MARGIN.TOP + ")")
          .call(d3.axisLeft(yScale).ticks(5));

        const gChart = svgEl.append("g");
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

      const $tableRow = $(`
      <li class="wl-table-row${
        inferredType.toLowerCase() !== "unknown" ? " wl-table-row--clickable" : ""
      }" data-feature-name="${featureName}" data-inferred-type="${inferredType}" data-scroll-to-feature-name="${featureName}" style="display: none;">
        <div class="wl-table-cell">
          <div class="wl-table-cell__title-wrap">
            <h4 class="wl-table-cell__title">${featureName}</h4>
          </div>
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

      const $tableRowButton = $(`<button class="wl-table-cell__title-button" type="button">View details</button>`);
      $tableRowButton.on("click", openPropertyPanel.bind(this, propertyPanelData, inferredType.toLowerCase()));
      $tableRow.find(".wl-table-cell__title-wrap").append($tableRowButton);
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
    const tableBodyChildrens = $tableBody.children();
    const featureListChildren = $sidebarFeatureNameList.children();
    let featureCount = 0;
    $.each($featureFilterInput, (_, filterInput) => {
      isActiveInferredType[filterInput.value.toLowerCase()] = filterInput.checked;
    });

    for (let i = 0; i < tableBodyChildrens.length; i++) {
      const name = tableBodyChildrens[i].dataset.featureName.toLowerCase();
      const type = tableBodyChildrens[i].dataset.inferredType.toLowerCase();

      if (isActiveInferredType[type]) {
        tableBodyChildrens[i].style.display = "";
        featureCount++;
      }
    }
    for (let i = 0; i < featureListChildren.length; i++) {
      const name = featureListChildren[i].dataset.featureName.toLowerCase();
      const type = featureListChildren[i].dataset.inferredType.toLowerCase();

      if (isActiveInferredType[type]) {
        featureListChildren[i].style.display = "";
      }
    }
    $featureCount.html(featureCount);
  }

  function renderProfileDropdown() {
    $profileDropdown.html("");

    for (let i = 0; i < profiles.length; i++) {
      const option = `<option value="${profiles[i].value}"${i === 0 ? "selected" : ""}>${profiles[i].label}</option>`;
      $profileDropdown.append(option);
    }
  }

  function handleProfileChange(event) {
    const value = event.target.value;

    updateHtmlElementValues(jsonData[value]);
    renderList();
  }

  function updateTableMessage(message) {
    $tableMessage.find("p").html(message);
  }

  function collectProfilesFromJSON(data) {
    return Object.keys(data).map((profile) => ({
      label: profile,
      value: profile,
    }));
  }

  function compareArrays(array, target) {
    if (array.length !== target.length) return false;

    array.sort();
    target.sort();

    for (let i = 0; i < array.length; i++) {
      if (array[i] !== target[i]) {
        return false;
      }
    }

    return true;
  }

  function checkJSONValidityForSingleProfile(data) {
    const VALID_KEY_NAMES = ["properties", "columns"];
    const keys = Object.keys(data);

    return keys.length === 2 && compareArrays(VALID_KEY_NAMES, keys);
  }

  function hasFalseValue(array) {
    return array.some((obj) => checkJSONValidityForSingleProfile(obj) === false);
  }

  function checkJSONValidityForMultiProfile(data) {
    const values = Object.values(data);

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
    if (typeof window.FileReader !== "function") {
      updateTableMessage(MESSAGES.error.fileAPINotSupported);
      return;
    }

    const input = document.getElementById("file-input");
    if (!input) {
      updateTableMessage(MESSAGES.error.noInputElementFound);
    } else if (!input.files) {
      updateTableMessage(MESSAGES.error.noBrowserSupport);
    } else if (!input.files[0]) {
      updateTableMessage(MESSAGES.error.noFileSelected);
    } else {
      const file = input.files[0];
      const fr = new FileReader();
      fr.onload = receivedText;
      fr.readAsText(file);
    }
  }

  function receivedText(e) {
    const lines = e.target.result;
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
    debounce((event) => {
      featureSearchValue = event.target.value.toLowerCase();
      handleSearch();
    }, 300),
  );
  $featureFilterInput.on("change", (event) => {
    const filterType = event.target.value.toLowerCase();
    const isChecked = event.target.checked;

    isActiveInferredType[filterType] = isChecked;
    handleSearch();
  });
  $(".wl-property-panel__button").on("click", handleClosePropertyPanel);
})();
