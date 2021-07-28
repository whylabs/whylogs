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
  const $profileDropdown = $(".sidebar-content__profile-dropdown");
  const $propertyPanelTitle = $(".wl-property-panel__title");
  const $propertyPanelProfileName = $(".wl-property-panel__table-th-profile");

  const $removeButton = $(`#remove-button-1`);

  // Constants and variables
  let batchArray = [];
  let featureSearchValue = "";
  const isActiveInferredType = {};
  let propertyPanelData = [];
  let profiles = [];
  let jsonData = {};
  let dataForRead = {};
  let featureDataForTableForAllProfiles = {};
  let numOfProfilesBasedOnType = {};
  let selectedProfiles = [];
  selectedProfiles.push("0");
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

    for (let i = 0; i < tableBodyChildrens.length; i++) {
      const name = tableBodyChildrens[i].dataset.featureName.toLowerCase();
      const type = tableBodyChildrens[i].dataset.inferredType.toLowerCase();

      if (isActiveInferredType[type] && name.startsWith(featureSearchValue)) {
        tableBodyChildrens[i].style.display = "";
      } else {
        tableBodyChildrens[i].style.display = "none";
      }
    }

    for (let i = 0; i < featureListChildren.length; i++) {
      const name = featureListChildren[i].dataset.featureName.toLowerCase();
      const type = featureListChildren[i].dataset.inferredType.toLowerCase();

      if (isActiveInferredType[type]) {
        featureListChildren[i].style.display = "";
      } else {
        featureListChildren[i].style.display = "none";
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
  function updateHtmlElementValues() {
    let iteration = 0;
    Object.entries(featureDataForTableForAllProfiles).forEach((feature) => {
      function getGraphHtml(data) {
        const MARGIN = {
          TOP: 5,
          RIGHT: 5,
          BOTTOM: 5,
          LEFT: 55,
        };
        const SVG_WIDTH = 350;
        const SVG_HEIGHT = 30;
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
      // strings for tableToShow
      let tempChartDataString = "";
      feature[1].chartData.forEach((chartData, index) => {
        if (selectedProfiles.includes(String(index))) {
          tempChartDataString += `<div>${getGraphHtml(chartData)}</div>`;
        }
      });

      let freaquentItemsElmString = "";
      feature[1].frequentItemsElemString.forEach((frequentItemElemString, index) => {
        if (selectedProfiles.includes(String(index))) {
          freaquentItemsElmString += ` <div class="wl-table-cell__bedge-wrap">${frequentItemElemString}</div>`;
        }
      });
      let inferredTypeString = "";
      feature[1].inferredType.forEach((inferredType, index) => {
        if (selectedProfiles.includes(String(index))) {
          inferredTypeString += `<div>${inferredType}</div>`;
        }
      });
      let totalCountString = "";
      feature[1].totalCount.forEach((totalCount, index) => {
        if (selectedProfiles.includes(String(index))) {
          totalCountString += `<div>${totalCount}</div>`;
        }
      });
      let nullRationString = "";
      feature[1].nullRatio.forEach((nullRatio, index) => {
        if (selectedProfiles.includes(String(index))) {
          nullRationString += `<div> ${nullRatio}</div>`;
        }
      });
      let estUniqueValString = "";
      feature[1].estUniqueVal.map((estUniqueVal, index) => {
        if (selectedProfiles.includes(String(index))) {
          estUniqueValString += `<div>${estUniqueVal}</div>`;
        }
      });
      let meanString = "";
      feature[1].mean.forEach((mean, index) => {
        if (selectedProfiles.includes(String(index))) {
          meanString += `<div>${mean}</div>`;
        }
      });
      let stddevString = "";
      feature[1].stddev.forEach((stddev, index) => {
        if (selectedProfiles.includes(String(index))) {
          stddevString += `<div>${stddev}</div>`;
        }
      });
      let dataTypeString = "";
      feature[1].dataType.forEach((dataType, index) => {
        if (selectedProfiles.includes(String(index))) {
          dataTypeString += `<div>${dataType}</div>`;
        }
      });
      let dataTypeCountString = "";
      feature[1].dataTypeCount.forEach((dataTypeCount, index) => {
        if (selectedProfiles.includes(String(index))) {
          dataTypeCountString += `<div>${dataTypeCount}</div>`;
        }
      });
      let quantilesMinString = "";
      feature[1].quantiles.forEach((quantiles, index) => {
        if (selectedProfiles.includes(String(index))) {
          quantilesMinString += `<div>${quantiles.min}</div>`;
        }
      });
      let quantilesMedianString = "";
      feature[1].quantiles.forEach((quantiles, index) => {
        if (selectedProfiles.includes(String(index))) {
          quantilesMedianString += `<div>${quantiles.median}</div>`;
        }
      });
      let quantilesThirdQuantileString = "";
      feature[1].quantiles.forEach((quentiles, index) => {
        if (selectedProfiles.includes(String(index))) {
          quantilesThirdQuantileString += `<div>${quentiles.thirdQuantile}</div>`;
        }
      });
      let quantilesMaxString = "";
      feature[1].quantiles.forEach((quantiles, index) => {
        if (selectedProfiles.includes(String(index))) {
          quantilesMaxString += `<div>${quantiles.max}</div>`;
        }
      });
      let quantilesFirsString = "";
      feature[1].quantiles.forEach((quantiles, index) => {
        if (selectedProfiles.includes(String(index))) {
          quantilesFirsString += `<div>${quantiles.firstQuantile}</div>`;
        }
      });
      const $tableRow =
        `
      <li class="wl-table-row${
        feature[1].inferredType[0].toLowerCase() !== "unknown" ? " wl-table-row--clickable" : ""
      }" data-feature-name="${feature[0]}" data-inferred-type="${
          feature[1].inferredType[0]
        }" data-scroll-to-feature-name="${feature[0]}" style="display: none;">
        <div class="wl-table-cell">
          <div class="wl-table-cell__title-wrap">
            <h4 class="wl-table-cell__title">${feature[0]}</h4>
          </div>
          <div class="wl-table-cell__graph-wrap">` +
        tempChartDataString +
        `</div></div><div class="wl-table-cell wl-table-cell--top-spacing align-middle" style="max-width: 270px">` +
        freaquentItemsElmString +
        `</div><div class="wl-table-cell wl-table-cell--top-spacing align-middle">` +
        inferredTypeString +
        `</div><div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">` +
        totalCountString +
        `</div><div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">` +
        nullRationString +
        `</div><div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">` +
        estUniqueValString +
        `</div><div class="wl-table-cell wl-table-cell--top-spacing align-middle">` +
        dataTypeString +
        `</div><div class="wl-table-cell wl-table-cell--top-spacing align-middle">` +
        dataTypeCountString +
        `</div><div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">` +
        meanString +
        `</div><div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">` +
        stddevString +
        `</div><div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">` +
        quantilesMinString +
        `</div><div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">` +
        quantilesFirsString +
        `</div><div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">` +
        quantilesMedianString +
        `</div><div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">` +
        quantilesThirdQuantileString +
        `</div><div class="wl-table-cell wl-table-cell--top-spacing align-middle text-end">` +
        quantilesMaxString +
        `</div></li>`;

      const $tableRowButton = $(`<button class="wl-table-cell__title-button" type="button">View details</button>`);
      $tableRowButton.on(
        "click",
        openPropertyPanel.bind(this, propertyPanelData, feature[1].inferredType[0].toLowerCase()),
      );
      // $tableRow.find(".wl-table-cell__title-wrap").append($tableRowButton);
      // Update data table rows/columns
      $tableBody.append($tableRow);

      // $featureCountDiscrete.html(numOfProfilesBasedOnType[feature[0]].inferredFeatureType.discrete.length);
      // $featureCountNonDiscrete.html(numOfProfilesBasedOnType[iteration].inferredFeatureType.nonDiscrete.length);
      // $featureCountUnknown.html(numOfProfilesBasedOnType[iteration].inferredFeatureType.unknown.length);
      // // $selectedProfile.html(formatLabelDate(+properties.dataTimestamp));
      // $featureCount.html(featureDataForTableForAllProfiles.length);

      iteration += 1;
    });
  }

  function renderList() {
    const tableBodyChildrens = $tableBody.children();
    const featureListChildren = $sidebarFeatureNameList.children();

    $.each($featureFilterInput, (_, filterInput) => {
      isActiveInferredType[filterInput.value.toLowerCase()] = filterInput.checked;
    });

    for (let i = 0; i < tableBodyChildrens.length; i++) {
      const name = tableBodyChildrens[i].dataset.featureName.toLowerCase();
      const type = tableBodyChildrens[i].dataset.inferredType.toLowerCase();

      if (isActiveInferredType[type] && name.startsWith(featureSearchValue)) {
        tableBodyChildrens[i].style.display = "";
      }
    }
    for (let i = 0; i < featureListChildren.length; i++) {
      const name = featureListChildren[i].dataset.featureName.toLowerCase();
      const type = featureListChildren[i].dataset.inferredType.toLowerCase();

      if (isActiveInferredType[type]) {
        featureListChildren[i].style.display = "";
      }
    }
  }

  function renderProfileDropdown() {
    $profileDropdown.html("");

    for (let i = 0; i < profiles.length; i++) {
      if (selectedProfiles.includes(profiles[i]) || (selectedProfiles.length === 0 && i === 0)) {
        const option = `<option class="already-choosen-data" value="${profiles[i].value}"${i === 0 ? "selected" : ""}>${
          profiles[i].label
        }</option>`;
        $profileDropdown.append(option);
      } else {
        const option = `<option value="${i}"${i === 0 ? "selected" : ""}>${profiles[i].label}</option>`;
        $profileDropdown.append(option);
      }
    }
  }

  function handleProfileChange(event) {
    const value = event.target.value;
    const id = event.target.dataset;
    selectedProfiles[parseInt(id.id)] = value;

    $tableBody.html("");
    updateHtmlElementValues();
    renderList();
  }

  function mapProfileDataToReadData(jsonData, dataForRead) {
    Object.entries(jsonData).forEach((profile) => {
      if (!dataForRead.properties) {
        dataForRead.properties = [];
      }
      dataForRead.properties.push(profile[1].properties);

      Object.entries(profile[1].columns).forEach((feature) => {
        let tempFeatureName = feature[0];
        if (!dataForRead.columns) {
          dataForRead.columns = [];
        }
        if (!dataForRead.columns[tempFeatureName]) dataForRead.columns[tempFeatureName] = [];
        dataForRead.columns[tempFeatureName].push(feature[1]);
      });
    });
    console.log(dataForRead);
    makeFeatureDataForAllProfilesToShowOnTable(
      featureDataForTableForAllProfiles,
      dataForRead,
      numOfProfilesBasedOnType,
    );
    console.log(featureDataForTableForAllProfiles);
    console.log(numOfProfilesBasedOnType);
  }

  function makeFeatureDataForAllProfilesToShowOnTable(
    featureDataForTableForAllProfiles,
    dataForRead,
    numOfProfilesBasedOnType,
  ) {
    Object.entries(dataForRead.columns).map((feature) => {
      if (!featureDataForTableForAllProfiles[feature[0]]) {
        featureDataForTableForAllProfiles[feature[0]] = {
          totalCount: [],
          inferredType: [],
          nullRatio: [],
          estUniqueVal: [],
          dataType: [],
          dataTypeCount: [],
          quantiles: [],
          mean: [],
          stddev: [],
          chartData: [],
          frequentItemsElemString: [],
        };
        numOfProfilesBasedOnType[feature[0]] = {
          discrete: [],
          nonDiscrete: [],
          unknown: [],
        };
        propertyPanelData[feature[0]] = {};
      }
      let iteration = 0;
      feature[1].forEach((tempFeatureValues) => {
        if (tempFeatureValues.numberSummary) {
          let inffTypeTemp = tempFeatureValues.numberSummary.isDiscrete ? "Discrete" : "Non-Discrete";
          tempFeatureValues.numberSummary.isDiscrete
            ? numOfProfilesBasedOnType[feature[0]].discrete.push(feature[0])
            : numOfProfilesBasedOnType[feature[0]].nonDiscrete.push(feature[0]);
          featureDataForTableForAllProfiles[feature[0]].totalCount.push(tempFeatureValues.numberSummary.count);
          featureDataForTableForAllProfiles[feature[0]].inferredType.push(inffTypeTemp);
          featureDataForTableForAllProfiles[feature[0]].quantiles.push(
            getQuantileValues(tempFeatureValues.numberSummary.quantiles.quantileValues),
          );
          featureDataForTableForAllProfiles[feature[0]].mean.push(fixNumberTo(tempFeatureValues.numberSummary.mean));
          featureDataForTableForAllProfiles[feature[0]].stddev.push(
            fixNumberTo(tempFeatureValues.numberSummary.stddev),
          );
        } else {
          numOfProfilesBasedOnType[feature[0]].unknown.push(feature[0]);
          featureDataForTableForAllProfiles[feature[0]].inferredType.push("Unknown");
          featureDataForTableForAllProfiles[feature[0]].totalCount.push("-");
          let quantiles = {
            min: "-",
            firstQuantile: "-",
            median: "-",
            thirdQuantile: "-",
            max: "-",
          };
          featureDataForTableForAllProfiles[feature[0]].quantiles.push(quantiles);
          featureDataForTableForAllProfiles[feature[0]].mean.push("-");
          featureDataForTableForAllProfiles[feature[0]].stddev.push("-");
        }
        featureDataForTableForAllProfiles[feature[0]].estUniqueVal.push(
          feature[1].uniqueCount ? fixNumberTo(tempFeatureValues.uniqueCount.estimate) : "-",
        );
        featureDataForTableForAllProfiles[feature[0]].nullRatio.push(
          tempFeatureValues.schema.typeCounts.NULL ? tempFeatureValues.schema.typeCounts.NULL : "0",
        );
        featureDataForTableForAllProfiles[feature[0]].dataType.push(tempFeatureValues.schema.inferredType.type);
        featureDataForTableForAllProfiles[feature[0]].dataTypeCount.push(
          tempFeatureValues.schema.typeCounts[featureDataForTableForAllProfiles[feature[0]].dataType],
        );

        featureDataForTableForAllProfiles[feature[0]].chartData[iteration] = [];
        featureDataForTableForAllProfiles[feature[0]].frequentItemsElemString[iteration] = "";
        if (
          featureDataForTableForAllProfiles[feature[0]].inferredType[iteration] === "Discrete" &&
          tempFeatureValues.frequentItems &&
          tempFeatureValues.frequentItems.items.length
        ) {
          // Chart
          tempFeatureValues.frequentItems.items.forEach((item, index) => {
            featureDataForTableForAllProfiles[feature[0]].chartData[iteration].push({
              axisY: item.estimate,
              axisX: index,
            });
          });

          // Frequent item chips / bedge
          propertyPanelData[feature[0]] = tempFeatureValues.frequentItems.items.reduce((acc, item) => {
            acc.push({
              value: item.jsonValue,
              count: item.estimate,
            });
            return acc;
          }, []);

          const slicedFrequentItems = tempFeatureValues.frequentItems.items.slice(0, 5);
          for (let fi = 0; fi < slicedFrequentItems.length; fi++) {
            featureDataForTableForAllProfiles[feature[0]].frequentItemsElemString[iteration] +=
              '<span class="wl-table-cell__bedge">' + slicedFrequentItems[fi].jsonValue + "</span>";
          }
        } else {
          if (featureDataForTableForAllProfiles[feature[0]].inferredType[iteration] === "Non-Discrete") {
            // Chart
            if (tempFeatureValues.numberSummary) {
              // Histogram chips / bedge
              propertyPanelData[feature[0]] = tempFeatureValues.numberSummary.histogram.counts.reduce(
                (acc, value, index) => {
                  acc.push({
                    value: value,
                    count: tempFeatureValues.numberSummary.histogram.bins[index],
                  });
                  return acc;
                },
                [],
              );

              tempFeatureValues.numberSummary.histogram.counts.slice(0, 30).forEach((count, index) => {
                featureDataForTableForAllProfiles[feature[0]].chartData[iteration].push({
                  axisY: count,
                  axisX: index,
                });
              });
            }

            // Frequent item chips / bedge
            featureDataForTableForAllProfiles[feature[0]].frequentItemsElemString[iteration] = "No data to show";
          } else {
            featureDataForTableForAllProfiles[feature[0]].frequentItemsElemString[iteration] = "-";
            featureDataForTableForAllProfiles[feature[0]].chartData[iteration] = [];
            propertyPanelData[feature[0]] = [];
          }
        }
        iteration += 1;
      });
    });
  }

  function updateTableMessage(message) {
    $tableMessage.find("p").html(message);
  }

  function collectProfilesFromJSON(data) {
    return Object.keys(data).map((profile, i) => ({
      label: profile,
      value: profile,
      index: i,
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
    mapProfileDataToReadData(jsonData, dataForRead);

    if (checkJSONValidityForMultiProfile(jsonData)) {
      profiles = collectProfilesFromJSON(jsonData);
      renderProfileDropdown();
      updateHtmlElementValues();
      $multiProfileWrap.removeClass("d-none");
      $singleProfileWrap.addClass("d-none");
    } else if (checkJSONValidityForSingleProfile(jsonData)) {
      $multiProfileWrap.addClass("d-none");
      $singleProfileWrap.removeClass("d-none");
      updateHtmlElementValues();
    } else {
      updateTableMessage(MESSAGES.error.invalidJSONFile);
      hideDataVisibility();
      return;
    }

    renderList();
    showDataVisibility();
    $jsonForm.trigger("reset");
  }

  for (let i = 0; i < 1; i++) {
    let addButton = $(`#add-profile-sidebar-button-${i}`);

    addButton.on("click", function () {
      $(`#add-profile-wrap-${i + 1}`).removeClass("d-none");
      $(`#remove-button-${i + 1}`).removeClass("d-none");
    });
  }

  $removeButton.on("click", function () {
    $(`#add-profile-wrap-1`).addClass("d-none");
    selectedProfiles.pop();
    $removeButton.addClass("d-none");
  });

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
