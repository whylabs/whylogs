"use strict";

(function () {
  const MESSAGES = {
    error: {
      noInputElementFound: "It seems we could not find the file input element.",
      noBrowserSupport: "This browser does not seem to support the `files` property of file inputs.",
      noFileSelected: "Please select a file.",
      noProfileSelected: "Please select a profile.",
      fileAPINotSupported: "The file API is not supported on this browser yet.",
      invalidJSONFile:
        "The JSON file you are trying to load does not match the expected whylogs format. Please check the file and try again.",
    },
  };

  // HTML Elements
  const $filterListItem = $("#filter-list-item");
  const $notClickableBurgerIcon = $("#not_clickable_burger_icon");
  const $filterCloseIcon = $("#filter-close-icon");
  const $closeIcon = $("#close-icon");
  const $signUpText = $(".sign-up-text");
  const $burgerIcon = $("#burger_icon");
  const $referenceJsonForm = $("#reference-json-form");
  const $referencefileInput = $("#reference-file-input");
  const $compareProfile = $("#compare-profile");
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
  const $multiProfileWrap = $("#sidebar-content-multi-profile");
  const $profileDropdown = $(".sidebar-content__profile-dropdown");
  const $propertyPanelTitle = $(".wl-property-panel__title");
  const $propertyPanelProfileName = $(".wl-property-panel__table-th-profile");
  const $filterOptions = $(".filter-options");

  // Constants and variables
  let featureSearchValue = "";
  let isActiveInferredType = {};
  let propertyPanelData = [];
  let profiles = [];
  let jsonData = {};
  let referenceJsonData = {};
  let dataForRead = {};
  let featureDataForTableForAllProfiles = {};
  let numOfProfilesBasedOnType = {};
  let selectedProfiles = [];
  selectedProfiles.push("0");
  // Util functions

  const colors = {
    0: "#0e7384",
    1: "#2683c9",
    2: "#44c0e7",
  };
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

    if (!checkJSONValidityForMultiProfile(jsonData) && checkJSONValidityForMultiProfile(jsonData)) {
      for (let i = 0; i < featureListChildren.length; i++) {
        const name = featureListChildren[i].dataset.featureName.toLowerCase();
        const type = featureListChildren[i].dataset.inferredType.toLowerCase();
        if (isActiveInferredType[type] && name.startsWith(featureSearchValue)) {
          featureListChildren[i].style.display = "";
        } else {
          featureListChildren[i].style.display = "none";
        }
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

  function openPropertyPanel(items, infType, infTypeSecond = null) {
    const types = [infType, infTypeSecond];
    $(".wl-property-panel__table-th-profile").addClass("d-none");
    $("#property-panel-0").removeClass("d-none");
    if (checkJSONValidityForMultiProfile(jsonData) || checkJSONValidityForSingleProfile(jsonData)) {
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
  }

  function handleClosePropertyPanel() {
    $(".wl-property-panel").removeClass("wl-property-panel--open");
    $(".wl-table-wrap").removeClass("wl-table-wrap--narrow");
    $(".wl-property-panel__frequent-items").html("");
  }

  // Override and populate HTML element values
  function updateHtmlElementValues() {
    $sidebarFeatureNameList.html("");
    $tableMessage.addClass("d-none");
    Object.entries(featureDataForTableForAllProfiles).forEach((feature) => {
      function getGraphHtml(data, index) {
        const SINGLE_PROFILE_JSON = !profiles.length;
        const MARGIN = {
          TOP: 5,
          RIGHT: 5,
          BOTTOM: 5,
          LEFT: 55,
        };
        const SVG_WIDTH = 350;
        const SVG_HEIGHT = SINGLE_PROFILE_JSON ? 75 : 35;
        const CHART_WIDTH = SVG_WIDTH - MARGIN.LEFT - MARGIN.RIGHT;
        const CHART_HEIGHT = SVG_HEIGHT - MARGIN.TOP - MARGIN.BOTTOM;

        const svgEl = d3.create("svg").attr("width", SVG_WIDTH).attr("height", SVG_HEIGHT);

        const maxYValue = d3.max(data, (d) => Math.abs(d.axisY));

        const xScale = d3
          .scaleBand()
          .domain(data.map((d) => d.axisX))
          .range([MARGIN.LEFT, MARGIN.LEFT + CHART_WIDTH]);
        const yScale = d3
          .scaleLinear()
          .domain([0, maxYValue * 1.02]) // so that chart's height has 102% height of the maximum value
          .range([CHART_HEIGHT, 0]);

        // Add the y Axis
        svgEl
          .append("g")
          .attr("transform", "translate(" + MARGIN.LEFT + ", " + MARGIN.TOP + ")")
          .call(d3.axisLeft(yScale).tickValues([0, maxYValue]));

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
          .attr("fill", colors[index]);

        return svgEl._groups[0][0].outerHTML;
      }
      // strings for tableToShow
      let tempChartDataString = "";
      let referenceTempChartDataString = "";
      feature[1].chartData.forEach((chartData, index) => {
        if (selectedProfiles.includes(String(index))) {
          let profileSelected = selectedProfiles.indexOf(String(index));
          tempChartDataString += `<div>${
            chartData.length > 0
              ? getGraphHtml(feature[1].chartData[0], profileSelected)
              : '<span class="wl-table-cell__bedge-wrap">No data to show the chart</span>'
          }</div>`;
          if (feature[1].chartData[1] && feature[1].chartData[1].length > 0) {
            referenceTempChartDataString+= `<div>${
              chartData.length > 0
                ? getGraphHtml(feature[1].chartData[1], profileSelected)
                : '<span class="wl-table-cell__bedge-wrap">No data to show the chart</span>'
            }</div>`;
          }
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
          inferredType ? (inferredTypeString += `<div>${inferredType}</div>`) : (inferredTypeString += `<div>-</div>`);
        }
      });
      let totalCountString = "";
      feature[1].totalCount.forEach((totalCount, index) => {
        if (selectedProfiles.includes(String(index))) {
          totalCount ? (totalCountString += `<div>${totalCount}</div>`) : (totalCountString += `<div>-</div>`);
        }
      });
      let nullRationString = "";
      feature[1].nullRatio.forEach((nullRatio, index) => {
        if (selectedProfiles.includes(String(index))) {
          nullRatio ? (nullRationString += `<div> ${nullRatio}</div>`) : (nullRationString += `<div>-</div>`);
        }
      });
      let estUniqueValString = "";
      feature[1].estUniqueVal.map((estUniqueVal, index) => {
        if (selectedProfiles.includes(String(index))) {
          estUniqueVal ? (estUniqueValString += `<div>${estUniqueVal}</div>`) : (estUniqueValString += `<div>$-</div>`);
        }
      });
      let meanString = "";
      feature[1].mean.forEach((mean, index) => {
        if (selectedProfiles.includes(String(index))) {
          mean ? (meanString += `<div>${mean}</div>`) : (meanString += `<div>-</div>`);
        }
      });
      let stddevString = "";
      feature[1].stddev.forEach((stddev, index) => {
        if (selectedProfiles.includes(String(index))) {
          stddev ? (stddevString += `<div>${stddev}</div>`) : (stddevString += `<div>-</div>`);
        }
      });
      let dataTypeString = "";
      feature[1].dataType.forEach((dataType, index) => {
        if (selectedProfiles.includes(String(index))) {
          dataType ? (dataTypeString += `<div>${dataType}</div>`) : (dataTypeString += `<div>-</div>`);
        }
      });
      let dataTypeCountString = "";
      feature[1].dataTypeCount.forEach((dataTypeCount, index) => {
        if (selectedProfiles.includes(String(index))) {
          dataTypeCount
            ? (dataTypeCountString += `<div>${dataTypeCount}</div>`)
            : (dataTypeCountString += `<div>-</div>`);
        }
      });
      let quantilesMinString = "";
      feature[1].quantiles.forEach((quantiles, index) => {
        if (selectedProfiles.includes(String(index))) {
          quantiles.min
            ? (quantilesMinString += `<div>${quantiles.min}</div>`)
            : (quantilesMinString += `<div>-</div>`);
        }
      });
      let quantilesMedianString = "";
      feature[1].quantiles.forEach((quantiles, index) => {
        if (selectedProfiles.includes(String(index))) {
          quantiles.median
            ? (quantilesMedianString += `<div>${quantiles.median}</div>`)
            : (quantilesMedianString += `<div>-</div>`);
        }
      });
      let quantilesThirdQuantileString = "";
      feature[1].quantiles.forEach((quentiles, index) => {
        if (selectedProfiles.includes(String(index))) {
          quentiles.thirdQuantile
            ? (quantilesThirdQuantileString += `<div>${quentiles.thirdQuantile}</div>`)
            : (quantilesThirdQuantileString += `<div>-</div>`);
        }
      });
      let quantilesMaxString = "";
      feature[1].quantiles.forEach((quantiles, index) => {
        if (selectedProfiles.includes(String(index))) {
          quantiles.max
            ? (quantilesMaxString += `<div>${quantiles.max}</div>`)
            : (quantilesMaxString += `<div>-</div>`);
        }
      });
      let quantilesFirsString = "";
      feature[1].quantiles.forEach((quantiles, index) => {
        if (selectedProfiles.includes(String(index))) {
          quantiles.firstQuantile
            ? (quantilesFirsString += `<div>${quantiles.firstQuantile}</div>`)
            : (quantilesFirsString += `<div>-</div>`);
        }
      });

      const showButton = selectedProfiles.some(
        (profile) => profile !== null && feature[1].inferredType[profile].toLowerCase() !== "unknown",
      );
      const $tableRow = $(
        `
      <li class="wl-table-row${showButton ? " wl-table-row--clickable" : ""}" data-feature-name="${
          feature[0]
        }" data-inferred-type="${
          feature[1].inferredType[parseInt(selectedProfiles[0] || "0")]
        }" data-scroll-to-feature-name="${feature[0]}" style="display: none;">
        <div class="wl-table-cell">
          <div class="wl-table-cell__title-wrap">
            <h4 class="wl-table-cell__title">${feature[0]}</h4>
          </div>
          <div class="wl-table-cell__graph-wrap">
            <div class="display-flex">` +
            tempChartDataString +
            referenceTempChartDataString +
            `</div>
          </div></div>
          <div class="wl-table-cell wl-table-cell--top-spacing align-middle" style="max-width: 270px; padding-right: 18px">` +
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
          `</div></li>`,
      );
      if (checkJSONValidityForMultiProfile(jsonData) || checkJSONValidityForSingleProfile(jsonData)) {
        const $tableRowButton = $(`<button class="wl-table-cell__title-button" type="button">View details</button>`);
        $tableRowButton.on(
          "click",
          openPropertyPanel.bind(this, propertyPanelData[feature[0]][0], feature[1].inferredType[0].toLowerCase()),
        );
        $tableRow.find(".wl-table-cell__title-wrap").append($tableRowButton);
      }
      // Update data table rows/columns
      $tableBody.append($tableRow);

      $sidebarFeatureNameList.append(
        `
          <li id="filter-list-item" class="wl_filter-list-item list-group-item js-list-group-item" data-feature-name="${feature[0]}" data-inferred-type="${feature[1].inferredType}">
            <div class="arrow-icon-container">
              <div class="wl_list-item-dot"></div>
              <img class="d-none wl_arrow-icon" src="./images/arrow-circle-right.png"/>
            </div>
            <span data-feature-name-id="${feature[0]}" >${feature[0]}</span>
          </li>
        `,
      );
    });
    if (!checkJSONValidityForMultiProfile(jsonData)) {
      const countDiscrete = Object.values(numOfProfilesBasedOnType).reduce(
        (acc, feature) => (acc += feature.discrete.length),
        0,
      );
      const countNonDiscrete = Object.values(numOfProfilesBasedOnType).reduce(
        (acc, feature) => (acc += feature.nonDiscrete.length),
        0,
      );
      const countUnknown = Object.values(numOfProfilesBasedOnType).reduce(
        (acc, feature) => (acc += feature.unknown.length),
        0,
      );

      $featureCountDiscrete.html(countDiscrete);
      $featureCountNonDiscrete.html(countNonDiscrete);
      $featureCountUnknown.html(countUnknown);
    }
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

  function getDataForRead(dataForRead, jsonData, referneceData) {
    if (!dataForRead.properties) {
      dataForRead.properties = [];
    }
    dataForRead.properties.push(jsonData.properties);

    Object.entries(jsonData.columns).forEach((feature) => {
      let tempFeatureName = feature[0];
      if (!dataForRead.columns) {
        dataForRead.columns = [];
      }
      if (!dataForRead.columns[tempFeatureName]) dataForRead.columns[tempFeatureName] = [];
        dataForRead.columns[tempFeatureName].push(feature[1]);
        if (
          referneceData && 
          referneceData.columns[tempFeatureName].numberSummary
        ) {
          const {
            numberSummary, 
            frequentItems,
            ...referenceDataForRead
          } = dataForRead.columns[tempFeatureName][0]
          dataForRead.columns[tempFeatureName].push({ 
            ...referenceDataForRead,
            referenceNumberSummary: referneceData.columns[tempFeatureName].numberSummary,
            referenceFrequentItems: referneceData.columns[tempFeatureName].frequentItems
          })
        }
    });
    return dataForRead
  }

  function mapProfileDataToReadData(jsonData, dataForRead, referneceData) {
    if (checkJSONValidityForMultiProfile(jsonData)) {
      const multiReferenceData = referneceData ? Object.values(referneceData)[0] : referneceData
      dataForRead = getDataForRead(dataForRead, Object.values(jsonData)[0], multiReferenceData)
    } else {
      dataForRead = getDataForRead(dataForRead, jsonData, referneceData)
    }

    makeFeatureDataForAllProfilesToShowOnTable(
      featureDataForTableForAllProfiles,
      dataForRead,
      numOfProfilesBasedOnType,
    );
  }
  

  function makeFeatureDataForAllProfilesToShowOnTable(
    featureDataForTableForAllProfiles,
    dataForRead,
    numOfProfilesBasedOnType,
  ) {
    if (Object.keys(featureDataForTableForAllProfiles).length !== 0) {
      featureDataForTableForAllProfiles = {};
    }
    if (Object.keys(numOfProfilesBasedOnType).length !== 0) {
      numOfProfilesBasedOnType = {};
    }
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
          refereneChartData: [],
          frequentItemsElemString: [],
        };
        numOfProfilesBasedOnType[feature[0]] = {
          discrete: [],
          nonDiscrete: [],
          unknown: [],
        };
        propertyPanelData[feature[0]] = [];
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
          if (tempFeatureValues.referenceFrequentItems) {
            tempFeatureValues.referenceFrequentItems.items.forEach((item, index) => {
              featureDataForTableForAllProfiles[feature[0]].chartData[1].push({
                axisY: item.estimate,
                axisX: index,
              });
            });
          }

          // Frequent item chips / bedge
          propertyPanelData[feature[0]][iteration] = tempFeatureValues.frequentItems.items.reduce((acc, item) => {
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
              propertyPanelData[feature[0]][0] = tempFeatureValues.numberSummary.histogram.counts.reduce(
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
                featureDataForTableForAllProfiles[feature[0]].chartData[0].push({
                  axisY: count,
                  axisX: index,
                });
              });
            }

            // Frequent item chips / bedge
            featureDataForTableForAllProfiles[feature[0]].frequentItemsElemString[iteration] = "No data to show";
          } else {
            featureDataForTableForAllProfiles[feature[0]].frequentItemsElemString[iteration] = "No data to show";
            featureDataForTableForAllProfiles[feature[0]].chartData[iteration] = [];
            propertyPanelData[feature[0]] = [];
          }
          if (tempFeatureValues.referenceNumberSummary) {
            tempFeatureValues.referenceNumberSummary.histogram.counts.slice(0, 30).forEach((count, index) => {
              featureDataForTableForAllProfiles[feature[0]].chartData[1].push({
                axisY: count,
                axisX: index,
              });
            });
          } 
        }
        iteration += 1;
      });
    });
    $featureCount.html(Object.values(featureDataForTableForAllProfiles).length);
  }

  function updateTableMessage(message) {
    $tableMessage.find("p").html(message);
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
    $compareProfile.removeClass("d-none");
  }

  function hideDataVisibility() {
    $tableMessage.removeClass("d-none");
    $sidebarContent.addClass("d-none");
    $tableContent.addClass("d-none");
    $compareProfile.removeClass("d-none");
  }

  function loadFile(inputId, jsonForm) {
    profiles = [];
    isActiveInferredType = {};
    $featureFilterInput.html("");
    $sidebarFeatureNameList.html("");
    $tableBody.html("");
    $(".form-check-input").prop("checked", true);
    if (typeof window.FileReader !== "function") {
      updateTableMessage(MESSAGES.error.fileAPINotSupported);
      return;
    }
    dataForRead = {};
    featureDataForTableForAllProfiles = {};
    numOfProfilesBasedOnType = {};

    handleClosePropertyPanel();
    const input = document.getElementById(inputId);
    if (!input) {
      updateTableMessage(MESSAGES.error.noInputElementFound);
    } else if (!input.files) {
      updateTableMessage(MESSAGES.error.noBrowserSupport);
    } else if (!input.files[0]) {
      updateTableMessage(MESSAGES.error.noFileSelected);
    } else {
      const file = input.files[0];
      const fr = new FileReader();
      fr.onload = (e) => receivedText(e, jsonForm, inputId);
      fr.readAsText(file);
    }
  }

  function receivedText(e, jsonForm, inputId) {
    const lines = e.target.result;
    let data = JSON.parse(lines);
    if (inputId === "file-input") {
      jsonData = data;
      referenceJsonData = undefined
    } else {
      referenceJsonData = data
    }

    mapProfileDataToReadData(jsonData, dataForRead, referenceJsonData);
    if (checkJSONValidityForSingleProfile(data) || checkJSONValidityForMultiProfile(data)) {
      $(".compare-select").addClass("d-none");
      $multiProfileWrap.removeClass("d-none");
      selectedProfiles[0] = "0";
      $tableBody.html("");
      updateHtmlElementValues();
      renderList();
      showDataVisibility();
      jsonForm.trigger("reset");
    } else {
      $tableBody.html("");
      updateTableMessage(MESSAGES.error.invalidJSONFile);
      hideDataVisibility();
      return;
    }
  }

  $burgerIcon.on("click", function () {
    $filterOptions.removeClass("d-none");
    $burgerIcon.addClass("d-none");
    $notClickableBurgerIcon.removeClass("d-none");
  });

  $filterCloseIcon.on("click", function () {
    $filterOptions.addClass("d-none");
    $burgerIcon.removeClass("d-none");
    $notClickableBurgerIcon.addClass("d-none");
  });

  $closeIcon.on("click", function () {
    $signUpText.addClass("d-none");
  });

  $(document).on("click", ".js-list-group-item span", function (e) {
    const listItem = $("li>span"),
      listItemIndex = listItem.index(e.target),
      $listItemDot = $(".wl_list-item-dot")[listItemIndex],
      $arrowIcon = $(".wl_arrow-icon")[listItemIndex];

    $(".wl_list-item-dot").removeClass("d-none");
    $(".wl_arrow-icon").addClass("d-none");
    $($arrowIcon).removeClass("d-none");
    $($listItemDot).addClass("d-none");
  });

  // Bind event listeners
  $fileInput.on("change", () => loadFile("file-input", $jsonForm));
  $referencefileInput.on("change", () => loadFile("reference-file-input", $referenceJsonForm));

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