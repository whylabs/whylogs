function registerHandlebarHelperFunctions() {
  //helper fun
  function fixNumberTo(number, decimals = 3) {
    return parseFloat(number).toFixed(decimals);
  }

  Handlebars.registerHelper("json", function (context) {
    return JSON.stringify(context);
  });

  Handlebars.registerHelper("inferredType", function (column) {
    let infferedType = "";

    if (column.numberSummary) {
      if (column.numberSummary.isDiscrete) {
        infferedType = "Discrete";
      } else {
        infferedType = "Non-discrete";
      }
    } else {
      infferedType = "Unknown";
    }
    return infferedType;
  });

  Handlebars.registerHelper("frequentItems", function (column) {
    frequentItemsElemString = "";
    if (column.numberSummary && column.numberSummary.isDiscrete) {
      const slicedFrequentItems = column.frequentItems.items.slice(0, 5);
      for (let fi = 0; fi < slicedFrequentItems.length; fi++) {
        frequentItemsElemString +=
          '<span class="wl-table-cell__bedge">' + slicedFrequentItems[fi].jsonValue + "</span>";
      }
    } else {
      frequentItemsElemString += "No data to show";
    }
    return frequentItemsElemString;
  });

  Handlebars.registerHelper("totalCount", function (column) {
    if (column.numberSummary) {
      return column.numberSummary.count.toString();
    }

    return "-";
  });

  Handlebars.registerHelper("nullFraction", function (column) {
    nullRatio = column.schema.typeCounts.NULL ? column.schema.typeCounts.NULL : "0";

    return nullRatio;
  });

  Handlebars.registerHelper("estUniqValue", function (column) {
    estUniqueVal = column.uniqueCount ? fixNumberTo(column.uniqueCount.estimate) : "-";
    return estUniqueVal;
  });

  Handlebars.registerHelper("dataType", function (column) {
    return column.schema.inferredType.type;
  });

  Handlebars.registerHelper("dataTypeCount", function (column) {
    dataType = column.schema.inferredType.type;
    return column.schema.typeCounts[dataType];
  });

  Handlebars.registerHelper("mean", function (column) {
    if (column.numberSummary) {
      return fixNumberTo(column.numberSummary.mean);
    }
    return "-";
  });

  Handlebars.registerHelper("stddev", function (column) {
    if (column.numberSummary) {
      return fixNumberTo(column.numberSummary.stddev);
    }
    return "-";
  });

  Handlebars.registerHelper("min", function (column) {
    if (column.numberSummary && column.numberSummary.quantiles.quantileValues) {
      return fixNumberTo(column.numberSummary.quantiles.quantileValues[0]);
    }
    return "-";
  });

  Handlebars.registerHelper("firstQuantile", function (column) {
    if (column.numberSummary && column.numberSummary.quantiles.quantileValues) {
      return fixNumberTo(column.numberSummary.quantiles.quantileValues[3]);
    }
    return "-";
  });

  Handlebars.registerHelper("median", function (column) {
    if (column.numberSummary && column.numberSummary.quantiles.quantileValues) {
      return fixNumberTo(column.numberSummary.quantiles.quantileValues[4]);
    }
    return "-";
  });

  Handlebars.registerHelper("thirdQuantile", function (column) {
    if (column.numberSummary && column.numberSummary.quantiles.quantileValues) {
      return fixNumberTo(column.numberSummary.quantiles.quantileValues[5]);
    }
    return "-";
  });

  Handlebars.registerHelper("max", function (column) {
    if (column.numberSummary && column.numberSummary.quantiles.quantileValues) {
      return fixNumberTo(column.numberSummary.quantiles.quantileValues[8]);
    }
    return "-";
  });

  Handlebars.registerHelper("getTotalFeatureCount", function () {
    return Object.values(this.columns).length.toString() || "0";
  });

  Handlebars.registerHelper("getFeatureList", function () {
    let featureListHTML = "";

    Object.entries(this.columns).forEach((feature) => {
      let inferredType = "Unknown";
      if (feature[1].numberSummary) {
        if (feature[1].numberSummary.isDiscrete) {
          inferredType = "Discrete";
        } else {
          inferredType = "Non-discrete";
        }
      }

      featureListHTML += `<li class="list-group-item js-list-group-item" data-feature-name="${feature[0]}" data-inferred-type="${inferredType}"><span data-feature-name-id="${feature[0]}" >${feature[0]}</span></li>`;
    });

    return featureListHTML;
  });

  Handlebars.registerHelper("getDiscreteTypeCount", function () {
    let count = 0;

    Object.entries(this.columns).forEach((feature) => {
      if (feature[1].numberSummary && feature[1].numberSummary.isDiscrete === true) {
        count++;
      }
    });
    return count.toString();
  });

  Handlebars.registerHelper("getNonDiscreteTypeCount", function () {
    let count = 0;

    Object.entries(this.columns).forEach((feature) => {
      if (feature[1].numberSummary && feature[1].numberSummary.isDiscrete === false) {
        count++;
      }
    });
    return count;
  });

  Handlebars.registerHelper("getUnknownTypeCount", function () {
    let count = 0;

    Object.entries(this.columns).forEach((feature) => {
      if (!feature[1].numberSummary) {
        count++;
      }
    });
    return count;
  });

  Handlebars.registerHelper("getGraphHtml", function (column) {
    let data = [];
    if (column.numberSummary) {
      if (column.numberSummary.isDiscrete) {
        column.frequentItems.items.forEach((item, index) => {
          data.push({
            axisY: item.estimate,
            axisX: index,
          });
        });
      } else {
        column.numberSummary.histogram.counts.slice(0, 30).forEach((count, index) => {
          data.push({
            axisY: count,
            axisX: index,
          });
        });
      }
    } else {
      return '<span class="wl-table-cell__bedge-wrap">No data to show the chart</span>';
    }
    // Map data to use in chart

    const MARGIN = {
      TOP: 5,
      RIGHT: 5,
      BOTTOM: 5,
      LEFT: 55,
    };
    const SVG_WIDTH = 350;
    const SVG_HEIGHT = 75;
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
      .attr("fill", PRIMARY_COLOR_HEX);

    return svgEl._groups[0][0].outerHTML;
  });
}

function initHandlebarsTemplate() {
  // Replace this context with JSON from .py file
  const context = {{{profile_from_whylogs}}};
  // Config handlebars and pass data to HBS template
  const source = document.getElementById("entry-template").innerHTML;
  const template = Handlebars.compile(source);
  const html = template(context);
  const target = document.getElementById("generated-html");
  target.innerHTML = html;
}

function initWebsiteScripts() {
  // Target HTML elements
  const $discrete = document.getElementById("inferredDiscrete");
  const $nonDiscrete = document.getElementById("inferredNonDiscrete");
  const $unknown = document.getElementById("inferredUnknown");
  const $sidebarFeatureNameList = document.getElementById("feature-list");
  const $featureSearch = document.getElementById("wl__feature-search");
  const $tableBody = document.getElementById("table-body");

  // Global variables
  const activeTypes = {
    discrete: true,
    "non-discrete": true,
    unknown: true,
  };
  let searchString = "";

  function handleSearch() {
    const featureListChildren = $sidebarFeatureNameList.children;
    const tableBodyChildren = $tableBody.children;

    for (let i = 0; i < tableBodyChildren.length; i++) {
      const type = tableBodyChildren[i].dataset.inferredType.toLowerCase();
      const name = tableBodyChildren[i].dataset.featureName.toLowerCase();

      if (activeTypes[type] && name.startsWith(searchString)) {
        tableBodyChildren[i].style.display = "";
      } else {
        tableBodyChildren[i].style.display = "none";
      }
    }

    for (let i = 0; i < featureListChildren.length; i++) {
      const name = featureListChildren[i].dataset.featureName.toLowerCase();
      const type = featureListChildren[i].dataset.inferredType.toLowerCase();

      if (activeTypes[type] && name.startsWith(searchString)) {
        featureListChildren[i].style.display = "";
      } else {
        featureListChildren[i].style.display = "none";
      }
    }
  }

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

  $featureSearch.addEventListener(
    "keyup",
    debounce((event) => {
      searchString = event.target.value.toLowerCase();
      handleSearch();
    }, 100),
  );

  $discrete.addEventListener("change", (event) => {
    if (event.currentTarget.checked) {
      activeTypes["discrete"] = true;
    } else {
      activeTypes["discrete"] = false;
    }
    handleSearch();
  });

  $nonDiscrete.addEventListener("change", (event) => {
    if (event.currentTarget.checked) {
      activeTypes["non-discrete"] = true;
    } else {
      activeTypes["non-discrete"] = false;
    }
    handleSearch();
  });

  $unknown.addEventListener("change", (event) => {
    if (event.currentTarget.checked) {
      activeTypes["unknown"] = true;
    } else {
      activeTypes["unknown"] = false;
    }
    handleSearch();
  });
}

function openPropertyPanel(column, infType) {
  const feature = JSON.parse(column);
  let propertyPanelData = [];

  if (feature.numberSummary) {
    if (feature.numberSummary.isDiscrete) {
      propertyPanelData = feature.frequentItems.items.reduce((acc, item) => {
        acc.push({
          value: item.jsonValue,
          count: item.estimate,
        });
        return acc;
      }, []);
    } else {
      propertyPanelData = feature.numberSummary.histogram.counts.reduce((acc, value, index) => {
        acc.push({
          value: value,
          count: feature.numberSummary.histogram.bins[index],
        });
        return acc;
      }, []);
    }
  }
  let chipString = "";
  const chipElement = (chip) => `<span class="wl-table-cell__bedge">${chip}</span>`;
  const chipElementTableData = (value) => `<td class="wl-property-panel__table-td" >${chipElement(value)}</td>`;
  const chipElementEstimation = (count) =>
    `<td class="wl-property-panel__table-td wl-property-panel__table-td-profile" >${count}</td>`;

  propertyPanelData.forEach((item) => {
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

function handleClosePropertyPanel() {
  $(".wl-property-panel").removeClass("wl-property-panel--open");
  $(".wl-table-wrap").removeClass("wl-table-wrap--narrow");
  $(".wl-property-panel__frequent-items").html("");
}

// Invoke functions -- keep in mind invokation order
registerHandlebarHelperFunctions();
initHandlebarsTemplate();
initWebsiteScripts();
