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
  const context = {
    properties: {
      schemaMajorVersion: 1,
      schemaMinorVersion: 1,
      sessionId: "d78c144f-a15a-4fd2-b1f6-11410afb55b1",
      sessionTimestamp: "1608772592814",
      dataTimestamp: "1608768000000",
      tags: {
        Name: "lending_club_credit_model",
      },
      metadata: {},
    },
    columns: {
      percent_bc_gt_75: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2278625",
            NULL: "21348",
          },
        },
        numberSummary: {
          count: "2278625",
          min: -391.2542574444356,
          max: 559.7833976194473,
          mean: 43.413165727438184,
          stddev: 66.80123119450013,
          histogram: {
            start: -391.2542724609375,
            end: 559.7834422088074,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "96",
              "128",
              "5120",
              "3072",
              "20480",
              "38912",
              "108544",
              "945153",
              "395264",
              "271360",
              "188416",
              "112640",
              "73728",
              "51200",
              "25600",
              "21504",
              "5120",
              "4096",
              "8192",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 559.7833862304688,
            min: -391.2542724609375,
            bins: [
              -391.2542724609375, -359.55301530527936, -327.85175814962116, -296.150500993963, -264.4492438383048,
              -232.74798668264668, -201.04672952698854, -169.34547237133037, -137.6442152156722, -105.94295806001406,
              -74.24170090435587, -42.540443748697726, -10.839186593039585, 20.862070562618612, 52.56332771827675,
              84.26458487393495, 115.96584202959309, 147.66709918525123, 179.36835634090937, 211.06961349656763,
              242.77087065222577, 274.4721278078839, 306.17338496354205, 337.8746421192002, 369.57589927485833,
              401.2771564305166, 432.9784135861747, 464.67967074183287, 496.380927897491, 528.0821850531491,
              559.7834422088074,
            ],
            n: "2278625",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 1634287.6306155797,
            upper: 1660192.683650335,
            lower: 1608783.686737209,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -391.2542724609375, -79.57911682128906, -24.66634750366211, 0.0, 22.582258224487305, 73.6591796875,
              179.75942993164062, 270.8111877441406, 559.7833862304688,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "586277",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "84079",
                value: 100.0,
                rank: 1,
              },
              {
                estimate: "78368",
                value: 50.0,
                rank: 2,
              },
              {
                estimate: "73660",
                value: 33.3,
                rank: 3,
              },
              {
                estimate: "72942",
                value: 66.7,
                rank: 4,
              },
              {
                estimate: "70855",
                value: 75.0,
                rank: 5,
              },
              {
                estimate: "70796",
                value: 25.0,
                rank: 6,
              },
              {
                estimate: "70792",
                value: 20.0,
                rank: 7,
              },
              {
                estimate: "70008",
                value: 40.0,
                rank: 8,
              },
              {
                estimate: "69584",
                value: 60.0,
                rank: 9,
              },
              {
                estimate: "69556",
                value: 42.5194263658,
                rank: 10,
              },
              {
                estimate: "69556",
                value: -5.8331921583,
                rank: 11,
              },
              {
                estimate: "69556",
                value: 60.7808005537,
                rank: 12,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "586277",
              jsonValue: "0.0",
            },
            {
              estimate: "84079",
              jsonValue: "100.0",
            },
            {
              estimate: "78368",
              jsonValue: "50.0",
            },
            {
              estimate: "73660",
              jsonValue: "33.3",
            },
            {
              estimate: "72942",
              jsonValue: "66.7",
            },
            {
              estimate: "70855",
              jsonValue: "75.0",
            },
            {
              estimate: "70796",
              jsonValue: "25.0",
            },
            {
              estimate: "70792",
              jsonValue: "20.0",
            },
            {
              estimate: "70008",
              jsonValue: "40.0",
            },
            {
              estimate: "69584",
              jsonValue: "60.0",
            },
            {
              estimate: "69556",
              jsonValue: "42.5194263658",
            },
            {
              estimate: "69556",
              jsonValue: "-5.8331921583",
            },
            {
              estimate: "69556",
              jsonValue: "60.7808005537",
            },
          ],
        },
        uniqueCount: {
          estimate: 1621353.5415686322,
          upper: 1648029.6403225143,
          lower: 1595575.9290389523,
        },
      },
      mort_acc: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -45.765909728222105,
          max: 178.7958157068487,
          mean: 1.6283262970485282,
          stddev: 3.177217470604123,
          histogram: {
            start: -45.76591110229492,
            end: 178.7958400231369,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "7232",
              "104448",
              "2024453",
              "146432",
              "13312",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 178.7958221435547,
            min: -45.76591110229492,
            bins: [
              -45.76591110229492, -38.28051939811386, -30.795127693932802, -23.309735989751744, -15.824344285570682,
              -8.33895258138962, -0.8535608772085652, 6.631830826972497, 14.117222531153558, 21.602614235334613,
              29.088005939515682, 36.57339764369674, 44.05878934787779, 51.54418105205886, 59.029572756239915,
              66.51496446042098, 74.00035616460204, 81.4857478687831, 88.97113957296415, 96.45653127714522,
              103.94192298132629, 111.42731468550733, 118.9127063896884, 126.39809809386946, 133.8834897980505,
              141.36888150223157, 148.85427320641264, 156.3396649105937, 163.82505661477475, 171.31044831895582,
              178.7958400231369,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 1301540.1438671767,
            upper: 1322164.2991244234,
            lower: 1281235.2259402084,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -45.76591110229492, -3.3916380405426025, -0.7845730781555176, 0.0, 0.0660712942481041, 2.4375855922698975,
              7.778087139129639, 13.334249496459961, 178.7958221435547,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "921264",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "72077",
                value: 1.0,
                rank: 1,
              },
              {
                estimate: "69085",
                value: 2.0,
                rank: 2,
              },
              {
                estimate: "65015",
                value: 3.0,
                rank: 3,
              },
              {
                estimate: "62052",
                value: 4.0,
                rank: 4,
              },
              {
                estimate: "59222",
                value: 5.0,
                rank: 5,
              },
              {
                estimate: "57374",
                value: 6.0,
                rank: 6,
              },
              {
                estimate: "55957",
                value: 7.0,
                rank: 7,
              },
              {
                estimate: "55474",
                value: 8.0,
                rank: 8,
              },
              {
                estimate: "55389",
                value: 9.0,
                rank: 9,
              },
              {
                estimate: "55182",
                value: 10.0,
                rank: 10,
              },
              {
                estimate: "55176",
                value: 0.6794679256,
                rank: 11,
              },
              {
                estimate: "55176",
                value: 12.3646993997,
                rank: 12,
              },
              {
                estimate: "55176",
                value: 1.3748166459,
                rank: 13,
              },
              {
                estimate: "55176",
                value: 0.9720460801,
                rank: 14,
              },
              {
                estimate: "55176",
                value: 2.0124099981,
                rank: 15,
              },
              {
                estimate: "55176",
                value: 3.2067749395,
                rank: 16,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "921264",
              jsonValue: "0.0",
            },
            {
              estimate: "72077",
              jsonValue: "1.0",
            },
            {
              estimate: "69085",
              jsonValue: "2.0",
            },
            {
              estimate: "65015",
              jsonValue: "3.0",
            },
            {
              estimate: "62052",
              jsonValue: "4.0",
            },
            {
              estimate: "59222",
              jsonValue: "5.0",
            },
            {
              estimate: "57374",
              jsonValue: "6.0",
            },
            {
              estimate: "55957",
              jsonValue: "7.0",
            },
            {
              estimate: "55474",
              jsonValue: "8.0",
            },
            {
              estimate: "55389",
              jsonValue: "9.0",
            },
            {
              estimate: "55182",
              jsonValue: "10.0",
            },
            {
              estimate: "55176",
              jsonValue: "0.6794679256",
            },
            {
              estimate: "55176",
              jsonValue: "12.3646993997",
            },
            {
              estimate: "55176",
              jsonValue: "1.3748166459",
            },
            {
              estimate: "55176",
              jsonValue: "0.9720460801",
            },
            {
              estimate: "55176",
              jsonValue: "2.0124099981",
            },
            {
              estimate: "55176",
              jsonValue: "3.2067749395",
            },
          ],
        },
        uniqueCount: {
          estimate: 1348821.379570034,
          upper: 1371013.511884339,
          lower: 1327376.7075704492,
        },
      },
      fico_range_high: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -2855.052555679892,
          max: 4513.458869706325,
          mean: 700.6765048728034,
          stddev: 699.4583625427612,
          histogram: {
            start: -2855.052490234375,
            end: 4513.459435720898,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "1024",
              "4160",
              "3072",
              "16384",
              "34816",
              "68610",
              "117760",
              "180224",
              "246784",
              "284673",
              "396289",
              "281601",
              "237568",
              "176128",
              "112640",
              "73728",
              "30720",
              "16384",
              "5120",
              "4096",
              "0",
              "4096",
              "4096",
              "0",
              "0",
              "0",
            ],
            max: 4513.458984375,
            min: -2855.052490234375,
            bins: [
              -2855.052490234375, -2609.435426035866, -2363.818361837357, -2118.201297638848, -1872.5842334403387,
              -1626.9671692418294, -1381.3501050433204, -1135.7330408448113, -890.1159766463022, -644.4989124477929,
              -398.88184824928385, -153.26478405077478, 92.35228014773429, 337.96934434624336, 583.5864085447524,
              829.2034727432615, 1074.8205369417706, 1320.43760114028, 1566.0546653387892, 1811.6717295372982,
              2057.2887937358073, 2302.9058579343164, 2548.5229221328254, 2794.1399863313345, 3039.7570505298436,
              3285.3741147283527, 3530.9911789268617, 3776.608243125371, 4022.22530732388, 4267.842371522389,
              4513.459435720898,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2185936.3904579817,
            upper: 2220596.4294129484,
            lower: 2151813.192687429,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -2855.052490234375, -898.5636596679688, -453.0279846191406, 248.06846618652344, 698.1520385742188,
              1160.34326171875, 1880.546875, 2375.028564453125, 4513.458984375,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "92963",
                value: 669.0,
                rank: 0,
              },
              {
                estimate: "92558",
                value: 674.0,
                rank: 1,
              },
              {
                estimate: "91325",
                value: 684.0,
                rank: 2,
              },
              {
                estimate: "91165",
                value: 664.0,
                rank: 3,
              },
              {
                estimate: "90485",
                value: 679.0,
                rank: 4,
              },
              {
                estimate: "89208",
                value: 689.0,
                rank: 5,
              },
              {
                estimate: "88999",
                value: 694.0,
                rank: 6,
              },
              {
                estimate: "88518",
                value: 699.0,
                rank: 7,
              },
              {
                estimate: "88172",
                value: 714.0,
                rank: 8,
              },
              {
                estimate: "88003",
                value: 709.0,
                rank: 9,
              },
              {
                estimate: "87574",
                value: 704.0,
                rank: 10,
              },
              {
                estimate: "87155",
                value: 1331.2090959099,
                rank: 11,
              },
              {
                estimate: "87155",
                value: 587.8335439165,
                rank: 12,
              },
              {
                estimate: "87155",
                value: 408.6790288548,
                rank: 13,
              },
              {
                estimate: "87155",
                value: 522.7479080058,
                rank: 14,
              },
              {
                estimate: "87155",
                value: 344.1920689817,
                rank: 15,
              },
              {
                estimate: "87155",
                value: 258.2824972316,
                rank: 16,
              },
              {
                estimate: "87155",
                value: 195.6655415077,
                rank: 17,
              },
              {
                estimate: "87155",
                value: 575.1758276412,
                rank: 18,
              },
              {
                estimate: "87155",
                value: 526.2747205876,
                rank: 19,
              },
              {
                estimate: "87155",
                value: 461.5714421766,
                rank: 20,
              },
              {
                estimate: "87155",
                value: 60.823214834,
                rank: 21,
              },
              {
                estimate: "87155",
                value: 532.0651021018,
                rank: 22,
              },
              {
                estimate: "87155",
                value: 988.7425016351,
                rank: 23,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "92963",
              jsonValue: "669.0",
            },
            {
              estimate: "92558",
              jsonValue: "674.0",
            },
            {
              estimate: "91325",
              jsonValue: "684.0",
            },
            {
              estimate: "91165",
              jsonValue: "664.0",
            },
            {
              estimate: "90485",
              jsonValue: "679.0",
            },
            {
              estimate: "89208",
              jsonValue: "689.0",
            },
            {
              estimate: "88999",
              jsonValue: "694.0",
            },
            {
              estimate: "88518",
              jsonValue: "699.0",
            },
            {
              estimate: "88172",
              jsonValue: "714.0",
            },
            {
              estimate: "88003",
              jsonValue: "709.0",
            },
            {
              estimate: "87574",
              jsonValue: "704.0",
            },
            {
              estimate: "87155",
              jsonValue: "1331.2090959099",
            },
            {
              estimate: "87155",
              jsonValue: "587.8335439165",
            },
            {
              estimate: "87155",
              jsonValue: "408.6790288548",
            },
            {
              estimate: "87155",
              jsonValue: "522.7479080058",
            },
            {
              estimate: "87155",
              jsonValue: "344.1920689817",
            },
            {
              estimate: "87155",
              jsonValue: "258.2824972316",
            },
            {
              estimate: "87155",
              jsonValue: "195.6655415077",
            },
            {
              estimate: "87155",
              jsonValue: "575.1758276412",
            },
            {
              estimate: "87155",
              jsonValue: "526.2747205876",
            },
            {
              estimate: "87155",
              jsonValue: "461.5714421766",
            },
            {
              estimate: "87155",
              jsonValue: "60.823214834",
            },
            {
              estimate: "87155",
              jsonValue: "532.0651021018",
            },
            {
              estimate: "87155",
              jsonValue: "988.7425016351",
            },
          ],
        },
        uniqueCount: {
          estimate: 2211952.9572877297,
          upper: 2248346.1769124027,
          lower: 2176785.5093468702,
        },
      },
      total_rev_hi_lim: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -1740595.4128060343,
          max: 4412918.26551473,
          mean: 35527.63085640086,
          stddev: 63019.719692894614,
          histogram: {
            start: -1740595.375,
            end: 4412918.94129185,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "2048",
              "5184",
              "2106372",
              "168961",
              "13312",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 4412918.5,
            min: -1740595.375,
            bins: [
              -1740595.375, -1535478.231123605, -1330361.08724721, -1125243.943370815, -920126.79949442,
              -715009.655618025, -509892.5117416298, -304775.36786523485, -99658.22398883989, 105458.91988755506,
              310576.06376395, 515693.2076403452, 720810.3515167404, 925927.4953931351, 1131044.6392695303,
              1336161.783145925, 1541278.9270223202, 1746396.0708987154, 1951513.2147751101, 2156630.3586515053,
              2361747.5025279, 2566864.646404295, 2771981.7902806904, 2977098.9341570856, 3182216.078033481,
              3387333.221909875, 3592450.3657862702, 3797567.5096626654, 4002684.6535390606, 4207801.797415456,
              4412918.94129185,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2210721.888956405,
            upper: 2245775.2880951944,
            lower: 2176211.429422289,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -1740595.375, -52597.66015625, -17610.62890625, 5569.63818359375, 20897.396484375, 48350.15234375,
              134800.0, 280709.90625, 4412918.5,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "87256",
                value: 5685.3942092292,
                rank: 0,
              },
              {
                estimate: "87256",
                value: 16908.6709263284,
                rank: 1,
              },
              {
                estimate: "87256",
                value: 123101.6366046846,
                rank: 2,
              },
              {
                estimate: "87256",
                value: 58431.0772842884,
                rank: 3,
              },
              {
                estimate: "87256",
                value: 27395.5687731852,
                rank: 4,
              },
              {
                estimate: "87256",
                value: 54489.2384520939,
                rank: 5,
              },
              {
                estimate: "87256",
                value: 45317.0706252437,
                rank: 6,
              },
              {
                estimate: "87256",
                value: 11151.2967598341,
                rank: 7,
              },
              {
                estimate: "87256",
                value: 22832.2302796403,
                rank: 8,
              },
              {
                estimate: "87256",
                value: 20390.1795678253,
                rank: 9,
              },
              {
                estimate: "87256",
                value: 24235.6601743722,
                rank: 10,
              },
              {
                estimate: "87256",
                value: -15544.3570721421,
                rank: 11,
              },
              {
                estimate: "87256",
                value: -8591.6150834017,
                rank: 12,
              },
              {
                estimate: "87256",
                value: 46786.3783398763,
                rank: 13,
              },
              {
                estimate: "87256",
                value: -19680.658229866,
                rank: 14,
              },
              {
                estimate: "87256",
                value: 78807.8529456133,
                rank: 15,
              },
              {
                estimate: "87256",
                value: 106549.9679427431,
                rank: 16,
              },
              {
                estimate: "87256",
                value: -2024.509194196,
                rank: 17,
              },
              {
                estimate: "87256",
                value: 10833.6508450998,
                rank: 18,
              },
              {
                estimate: "87256",
                value: 26940.1220977895,
                rank: 19,
              },
              {
                estimate: "87256",
                value: 948.4863002732,
                rank: 20,
              },
              {
                estimate: "87256",
                value: 33725.7699028287,
                rank: 21,
              },
              {
                estimate: "87256",
                value: 47040.3615379091,
                rank: 22,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "87256",
              jsonValue: "5685.3942092292",
            },
            {
              estimate: "87256",
              jsonValue: "16908.6709263284",
            },
            {
              estimate: "87256",
              jsonValue: "123101.6366046846",
            },
            {
              estimate: "87256",
              jsonValue: "58431.0772842884",
            },
            {
              estimate: "87256",
              jsonValue: "27395.5687731852",
            },
            {
              estimate: "87256",
              jsonValue: "54489.2384520939",
            },
            {
              estimate: "87256",
              jsonValue: "45317.0706252437",
            },
            {
              estimate: "87256",
              jsonValue: "11151.2967598341",
            },
            {
              estimate: "87256",
              jsonValue: "22832.2302796403",
            },
            {
              estimate: "87256",
              jsonValue: "20390.1795678253",
            },
            {
              estimate: "87256",
              jsonValue: "24235.6601743722",
            },
            {
              estimate: "87256",
              jsonValue: "-15544.3570721421",
            },
            {
              estimate: "87256",
              jsonValue: "-8591.6150834017",
            },
            {
              estimate: "87256",
              jsonValue: "46786.3783398763",
            },
            {
              estimate: "87256",
              jsonValue: "-19680.658229866",
            },
            {
              estimate: "87256",
              jsonValue: "78807.8529456133",
            },
            {
              estimate: "87256",
              jsonValue: "106549.9679427431",
            },
            {
              estimate: "87256",
              jsonValue: "-2024.509194196",
            },
            {
              estimate: "87256",
              jsonValue: "10833.6508450998",
            },
            {
              estimate: "87256",
              jsonValue: "26940.1220977895",
            },
            {
              estimate: "87256",
              jsonValue: "948.4863002732",
            },
            {
              estimate: "87256",
              jsonValue: "33725.7699028287",
            },
            {
              estimate: "87256",
              jsonValue: "47040.3615379091",
            },
          ],
        },
        uniqueCount: {
          estimate: 2292458.1178612635,
          upper: 2330175.8873503534,
          lower: 2256010.7326441105,
        },
      },
      title: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2130182",
            NULL: "169791",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 12.0,
            upper: 12.0,
            lower: 12.0,
          },
          frequent: {
            items: [
              {
                value: "Debt consolidation",
                estimate: 1211625.0,
              },
              {
                value: "Credit card refinancing",
                estimate: 472018.0,
              },
              {
                value: "Home improvement",
                estimate: 155275.0,
              },
              {
                value: "Other",
                estimate: 130725.0,
              },
              {
                value: "Major purchase",
                estimate: 54656.0,
              },
              {
                value: "Business",
                estimate: 24162.0,
              },
              {
                value: "Medical expenses",
                estimate: 23485.0,
              },
              {
                value: "Car financing",
                estimate: 22194.0,
              },
              {
                value: "Vacation",
                estimate: 13310.0,
              },
              {
                value: "Moving and relocation",
                estimate: 12564.0,
              },
              {
                value: "Home buying",
                estimate: 8810.0,
              },
              {
                value: "Green loan",
                estimate: 1358.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "1211625",
              jsonValue: '"Debt consolidation"',
            },
            {
              estimate: "472018",
              jsonValue: '"Credit card refinancing"',
            },
            {
              estimate: "155275",
              jsonValue: '"Home improvement"',
            },
            {
              estimate: "130725",
              jsonValue: '"Other"',
            },
            {
              estimate: "54656",
              jsonValue: '"Major purchase"',
            },
            {
              estimate: "24162",
              jsonValue: '"Business"',
            },
            {
              estimate: "23485",
              jsonValue: '"Medical expenses"',
            },
            {
              estimate: "22194",
              jsonValue: '"Car financing"',
            },
            {
              estimate: "13310",
              jsonValue: '"Vacation"',
            },
            {
              estimate: "12564",
              jsonValue: '"Moving and relocation"',
            },
            {
              estimate: "8810",
              jsonValue: '"Home buying"',
            },
            {
              estimate: "1358",
              jsonValue: '"Green loan"',
            },
          ],
        },
        uniqueCount: {
          estimate: 12.000000327825557,
          upper: 12.000599478849342,
          lower: 12.0,
        },
      },
      num_tl_op_past_12m: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -35.45495126604918,
          max: 110.87832399562448,
          mean: 2.284560260141596,
          stddev: 3.594324384376031,
          histogram: {
            start: -35.454952239990234,
            end: 110.87833750384827,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "64",
              "10240",
              "100354",
              "1624066",
              "435201",
              "94208",
              "26624",
              "0",
              "9216",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 110.87832641601562,
            min: -35.454952239990234,
            bins: [
              -35.454952239990234, -30.577175915195618, -25.699399590401, -20.82162326560638, -15.943846940811763,
              -11.066070616017146, -6.188294291222526, -1.3105179664279092, 3.5672583583667077, 8.445034683161325,
              13.322811007955941, 18.20058733275056, 23.078363657545182, 27.9561399823398, 32.833916307134416,
              37.711692631929026, 42.58946895672365, 47.467245281518274, 52.34502160631288, 57.22279793110751,
              62.10057425590212, 66.97835058069674, 71.85612690549135, 76.73390323028597, 81.6116795550806,
              86.48945587987521, 91.36723220466983, 96.24500852946446, 101.12278485425907, 106.00056117905368,
              110.87833750384829,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 1759096.4784297294,
            upper: 1786982.3205799935,
            lower: 1731642.4543928562,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -35.454952239990234, -4.548508167266846, -1.2493984699249268, 0.0, 1.3706846237182617, 3.490288257598877,
              8.851482391357422, 15.105705261230469, 110.87832641601562,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "387590",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "99113",
                value: 1.0,
                rank: 1,
              },
              {
                estimate: "96545",
                value: 2.0,
                rank: 2,
              },
              {
                estimate: "91268",
                value: 3.0,
                rank: 3,
              },
              {
                estimate: "86370",
                value: 4.0,
                rank: 4,
              },
              {
                estimate: "81195",
                value: 5.0,
                rank: 5,
              },
              {
                estimate: "79094",
                value: 6.0,
                rank: 6,
              },
              {
                estimate: "78020",
                value: 7.0,
                rank: 7,
              },
              {
                estimate: "77164",
                value: 8.0,
                rank: 8,
              },
              {
                estimate: "76724",
                value: 9.0,
                rank: 9,
              },
              {
                estimate: "76698",
                value: 10.0,
                rank: 10,
              },
              {
                estimate: "76496",
                value: 12.0,
                rank: 11,
              },
              {
                estimate: "76478",
                value: 0.6159261079,
                rank: 12,
              },
              {
                estimate: "76478",
                value: 0.9020216815000001,
                rank: 13,
              },
              {
                estimate: "76478",
                value: 1.2754734532,
                rank: 14,
              },
              {
                estimate: "76478",
                value: 5.8589203843,
                rank: 15,
              },
              {
                estimate: "76478",
                value: 5.4033900052,
                rank: 16,
              },
              {
                estimate: "76478",
                value: 1.5023325067000002,
                rank: 17,
              },
              {
                estimate: "76478",
                value: 4.2384134002,
                rank: 18,
              },
              {
                estimate: "76478",
                value: 5.3896316653,
                rank: 19,
              },
              {
                estimate: "76478",
                value: 2.2551336167000002,
                rank: 20,
              },
              {
                estimate: "76478",
                value: 8.6296817058,
                rank: 21,
              },
              {
                estimate: "76478",
                value: -0.6498430896,
                rank: 22,
              },
              {
                estimate: "76478",
                value: 6.1900588194,
                rank: 23,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "387590",
              jsonValue: "0.0",
            },
            {
              estimate: "99113",
              jsonValue: "1.0",
            },
            {
              estimate: "96545",
              jsonValue: "2.0",
            },
            {
              estimate: "91268",
              jsonValue: "3.0",
            },
            {
              estimate: "86370",
              jsonValue: "4.0",
            },
            {
              estimate: "81195",
              jsonValue: "5.0",
            },
            {
              estimate: "79094",
              jsonValue: "6.0",
            },
            {
              estimate: "78020",
              jsonValue: "7.0",
            },
            {
              estimate: "77164",
              jsonValue: "8.0",
            },
            {
              estimate: "76724",
              jsonValue: "9.0",
            },
            {
              estimate: "76698",
              jsonValue: "10.0",
            },
            {
              estimate: "76496",
              jsonValue: "12.0",
            },
            {
              estimate: "76478",
              jsonValue: "0.6159261079",
            },
            {
              estimate: "76478",
              jsonValue: "0.9020216815",
            },
            {
              estimate: "76478",
              jsonValue: "1.2754734532",
            },
            {
              estimate: "76478",
              jsonValue: "5.8589203843",
            },
            {
              estimate: "76478",
              jsonValue: "5.4033900052",
            },
            {
              estimate: "76478",
              jsonValue: "1.5023325067",
            },
            {
              estimate: "76478",
              jsonValue: "4.2384134002",
            },
            {
              estimate: "76478",
              jsonValue: "5.3896316653",
            },
            {
              estimate: "76478",
              jsonValue: "2.2551336167",
            },
            {
              estimate: "76478",
              jsonValue: "8.6296817058",
            },
            {
              estimate: "76478",
              jsonValue: "-0.6498430896",
            },
            {
              estimate: "76478",
              jsonValue: "6.1900588194",
            },
          ],
        },
        uniqueCount: {
          estimate: 1871215.3740029878,
          upper: 1902002.444698496,
          lower: 1841465.2524940402,
        },
      },
      num_actv_bc_tl: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -110.1564253550464,
          max: 197.63873348548097,
          mean: 4.1345701818659615,
          stddev: 6.25618591588222,
          histogram: {
            start: -110.15642547607422,
            end: 197.63875267402955,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "1024",
              "0",
              "15424",
              "1093635",
              "1041410",
              "119808",
              "16384",
              "8192",
              "0",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 197.63873291015625,
            min: -110.15642547607422,
            bins: [
              -110.15642547607422, -99.89658620440409, -89.63674693273397, -79.37690766106384, -69.11706838939372,
              -58.85722911772359, -48.59738984605346, -38.33755057438334, -28.07771130271321, -17.81787203104308,
              -7.5580327593729635, 2.7018065122971677, 12.961645783967299, 23.22148505563743, 33.48132432730753,
              43.741163598977664, 54.001002870647795, 64.26084214231793, 74.52068141398806, 84.78052068565816,
              95.04035995732829, 105.30019922899842, 115.56003850066855, 125.81987777233869, 136.07971704400882,
              146.33955631567892, 156.59939558734908, 166.85923485901918, 177.11907413068928, 187.37891340235944,
              197.63875267402955,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2190104.9282808304,
            upper: 2224831.1243401607,
            lower: 2155916.5990659585,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -110.15642547607422, -6.909045696258545, -2.3071634769439697, 0.7751893997192383, 2.889927625656128,
              6.012236595153809, 14.366846084594727, 25.854978561401367, 197.63873291015625,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "103143",
                value: 2.0,
                rank: 0,
              },
              {
                estimate: "101398",
                value: 3.0,
                rank: 1,
              },
              {
                estimate: "98985",
                value: 4.0,
                rank: 2,
              },
              {
                estimate: "95576",
                value: 1.0,
                rank: 3,
              },
              {
                estimate: "93493",
                value: 5.0,
                rank: 4,
              },
              {
                estimate: "89600",
                value: 6.0,
                rank: 5,
              },
              {
                estimate: "87513",
                value: 7.0,
                rank: 6,
              },
              {
                estimate: "85727",
                value: 8.0,
                rank: 7,
              },
              {
                estimate: "85367",
                rank: 8,
                value: 0.0,
              },
              {
                estimate: "84644",
                value: 9.0,
                rank: 9,
              },
              {
                estimate: "84354",
                value: 10.0,
                rank: 10,
              },
              {
                estimate: "84154",
                value: 11.0,
                rank: 11,
              },
              {
                estimate: "83956",
                value: 6.768945144,
                rank: 12,
              },
              {
                estimate: "83956",
                value: 0.4412846791,
                rank: 13,
              },
              {
                estimate: "83956",
                value: 3.725183007,
                rank: 14,
              },
              {
                estimate: "83956",
                value: 7.8704828306,
                rank: 15,
              },
              {
                estimate: "83956",
                value: -1.3972398217,
                rank: 16,
              },
              {
                estimate: "83956",
                value: 5.5440374859,
                rank: 17,
              },
              {
                estimate: "83956",
                value: 5.9427740234,
                rank: 18,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "103143",
              jsonValue: "2.0",
            },
            {
              estimate: "101398",
              jsonValue: "3.0",
            },
            {
              estimate: "98985",
              jsonValue: "4.0",
            },
            {
              estimate: "95576",
              jsonValue: "1.0",
            },
            {
              estimate: "93493",
              jsonValue: "5.0",
            },
            {
              estimate: "89600",
              jsonValue: "6.0",
            },
            {
              estimate: "87513",
              jsonValue: "7.0",
            },
            {
              estimate: "85727",
              jsonValue: "8.0",
            },
            {
              estimate: "85367",
              jsonValue: "0.0",
            },
            {
              estimate: "84644",
              jsonValue: "9.0",
            },
            {
              estimate: "84354",
              jsonValue: "10.0",
            },
            {
              estimate: "84154",
              jsonValue: "11.0",
            },
            {
              estimate: "83956",
              jsonValue: "6.768945144",
            },
            {
              estimate: "83956",
              jsonValue: "0.4412846791",
            },
            {
              estimate: "83956",
              jsonValue: "3.725183007",
            },
            {
              estimate: "83956",
              jsonValue: "7.8704828306",
            },
            {
              estimate: "83956",
              jsonValue: "-1.3972398217",
            },
            {
              estimate: "83956",
              jsonValue: "5.5440374859",
            },
            {
              estimate: "83956",
              jsonValue: "5.9427740234",
            },
          ],
        },
        uniqueCount: {
          estimate: 2150631.3690168047,
          upper: 2186015.6657245937,
          lower: 2116438.8621369945,
        },
      },
      num_il_tl: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -202.46087183872032,
          max: 365.1051778313768,
          mean: 8.689365695619985,
          stddev: 13.77684568340517,
          histogram: {
            start: -202.46087646484375,
            end: 365.1052000847351,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "2112",
              "27648",
              "1168388",
              "886785",
              "154624",
              "38912",
              "17408",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 365.10516357421875,
            min: -202.46087646484375,
            bins: [
              -202.46087646484375, -183.54200724652446, -164.62313802820518, -145.70426880988586, -126.78539959156657,
              -107.86653037324729, -88.94766115492799, -70.02879193660868, -51.1099227182894, -32.19105349997011,
              -13.272184281650823, 5.646684936668493, 24.56555415498778, 43.48442337330707, 62.40329259162638,
              81.32216180994567, 100.24103102826496, 119.15990024658424, 138.07876946490353, 156.99763868322282,
              175.9165079015421, 194.83537711986145, 213.75424633818074, 232.67311555650002, 251.5919847748193,
              270.5108539931386, 289.4297232114579, 308.34859242977717, 327.2674616480965, 346.1863308664158,
              365.1052000847351,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2198740.8127224813,
            upper: 2233604.0652584764,
            lower: 2164417.5518723167,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -202.46087646484375, -16.494794845581055, -4.612861633300781, 1.0879238843917847, 5.17725133895874,
              12.537430763244629, 33.12790298461914, 59.39145278930664, 365.10516357421875,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "95776",
                value: 4.0,
                rank: 0,
              },
              {
                estimate: "95597",
                value: 5.0,
                rank: 1,
              },
              {
                estimate: "95469",
                value: 6.0,
                rank: 2,
              },
              {
                estimate: "95125",
                value: 8.0,
                rank: 3,
              },
              {
                estimate: "95041",
                value: 3.0,
                rank: 4,
              },
              {
                estimate: "94988",
                value: 2.0,
                rank: 5,
              },
              {
                estimate: "94295",
                value: 7.0,
                rank: 6,
              },
              {
                estimate: "93727",
                value: 10.0,
                rank: 7,
              },
              {
                estimate: "93145",
                value: 1.0,
                rank: 8,
              },
              {
                estimate: "92790",
                value: 9.0,
                rank: 9,
              },
              {
                estimate: "91923",
                rank: 10,
                value: 0.0,
              },
              {
                estimate: "91891",
                value: 5.0995822847,
                rank: 11,
              },
              {
                estimate: "91891",
                value: -2.7845658165,
                rank: 12,
              },
              {
                estimate: "91891",
                value: -0.44467743390000003,
                rank: 13,
              },
              {
                estimate: "91891",
                value: 10.3186358578,
                rank: 14,
              },
              {
                estimate: "91891",
                value: 9.4855483236,
                rank: 15,
              },
              {
                estimate: "91891",
                value: 2.168060862,
                rank: 16,
              },
              {
                estimate: "91891",
                value: 2.0795412272,
                rank: 17,
              },
              {
                estimate: "91891",
                value: -2.5561939281,
                rank: 18,
              },
              {
                estimate: "91891",
                value: 3.2451017849,
                rank: 19,
              },
              {
                estimate: "91891",
                value: 10.5717436927,
                rank: 20,
              },
              {
                estimate: "91891",
                value: 5.7475785192,
                rank: 21,
              },
              {
                estimate: "91891",
                value: 1.1876549233,
                rank: 22,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "95776",
              jsonValue: "4.0",
            },
            {
              estimate: "95597",
              jsonValue: "5.0",
            },
            {
              estimate: "95469",
              jsonValue: "6.0",
            },
            {
              estimate: "95125",
              jsonValue: "8.0",
            },
            {
              estimate: "95041",
              jsonValue: "3.0",
            },
            {
              estimate: "94988",
              jsonValue: "2.0",
            },
            {
              estimate: "94295",
              jsonValue: "7.0",
            },
            {
              estimate: "93727",
              jsonValue: "10.0",
            },
            {
              estimate: "93145",
              jsonValue: "1.0",
            },
            {
              estimate: "92790",
              jsonValue: "9.0",
            },
            {
              estimate: "91923",
              jsonValue: "0.0",
            },
            {
              estimate: "91891",
              jsonValue: "5.0995822847",
            },
            {
              estimate: "91891",
              jsonValue: "-2.7845658165",
            },
            {
              estimate: "91891",
              jsonValue: "-0.4446774339",
            },
            {
              estimate: "91891",
              jsonValue: "10.3186358578",
            },
            {
              estimate: "91891",
              jsonValue: "9.4855483236",
            },
            {
              estimate: "91891",
              jsonValue: "2.168060862",
            },
            {
              estimate: "91891",
              jsonValue: "2.0795412272",
            },
            {
              estimate: "91891",
              jsonValue: "-2.5561939281",
            },
            {
              estimate: "91891",
              jsonValue: "3.2451017849",
            },
            {
              estimate: "91891",
              jsonValue: "10.5717436927",
            },
            {
              estimate: "91891",
              jsonValue: "5.7475785192",
            },
            {
              estimate: "91891",
              jsonValue: "1.1876549233",
            },
          ],
        },
        uniqueCount: {
          estimate: 2174436.498822134,
          upper: 2210212.4608744974,
          lower: 2139865.5184036363,
        },
      },
      collection_recovery_fee: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -9219.366339877792,
          max: 13031.727538901332,
          mean: 31.50361789108916,
          stddev: 217.1177171173994,
          histogram: {
            start: -9219.3662109375,
            end: 13031.728842235254,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "64",
              "0",
              "0",
              "0",
              "0",
              "6146",
              "2241539",
              "34816",
              "13312",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 13031.7275390625,
            min: -9219.3662109375,
            bins: [
              -9219.3662109375, -8477.663042498409, -7735.9598740593165, -6994.256705620224, -6252.553537181133,
              -5510.850368742042, -4769.147200302949, -4027.444031863857, -3285.740863424766, -2544.0376949856745,
              -1802.3345265465823, -1060.63135810749, -318.9281896683988, 422.77497877069254, 1164.4781472097857,
              1906.181315648877, 2647.8844840879683, 3389.5876525270596, 4131.290820966151, 4872.993989405244,
              5614.697157844335, 6356.400326283427, 7098.10349472252, 7839.806663161609, 8581.509831600702,
              9323.213000039796, 10064.916168478885, 10806.619336917978, 11548.322505357071, 12290.02567379616,
              13031.728842235254,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 196325.49000723474,
            upper: 199409.11410133156,
            lower: 193289.16746421627,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -9219.3662109375, -39.654232025146484, -0.0, 0.0, 0.0, 0.0, 137.99339294433594, 784.5069580078125,
              13031.7275390625,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "2066072",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "17905",
                value: 94.1214700534,
                rank: 1,
              },
              {
                estimate: "17075",
                value: 137.9934,
                rank: 2,
              },
              {
                estimate: "14947",
                value: 100.16922162,
                rank: 3,
              },
              {
                estimate: "13731",
                value: 106.2781400219,
                rank: 4,
              },
              {
                estimate: "13375",
                value: 91.4623323869,
                rank: 5,
              },
              {
                estimate: "8257",
                value: 593.4393592664,
                rank: 6,
              },
              {
                estimate: "8257",
                value: -533.8731166696,
                rank: 7,
              },
              {
                estimate: "8257",
                value: 15.1088911212,
                rank: 8,
              },
              {
                estimate: "8257",
                value: -35.3886747809,
                rank: 9,
              },
            ],
            longs: [],
          },
          isDiscrete: true,
        },
        frequentItems: {
          items: [
            {
              estimate: "2066072",
              jsonValue: "0.0",
            },
            {
              estimate: "17905",
              jsonValue: "94.1214700534",
            },
            {
              estimate: "17075",
              jsonValue: "137.9934",
            },
            {
              estimate: "14947",
              jsonValue: "100.16922162",
            },
            {
              estimate: "13731",
              jsonValue: "106.2781400219",
            },
            {
              estimate: "13375",
              jsonValue: "91.4623323869",
            },
            {
              estimate: "8257",
              jsonValue: "593.4393592664",
            },
            {
              estimate: "8257",
              jsonValue: "-533.8731166696",
            },
            {
              estimate: "8257",
              jsonValue: "15.1088911212",
            },
            {
              estimate: "8257",
              jsonValue: "-35.3886747809",
            },
          ],
        },
        uniqueCount: {
          estimate: 200013.01531396792,
          upper: 203303.82562262833,
          lower: 196833.04384107853,
        },
      },
      funded_amnt: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -132581.99877551044,
          max: 204312.51300272593,
          mean: 15277.396302010719,
          stddev: 19755.704923302987,
          histogram: {
            start: -132582.0,
            end: 204312.53605625156,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "64",
              "5120",
              "2048",
              "5120",
              "25600",
              "76802",
              "389121",
              "783360",
              "464897",
              "253953",
              "134144",
              "77824",
              "43008",
              "21504",
              "16384",
              "1024",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 204312.515625,
            min: -132582.0,
            bins: [
              -132582.0, -121352.18213145828, -110122.36426291656, -98892.54639437485, -87662.72852583312,
              -76432.9106572914, -65203.092788749695, -53973.27492020797, -42743.45705166625, -31513.639183124527,
              -20283.821314582805, -9054.003446041097, 2175.814422500611, 13405.632291042333, 24635.450159584056,
              35865.26802812578, 47095.0858966675, 58324.90376520922, 69554.72163375095, 80784.53950229267,
              92014.35737083439, 103244.17523937608, 114473.9931079178, 125703.81097645953, 136933.62884500122,
              148163.44671354297, 159393.26458208467, 170623.08245062642, 181852.9003191681, 193082.71818770986,
              204312.53605625156,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2192108.9919047626,
            upper: 2226866.993611971,
            lower: 2157889.3501376915,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -132582.0, -24665.02734375, -9032.1875, 3138.495361328125, 11086.48828125, 23863.794921875, 52732.8359375,
              78119.765625, 204312.515625,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "90404",
                value: 10000.0,
                rank: 0,
              },
              {
                estimate: "89466",
                value: 20000.0,
                rank: 1,
              },
              {
                estimate: "88853",
                value: 15000.0,
                rank: 2,
              },
              {
                estimate: "88411",
                value: 12000.0,
                rank: 3,
              },
              {
                estimate: "87508",
                value: 5000.0,
                rank: 4,
              },
              {
                estimate: "87166",
                value: 35000.0,
                rank: 5,
              },
              {
                estimate: "87100",
                value: 8000.0,
                rank: 6,
              },
              {
                estimate: "87077",
                value: 24677.9524178977,
                rank: 7,
              },
              {
                estimate: "87077",
                value: -4892.0849425014,
                rank: 8,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "90404",
              jsonValue: "10000.0",
            },
            {
              estimate: "89466",
              jsonValue: "20000.0",
            },
            {
              estimate: "88853",
              jsonValue: "15000.0",
            },
            {
              estimate: "88411",
              jsonValue: "12000.0",
            },
            {
              estimate: "87508",
              jsonValue: "5000.0",
            },
            {
              estimate: "87166",
              jsonValue: "35000.0",
            },
            {
              estimate: "87100",
              jsonValue: "8000.0",
            },
            {
              estimate: "87077",
              jsonValue: "24677.9524178977",
            },
            {
              estimate: "87077",
              jsonValue: "-4892.0849425014",
            },
          ],
        },
        uniqueCount: {
          estimate: 2222975.525929202,
          upper: 2259550.099664538,
          lower: 2187632.832123553,
        },
      },
      pub_rec: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -57.71478598328134,
          max: 136.45328891721343,
          mean: 0.24179890436067797,
          stddev: 0.9749386016105364,
          histogram: {
            start: -57.714786529541016,
            end: 136.45330649200898,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "2003013",
              "288768",
              "8192",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 136.4532928466797,
            min: -57.714786529541016,
            bins: [
              -57.714786529541016, -51.242516762156015, -44.770246994771014, -38.29797722738601, -31.825707460001016,
              -25.35343769261602, -18.88116792523102, -12.408898157846018, -5.936628390461017, 0.5356413769239836,
              7.007911144308977, 13.480180911693978, 19.95245067907898, 26.42472044646398, 32.89699021384898,
              39.36925998123398, 45.84152974861898, 52.31379951600398, 58.78606928338898, 65.25833905077398,
              71.73060881815897, 78.20287858554397, 84.67514835292897, 91.14741812031397, 97.61968788769897,
              104.09195765508397, 110.56422742246897, 117.03649718985397, 123.50876695723898, 129.98103672462398,
              136.45330649200898,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 402165.07330854674,
            upper: 408515.57591016917,
            lower: 395912.5186689806,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -57.714786529541016, -0.7090258002281189, -0.0, 0.0, 0.0, 0.0, 1.8508416414260864, 4.092914581298828,
              136.4532928466797,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "1884974",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "30183",
                value: 1.0,
                rank: 1,
              },
              {
                estimate: "18874",
                value: 2.0,
                rank: 2,
              },
              {
                estimate: "17421",
                value: 3.0,
                rank: 3,
              },
              {
                estimate: "16789",
                value: 5.0,
                rank: 4,
              },
              {
                estimate: "16750",
                value: 4.0,
                rank: 5,
              },
              {
                estimate: "16696",
                value: 6.0,
                rank: 6,
              },
              {
                estimate: "16572",
                value: 0.8235737897000001,
                rank: 7,
              },
              {
                estimate: "16572",
                value: 2.4428543598,
                rank: 8,
              },
              {
                estimate: "16572",
                value: 1.9984032217,
                rank: 9,
              },
              {
                estimate: "16572",
                value: 2.0442494285,
                rank: 10,
              },
              {
                estimate: "16572",
                value: 0.9954079545000001,
                rank: 11,
              },
              {
                estimate: "16572",
                value: 0.18611511930000002,
                rank: 12,
              },
              {
                estimate: "16572",
                value: -0.1400022918,
                rank: 13,
              },
              {
                estimate: "16572",
                value: 1.353710898,
                rank: 14,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "1884974",
              jsonValue: "0.0",
            },
            {
              estimate: "30183",
              jsonValue: "1.0",
            },
            {
              estimate: "18874",
              jsonValue: "2.0",
            },
            {
              estimate: "17421",
              jsonValue: "3.0",
            },
            {
              estimate: "16789",
              jsonValue: "5.0",
            },
            {
              estimate: "16750",
              jsonValue: "4.0",
            },
            {
              estimate: "16696",
              jsonValue: "6.0",
            },
            {
              estimate: "16572",
              jsonValue: "0.8235737897",
            },
            {
              estimate: "16572",
              jsonValue: "2.4428543598",
            },
            {
              estimate: "16572",
              jsonValue: "1.9984032217",
            },
            {
              estimate: "16572",
              jsonValue: "2.0442494285",
            },
            {
              estimate: "16572",
              jsonValue: "0.9954079545",
            },
            {
              estimate: "16572",
              jsonValue: "0.1861151193",
            },
            {
              estimate: "16572",
              jsonValue: "-0.1400022918",
            },
            {
              estimate: "16572",
              jsonValue: "1.353710898",
            },
          ],
        },
        uniqueCount: {
          estimate: 396487.94513154455,
          upper: 403011.3536959839,
          lower: 390184.25357985386,
        },
      },
      dti: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            NULL: "545",
            FRACTIONAL: "2299428",
          },
        },
        numberSummary: {
          count: "2299428",
          min: -847.9034247986132,
          max: 3033.614607568782,
          mean: 19.543945419744357,
          stddev: 24.80338908043866,
          histogram: {
            start: -847.9034423828125,
            end: 3033.6148053145753,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "512",
              "2148388",
              "150528",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 3033.614501953125,
            min: -847.9034423828125,
            bins: [
              -847.9034423828125, -718.5195007928996, -589.1355592029867, -459.75161761307373, -330.3676760231608,
              -200.98373443324795, -71.59979284333497, 57.78414874657801, 187.16809033649088, 316.55203192640374,
              445.9359735163166, 575.3199151062297, 704.7038566961426, 834.0877982860554, 963.4717398759685,
              1092.8556814658814, 1222.2396230557943, 1351.6235646457071, 1481.00750623562, 1610.3914478255329,
              1739.7753894154457, 1869.159331005359, 1998.543272595272, 2127.9272141851848, 2257.3111557750976,
              2386.6950973650105, 2516.0790389549234, 2645.4629805448362, 2774.8469221347495, 2904.2308637246624,
              3033.6148053145753,
            ],
            n: "2299428",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2222442.4666572656,
            upper: 2257681.8781357603,
            lower: 2187748.878607164,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -847.9034423828125, -29.791902542114258, -11.942795753479004, 4.542914867401123, 15.830941200256348,
              31.20582389831543, 63.155216217041016, 93.69354248046875, 3033.614501953125,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "60134",
                value: 9.1145825925,
                rank: 0,
              },
              {
                estimate: "60134",
                value: 15.1920926227,
                rank: 1,
              },
              {
                estimate: "60134",
                value: 7.8024979848000005,
                rank: 2,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "60134",
              jsonValue: "9.1145825925",
            },
            {
              estimate: "60134",
              jsonValue: "15.1920926227",
            },
            {
              estimate: "60134",
              jsonValue: "7.8024979848",
            },
          ],
        },
        uniqueCount: {
          estimate: 2220308.7101373537,
          upper: 2256839.4068035632,
          lower: 2185008.415563216,
        },
      },
      mths_since_rcnt_il: {
        counters: {
          count: "231585",
        },
        schema: {
          inferredType: {
            type: "NULL",
            ratio: 1.0,
          },
          typeCounts: {
            NULL: "231585",
          },
        },
      },
      dti_joint: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            NULL: "2246313",
            FRACTIONAL: "53660",
          },
        },
        numberSummary: {
          count: "53660",
          min: -89.93563063071949,
          max: 157.1419388110259,
          mean: 18.387253250583715,
          stddev: 21.081468599849718,
          histogram: {
            start: -89.93563079833984,
            end: 157.1419529700531,
            counts: [
              "0",
              "4",
              "0",
              "0",
              "8",
              "32",
              "208",
              "560",
              "816",
              "2304",
              "5136",
              "9072",
              "10144",
              "8496",
              "6112",
              "3952",
              "2720",
              "1552",
              "1104",
              "736",
              "96",
              "400",
              "0",
              "144",
              "64",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 157.14193725585938,
            min: -89.93563079833984,
            bins: [
              -89.93563079833984, -81.69971133939342, -73.46379188044698, -65.22787242150055, -56.991952962554116,
              -48.75603350360768, -40.520114044661256, -32.28419458571482, -24.048275126768388, -15.812355667821961,
              -7.576436208875521, 0.659483250070906, 8.895402709017333, 17.131322167963773, 25.3672416269102,
              33.60316108585664, 41.83908054480307, 50.075000003749494, 58.31091946269592, 66.54683892164238,
              74.7827583805888, 83.01867783953523, 91.25459729848166, 99.49051675742808, 107.72643621637451,
              115.96235567532096, 124.19827513426739, 132.43419459321382, 140.67011405216024, 148.90603351110667,
              157.14195297005313,
            ],
            n: "53660",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 51908.64378439082,
            upper: 52699.80127161425,
            lower: 51129.25432460641,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -89.93563079833984, -26.78022575378418, -11.382527351379395, 4.967487812042236, 16.11406898498535,
              29.514402389526367, 56.750308990478516, 83.64151000976562, 157.14193725585938,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "2159",
                value: 22.49,
                rank: 0,
              },
              {
                estimate: "2157",
                value: 24.2621196575,
                rank: 1,
              },
              {
                estimate: "2157",
                value: -11.9631685905,
                rank: 2,
              },
              {
                estimate: "2157",
                value: 18.1651760769,
                rank: 3,
              },
              {
                estimate: "2157",
                value: -4.6984693655,
                rank: 4,
              },
              {
                estimate: "2157",
                value: -1.5803105966,
                rank: 5,
              },
              {
                estimate: "2157",
                value: 19.12202416,
                rank: 6,
              },
              {
                estimate: "2157",
                value: 21.1212729618,
                rank: 7,
              },
              {
                estimate: "2157",
                value: 9.6456349893,
                rank: 8,
              },
              {
                estimate: "2157",
                value: 5.7999283028,
                rank: 9,
              },
              {
                estimate: "2157",
                value: 8.2961392742,
                rank: 10,
              },
              {
                estimate: "2157",
                value: 1.1660759424,
                rank: 11,
              },
              {
                estimate: "2157",
                value: 2.8650501199000002,
                rank: 12,
              },
              {
                estimate: "2157",
                value: 15.2754768102,
                rank: 13,
              },
              {
                estimate: "2157",
                value: 43.1238119447,
                rank: 14,
              },
              {
                estimate: "2157",
                value: 5.7415504136,
                rank: 15,
              },
              {
                estimate: "2157",
                value: 29.3215849432,
                rank: 16,
              },
              {
                estimate: "2157",
                value: 11.229578248,
                rank: 17,
              },
              {
                estimate: "2157",
                value: 19.9266214761,
                rank: 18,
              },
              {
                estimate: "2157",
                value: 6.4867740662,
                rank: 19,
              },
              {
                estimate: "2157",
                value: 3.2974181431,
                rank: 20,
              },
              {
                estimate: "2157",
                value: 10.0504073073,
                rank: 21,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "2159",
              jsonValue: "22.49",
            },
            {
              estimate: "2157",
              jsonValue: "24.2621196575",
            },
            {
              estimate: "2157",
              jsonValue: "-11.9631685905",
            },
            {
              estimate: "2157",
              jsonValue: "18.1651760769",
            },
            {
              estimate: "2157",
              jsonValue: "-4.6984693655",
            },
            {
              estimate: "2157",
              jsonValue: "-1.5803105966",
            },
            {
              estimate: "2157",
              jsonValue: "19.12202416",
            },
            {
              estimate: "2157",
              jsonValue: "21.1212729618",
            },
            {
              estimate: "2157",
              jsonValue: "9.6456349893",
            },
            {
              estimate: "2157",
              jsonValue: "5.7999283028",
            },
            {
              estimate: "2157",
              jsonValue: "8.2961392742",
            },
            {
              estimate: "2157",
              jsonValue: "1.1660759424",
            },
            {
              estimate: "2157",
              jsonValue: "2.8650501199",
            },
            {
              estimate: "2157",
              jsonValue: "15.2754768102",
            },
            {
              estimate: "2157",
              jsonValue: "43.1238119447",
            },
            {
              estimate: "2157",
              jsonValue: "5.7415504136",
            },
            {
              estimate: "2157",
              jsonValue: "29.3215849432",
            },
            {
              estimate: "2157",
              jsonValue: "11.229578248",
            },
            {
              estimate: "2157",
              jsonValue: "19.9266214761",
            },
            {
              estimate: "2157",
              jsonValue: "6.4867740662",
            },
            {
              estimate: "2157",
              jsonValue: "3.2974181431",
            },
            {
              estimate: "2157",
              jsonValue: "10.0504073073",
            },
          ],
        },
        uniqueCount: {
          estimate: 50821.65972970404,
          upper: 51657.827523480926,
          lower: 50013.65517114238,
        },
      },
      collections_12_mths_ex_med: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -8.42023971784132,
          max: 25.177780847452453,
          mean: 0.022077292372408144,
          stddev: 0.23239109986798911,
          histogram: {
            start: -8.420239448547363,
            end: 25.177782669145202,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "64",
              "7168",
              "2271237",
              "13312",
              "8192",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 25.177780151367188,
            min: -8.420239448547363,
            bins: [
              -8.420239448547363, -7.300305377957611, -6.180371307367858, -5.060437236778107, -3.9405031661883543,
              -2.820569095598602, -1.7006350250088502, -0.5807009544190977, 0.5392331161706547, 1.6591671867604063,
              2.7791012573501597, 3.8990353279399113, 5.018969398529663, 6.138903469119416, 7.258837539709168,
              8.37877161029892, 9.498705680888673, 10.618639751478426, 11.738573822068176, 12.85850789265793,
              13.978441963247683, 15.098376033837432, 16.218310104427186, 17.33824417501694, 18.45817824560669,
              19.578112316196442, 20.698046386786196, 21.81798045737595, 22.9379145279657, 24.057848598555452,
              25.177782669145202,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 43887.53409770841,
            upper: 44551.26581345206,
            lower: 43233.59681668136,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [-8.420239448547363, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.22275173664093018, 25.177780151367188],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "2253996",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "3486",
                value: 1.0,
                rank: 1,
              },
              {
                estimate: "1932",
                value: 2.0,
                rank: 2,
              },
              {
                estimate: "1844",
                value: 1.9797947995,
                rank: 3,
              },
              {
                estimate: "1844",
                value: 1.2718584623,
                rank: 4,
              },
              {
                estimate: "1844",
                value: 0.3504718949,
                rank: 5,
              },
              {
                estimate: "1844",
                value: 0.5053688355,
                rank: 6,
              },
              {
                estimate: "1844",
                value: 2.8511398164,
                rank: 7,
              },
              {
                estimate: "1844",
                value: 1.190782923,
                rank: 8,
              },
              {
                estimate: "1844",
                value: 1.7942487406,
                rank: 9,
              },
              {
                estimate: "1844",
                value: 1.1867696591,
                rank: 10,
              },
              {
                estimate: "1844",
                value: 1.9674071437,
                rank: 11,
              },
              {
                estimate: "1844",
                value: 1.3914131643,
                rank: 12,
              },
              {
                estimate: "1844",
                value: 0.7198054637,
                rank: 13,
              },
              {
                estimate: "1844",
                value: 0.4057294262,
                rank: 14,
              },
              {
                estimate: "1844",
                value: 2.4064590341,
                rank: 15,
              },
            ],
            longs: [],
          },
          isDiscrete: true,
        },
        frequentItems: {
          items: [
            {
              estimate: "2253996",
              jsonValue: "0.0",
            },
            {
              estimate: "3486",
              jsonValue: "1.0",
            },
            {
              estimate: "1932",
              jsonValue: "2.0",
            },
            {
              estimate: "1844",
              jsonValue: "1.9797947995",
            },
            {
              estimate: "1844",
              jsonValue: "1.2718584623",
            },
            {
              estimate: "1844",
              jsonValue: "0.3504718949",
            },
            {
              estimate: "1844",
              jsonValue: "0.5053688355",
            },
            {
              estimate: "1844",
              jsonValue: "2.8511398164",
            },
            {
              estimate: "1844",
              jsonValue: "1.190782923",
            },
            {
              estimate: "1844",
              jsonValue: "1.7942487406",
            },
            {
              estimate: "1844",
              jsonValue: "1.1867696591",
            },
            {
              estimate: "1844",
              jsonValue: "1.9674071437",
            },
            {
              estimate: "1844",
              jsonValue: "1.3914131643",
            },
            {
              estimate: "1844",
              jsonValue: "0.7198054637",
            },
            {
              estimate: "1844",
              jsonValue: "0.4057294262",
            },
            {
              estimate: "1844",
              jsonValue: "2.4064590341",
            },
          ],
        },
        uniqueCount: {
          estimate: 44204.16305313367,
          upper: 44931.45330088386,
          lower: 43501.36890110681,
        },
      },
      revol_bal: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -1283143.9835379608,
          max: 3618385.0113642886,
          mean: 17762.334666252795,
          stddev: 40386.42974426099,
          histogram: {
            start: -1283144.0,
            end: 3618385.3618385,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "64",
              "1752068",
              "526337",
              "8192",
              "4096",
              "8192",
              "0",
              "1024",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 3618385.0,
            min: -1283144.0,
            bins: [
              -1283144.0, -1119759.6879387167, -956375.3758774333, -792991.06381615, -629606.7517548667,
              -466222.4396935834, -302838.1276323, -139453.81557101663, 23930.49649026664, 187314.8085515499,
              350699.1206128332, 514083.4326741167, 677467.7447354, 840852.0567966835, 1004236.3688579667,
              1167620.68091925, 1331004.9929805333, 1494389.3050418166, 1657773.6171030998, 1821157.929164383,
              1984542.2412256664, 2147926.55328695, 2311310.8653482334, 2474695.1774095166, 2638079.4894708,
              2801463.801532083, 2964848.113593367, 3128232.4256546497, 3291616.7377159335, 3455001.0497772163,
              3618385.3618385,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2169736.886353215,
            upper: 2204139.8298148443,
            lower: 2135866.7982217693,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -1283144.0, -27439.767578125, -7826.2197265625, 2079.641357421875, 9153.69921875, 22862.3359375,
              70941.625, 163424.453125, 3618385.0,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "87256",
                value: 14585.6291174724,
                rank: 0,
              },
              {
                estimate: "87256",
                value: 11237.7592940932,
                rank: 1,
              },
              {
                estimate: "87256",
                value: 3036.1864915633,
                rank: 2,
              },
              {
                estimate: "87256",
                value: 18102.0697424641,
                rank: 3,
              },
              {
                estimate: "87256",
                value: 2261.3612589032,
                rank: 4,
              },
              {
                estimate: "87256",
                value: 2211.0838972309,
                rank: 5,
              },
              {
                estimate: "87256",
                value: 35033.3978723965,
                rank: 6,
              },
              {
                estimate: "87256",
                value: -7749.2862142772,
                rank: 7,
              },
              {
                estimate: "87256",
                value: -1674.0051018338,
                rank: 8,
              },
              {
                estimate: "87256",
                value: 396.8780102433,
                rank: 9,
              },
              {
                estimate: "87256",
                value: 34135.594789147,
                rank: 10,
              },
              {
                estimate: "87256",
                value: 40374.2468342697,
                rank: 11,
              },
              {
                estimate: "87256",
                value: 509.6972339816,
                rank: 12,
              },
              {
                estimate: "87256",
                value: 30371.6307525218,
                rank: 13,
              },
              {
                estimate: "87256",
                value: 6287.5210549783,
                rank: 14,
              },
              {
                estimate: "87256",
                value: 2936.2418737804,
                rank: 15,
              },
              {
                estimate: "87256",
                value: 3857.222553478,
                rank: 16,
              },
              {
                estimate: "87256",
                value: -17890.9873686038,
                rank: 17,
              },
              {
                estimate: "87256",
                value: 35205.8237859378,
                rank: 18,
              },
              {
                estimate: "87256",
                value: 31674.739865609,
                rank: 19,
              },
              {
                estimate: "87256",
                value: 41614.0598799419,
                rank: 20,
              },
              {
                estimate: "87256",
                value: 15404.0493476191,
                rank: 21,
              },
              {
                estimate: "87256",
                value: 72326.3718644366,
                rank: 22,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "87256",
              jsonValue: "14585.6291174724",
            },
            {
              estimate: "87256",
              jsonValue: "11237.7592940932",
            },
            {
              estimate: "87256",
              jsonValue: "3036.1864915633",
            },
            {
              estimate: "87256",
              jsonValue: "18102.0697424641",
            },
            {
              estimate: "87256",
              jsonValue: "2261.3612589032",
            },
            {
              estimate: "87256",
              jsonValue: "2211.0838972309",
            },
            {
              estimate: "87256",
              jsonValue: "35033.3978723965",
            },
            {
              estimate: "87256",
              jsonValue: "-7749.2862142772",
            },
            {
              estimate: "87256",
              jsonValue: "-1674.0051018338",
            },
            {
              estimate: "87256",
              jsonValue: "396.8780102433",
            },
            {
              estimate: "87256",
              jsonValue: "34135.594789147",
            },
            {
              estimate: "87256",
              jsonValue: "40374.2468342697",
            },
            {
              estimate: "87256",
              jsonValue: "509.6972339816",
            },
            {
              estimate: "87256",
              jsonValue: "30371.6307525218",
            },
            {
              estimate: "87256",
              jsonValue: "6287.5210549783",
            },
            {
              estimate: "87256",
              jsonValue: "2936.2418737804",
            },
            {
              estimate: "87256",
              jsonValue: "3857.222553478",
            },
            {
              estimate: "87256",
              jsonValue: "-17890.9873686038",
            },
            {
              estimate: "87256",
              jsonValue: "35205.8237859378",
            },
            {
              estimate: "87256",
              jsonValue: "31674.739865609",
            },
            {
              estimate: "87256",
              jsonValue: "41614.0598799419",
            },
            {
              estimate: "87256",
              jsonValue: "15404.0493476191",
            },
            {
              estimate: "87256",
              jsonValue: "72326.3718644366",
            },
          ],
        },
        uniqueCount: {
          estimate: 2210136.731453184,
          upper: 2246500.068749001,
          lower: 2174998.159364894,
        },
      },
      fico_range_low: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -2925.1274675272807,
          max: 4509.797956103501,
          mean: 695.7367579602126,
          stddev: 695.6533021843177,
          histogram: {
            start: -2925.12744140625,
            end: 4509.7983025422855,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "2048",
              "1088",
              "5120",
              "17408",
              "29696",
              "61442",
              "115713",
              "167936",
              "237569",
              "280576",
              "401409",
              "289792",
              "245760",
              "186368",
              "120832",
              "73728",
              "33792",
              "21504",
              "8192",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 4509.7978515625,
            min: -2925.12744140625,
            bins: [
              -2925.12744140625, -2677.296583274632, -2429.4657251430144, -2181.6348670113966, -1933.8040088797786,
              -1685.9731507481608, -1438.1422926165428, -1190.311434484925, -942.4805763533072, -694.6497182216895,
              -446.81886009007167, -198.98800195845388, 48.842856173164364, 296.67371430478215, 544.5045724363999,
              792.3354305680177, 1040.1662886996355, 1287.9971468312533, 1535.828004962871, 1783.6588630944889,
              2031.4897212261067, 2279.3205793577245, 2527.1514374893422, 2774.98229562096, 3022.8131537525787,
              3270.6440118841965, 3518.4748700158143, 3766.305728147432, 4014.13658627905, 4261.967444410668,
              4509.7983025422855,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2152917.87921959,
            upper: 2187053.8953149593,
            lower: 2119310.5801727455,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -2925.12744140625, -971.2421264648438, -453.083740234375, 238.08267211914062, 690.0, 1145.7879638671875,
              1849.1148681640625, 2368.926513671875, 4509.7978515625,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "92963",
                value: 665.0,
                rank: 0,
              },
              {
                estimate: "92559",
                value: 670.0,
                rank: 1,
              },
              {
                estimate: "91325",
                value: 680.0,
                rank: 2,
              },
              {
                estimate: "91165",
                value: 660.0,
                rank: 3,
              },
              {
                estimate: "90485",
                value: 675.0,
                rank: 4,
              },
              {
                estimate: "89208",
                value: 685.0,
                rank: 5,
              },
              {
                estimate: "88999",
                value: 690.0,
                rank: 6,
              },
              {
                estimate: "88519",
                value: 695.0,
                rank: 7,
              },
              {
                estimate: "88171",
                value: 710.0,
                rank: 8,
              },
              {
                estimate: "88002",
                value: 705.0,
                rank: 9,
              },
              {
                estimate: "87574",
                value: 700.0,
                rank: 10,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "92963",
              jsonValue: "665.0",
            },
            {
              estimate: "92559",
              jsonValue: "670.0",
            },
            {
              estimate: "91325",
              jsonValue: "680.0",
            },
            {
              estimate: "91165",
              jsonValue: "660.0",
            },
            {
              estimate: "90485",
              jsonValue: "675.0",
            },
            {
              estimate: "89208",
              jsonValue: "685.0",
            },
            {
              estimate: "88999",
              jsonValue: "690.0",
            },
            {
              estimate: "88519",
              jsonValue: "695.0",
            },
            {
              estimate: "88171",
              jsonValue: "710.0",
            },
            {
              estimate: "88002",
              jsonValue: "705.0",
            },
            {
              estimate: "87574",
              jsonValue: "700.0",
            },
          ],
        },
        uniqueCount: {
          estimate: 2234884.6172653385,
          upper: 2271655.131052247,
          lower: 2199352.5829277374,
        },
      },
      avg_cur_bal: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -561047.6060801669,
          max: 1451738.7699598635,
          mean: 13290.480547585365,
          stddev: 26336.251600397183,
          histogram: {
            start: -561047.625,
            end: 1451738.895173875,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "64",
              "24578",
              "2076675",
              "173056",
              "25600",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 1451738.75,
            min: -561047.625,
            bins: [
              -561047.625, -493954.7409942042, -426861.85698840837, -359768.9729826125, -292676.0889768167,
              -225583.20497102087, -158490.320965225, -91397.43695942918, -24304.552953633363, 42788.33105216245,
              109881.21505795827, 176974.09906375408, 244066.98306955, 311159.8670753458, 378252.75108114164,
              445345.63508693746, 512438.5190927333, 579531.4030985292, 646624.2871043249, 713717.1711101208,
              780810.0551159165, 847902.9391217125, 914995.8231275082, 982088.7071333041, 1049181.5911391,
              1116274.4751448957, 1183367.3591506917, 1250460.2431564874, 1317553.1271622833, 1384646.011168079,
              1451738.895173875,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2245185.3457986917,
            upper: 2280785.6999065815,
            lower: 2210136.4109572168,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -561047.625, -25144.19921875, -5834.4423828125, 1243.5830078125, 5486.85791015625, 17002.595703125,
              57968.8671875, 110540.734375, 1451738.75,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "87256",
                value: 614.3678406933,
                rank: 0,
              },
              {
                estimate: "87256",
                value: 5564.7762894465,
                rank: 1,
              },
              {
                estimate: "87256",
                value: 2345.9801814127,
                rank: 2,
              },
              {
                estimate: "87256",
                value: 148833.3199215929,
                rank: 3,
              },
              {
                estimate: "87256",
                value: 1992.0111076215,
                rank: 4,
              },
              {
                estimate: "87256",
                value: 13415.0280793893,
                rank: 5,
              },
              {
                estimate: "87256",
                value: 40036.5458903415,
                rank: 6,
              },
              {
                estimate: "87256",
                value: 1688.7592489285,
                rank: 7,
              },
              {
                estimate: "87256",
                value: 3378.4445092446,
                rank: 8,
              },
              {
                estimate: "87256",
                value: 24163.0562709064,
                rank: 9,
              },
              {
                estimate: "87256",
                value: 6687.2110581231,
                rank: 10,
              },
              {
                estimate: "87256",
                value: 59268.3921272265,
                rank: 11,
              },
              {
                estimate: "87256",
                value: 14424.2632386759,
                rank: 12,
              },
              {
                estimate: "87256",
                value: 96705.685154356,
                rank: 13,
              },
              {
                estimate: "87256",
                value: 17353.1859471889,
                rank: 14,
              },
              {
                estimate: "87256",
                value: 29425.4874197788,
                rank: 15,
              },
              {
                estimate: "87256",
                value: 38272.2926377595,
                rank: 16,
              },
              {
                estimate: "87256",
                value: 6467.4205804076,
                rank: 17,
              },
              {
                estimate: "87256",
                value: 4084.7567889323,
                rank: 18,
              },
              {
                estimate: "87256",
                value: 7261.155060672,
                rank: 19,
              },
              {
                estimate: "87256",
                value: 38155.2607486713,
                rank: 20,
              },
              {
                estimate: "87256",
                value: 4186.3098823621,
                rank: 21,
              },
              {
                estimate: "87256",
                value: 1307.6775682215,
                rank: 22,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "87256",
              jsonValue: "614.3678406933",
            },
            {
              estimate: "87256",
              jsonValue: "5564.7762894465",
            },
            {
              estimate: "87256",
              jsonValue: "2345.9801814127",
            },
            {
              estimate: "87256",
              jsonValue: "148833.3199215929",
            },
            {
              estimate: "87256",
              jsonValue: "1992.0111076215",
            },
            {
              estimate: "87256",
              jsonValue: "13415.0280793893",
            },
            {
              estimate: "87256",
              jsonValue: "40036.5458903415",
            },
            {
              estimate: "87256",
              jsonValue: "1688.7592489285",
            },
            {
              estimate: "87256",
              jsonValue: "3378.4445092446",
            },
            {
              estimate: "87256",
              jsonValue: "24163.0562709064",
            },
            {
              estimate: "87256",
              jsonValue: "6687.2110581231",
            },
            {
              estimate: "87256",
              jsonValue: "59268.3921272265",
            },
            {
              estimate: "87256",
              jsonValue: "14424.2632386759",
            },
            {
              estimate: "87256",
              jsonValue: "96705.685154356",
            },
            {
              estimate: "87256",
              jsonValue: "17353.1859471889",
            },
            {
              estimate: "87256",
              jsonValue: "29425.4874197788",
            },
            {
              estimate: "87256",
              jsonValue: "38272.2926377595",
            },
            {
              estimate: "87256",
              jsonValue: "6467.4205804076",
            },
            {
              estimate: "87256",
              jsonValue: "4084.7567889323",
            },
            {
              estimate: "87256",
              jsonValue: "7261.155060672",
            },
            {
              estimate: "87256",
              jsonValue: "38155.2607486713",
            },
            {
              estimate: "87256",
              jsonValue: "4186.3098823621",
            },
            {
              estimate: "87256",
              jsonValue: "1307.6775682215",
            },
          ],
        },
        uniqueCount: {
          estimate: 2188844.7129875314,
          upper: 2224857.7331114933,
          lower: 2154044.6589262546,
        },
      },
      hardship_flag: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2299973",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 2.0,
            upper: 2.0,
            lower: 2.0,
          },
          frequent: {
            items: [
              {
                value: "N",
                estimate: 2299346.0,
              },
              {
                value: "Y",
                estimate: 627.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "2299346",
              jsonValue: '"N"',
            },
            {
              estimate: "627",
              jsonValue: '"Y"',
            },
          ],
        },
        uniqueCount: {
          estimate: 2.000000004967054,
          upper: 2.000099863468538,
          lower: 2.0,
        },
      },
      last_pymnt_amnt: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -98765.68804037059,
          max: 183123.65256648135,
          mean: 4139.351627313984,
          stddev: 9943.373917335428,
          histogram: {
            start: -98765.6875,
            end: 183123.67456236563,
            counts: [
              "0",
              "0",
              "0",
              "1024",
              "0",
              "64",
              "4096",
              "0",
              "10240",
              "38912",
              "1727492",
              "288768",
              "116736",
              "52224",
              "33792",
              "13313",
              "8192",
              "0",
              "4096",
              "1024",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 183123.65625,
            min: -98765.6875,
            bins: [
              -98765.6875, -89369.37543125448, -79973.06336250895, -70576.75129376343, -61180.439225017915,
              -51784.12715627239, -42387.81508752688, -32991.50301878135, -23595.19095003583, -14198.878881290308,
              -4802.566812544785, 4593.745256200738, 13990.057324946247, 23386.36939369177, 32782.68146243729,
              42178.993531182816, 51575.30559992834, 60971.61766867386, 70367.92973741938, 79764.24180616491,
              89160.55387491043, 98556.86594365595, 107953.17801240148, 117349.490081147, 126745.8021498925,
              136142.11421863802, 145538.42628738354, 154934.73835612906, 164331.0504248746, 173727.36249362014,
              183123.67456236563,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2164868.733412832,
            upper: 2199194.4164732127,
            lower: 2131074.707883748,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -98765.6875, -10169.857421875, -1123.7166748046875, 132.3211669921875, 636.2235107421875,
              3630.378662109375, 23125.10546875, 43365.72265625, 183123.65625,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "87256",
                value: -1668.2341001662,
                rank: 0,
              },
              {
                estimate: "87256",
                value: 161.0867800928,
                rank: 1,
              },
              {
                estimate: "87256",
                value: 57820.5990105604,
                rank: 2,
              },
              {
                estimate: "87256",
                value: 18232.3427967884,
                rank: 3,
              },
              {
                estimate: "87256",
                value: 1859.757661891,
                rank: 4,
              },
              {
                estimate: "87256",
                value: -176.2525709797,
                rank: 5,
              },
              {
                estimate: "87256",
                value: 188.0587019261,
                rank: 6,
              },
              {
                estimate: "87256",
                value: 20222.3610312037,
                rank: 7,
              },
              {
                estimate: "87256",
                value: 282.4219953145,
                rank: 8,
              },
              {
                estimate: "87256",
                value: 256.3323450307,
                rank: 9,
              },
              {
                estimate: "87256",
                value: -97.7153118936,
                rank: 10,
              },
              {
                estimate: "87256",
                value: 1032.6132474106,
                rank: 11,
              },
              {
                estimate: "87256",
                value: 390.2748622933,
                rank: 12,
              },
              {
                estimate: "87256",
                value: 88.1409439553,
                rank: 13,
              },
              {
                estimate: "87256",
                value: 44076.6246063438,
                rank: 14,
              },
              {
                estimate: "87256",
                value: 436.1984658494,
                rank: 15,
              },
              {
                estimate: "87256",
                value: 1091.6086655689,
                rank: 16,
              },
              {
                estimate: "87256",
                value: 84.0182504381,
                rank: 17,
              },
              {
                estimate: "87256",
                value: -956.5353401413,
                rank: 18,
              },
              {
                estimate: "87256",
                value: 322.1137225282,
                rank: 19,
              },
              {
                estimate: "87256",
                value: 417.4425510911,
                rank: 20,
              },
              {
                estimate: "87256",
                value: 3131.4767473724,
                rank: 21,
              },
              {
                estimate: "87256",
                value: -2732.7007567192,
                rank: 22,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "87256",
              jsonValue: "-1668.2341001662",
            },
            {
              estimate: "87256",
              jsonValue: "161.0867800928",
            },
            {
              estimate: "87256",
              jsonValue: "57820.5990105604",
            },
            {
              estimate: "87256",
              jsonValue: "18232.3427967884",
            },
            {
              estimate: "87256",
              jsonValue: "1859.757661891",
            },
            {
              estimate: "87256",
              jsonValue: "-176.2525709797",
            },
            {
              estimate: "87256",
              jsonValue: "188.0587019261",
            },
            {
              estimate: "87256",
              jsonValue: "20222.3610312037",
            },
            {
              estimate: "87256",
              jsonValue: "282.4219953145",
            },
            {
              estimate: "87256",
              jsonValue: "256.3323450307",
            },
            {
              estimate: "87256",
              jsonValue: "-97.7153118936",
            },
            {
              estimate: "87256",
              jsonValue: "1032.6132474106",
            },
            {
              estimate: "87256",
              jsonValue: "390.2748622933",
            },
            {
              estimate: "87256",
              jsonValue: "88.1409439553",
            },
            {
              estimate: "87256",
              jsonValue: "44076.6246063438",
            },
            {
              estimate: "87256",
              jsonValue: "436.1984658494",
            },
            {
              estimate: "87256",
              jsonValue: "1091.6086655689",
            },
            {
              estimate: "87256",
              jsonValue: "84.0182504381",
            },
            {
              estimate: "87256",
              jsonValue: "-956.5353401413",
            },
            {
              estimate: "87256",
              jsonValue: "322.1137225282",
            },
            {
              estimate: "87256",
              jsonValue: "417.4425510911",
            },
            {
              estimate: "87256",
              jsonValue: "3131.4767473724",
            },
            {
              estimate: "87256",
              jsonValue: "-2732.7007567192",
            },
          ],
        },
        uniqueCount: {
          estimate: 2217040.9012545403,
          upper: 2253517.832723813,
          lower: 2181792.560994525,
        },
      },
      term: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2299973",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 2.0,
            upper: 2.0,
            lower: 2.0,
          },
          frequent: {
            items: [
              {
                value: " 36 months",
                estimate: 1673436.0,
              },
              {
                value: " 60 months",
                estimate: 626537.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "1673436",
              jsonValue: '"36 months"',
            },
            {
              estimate: "626537",
              jsonValue: '"60 months"',
            },
          ],
        },
        uniqueCount: {
          estimate: 2.000000004967054,
          upper: 2.000099863468538,
          lower: 2.0,
        },
      },
      open_acc: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -97.0717773369855,
          max: 202.89375923148413,
          mean: 12.011264600882484,
          stddev: 14.516976948160139,
          histogram: {
            start: -97.07177734375,
            end: 202.8937733411331,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "64",
              "8192",
              "20482",
              "87040",
              "443394",
              "823296",
              "516097",
              "232448",
              "99328",
              "34816",
              "21504",
              "5120",
              "4096",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 202.8937530517578,
            min: -97.07177734375,
            bins: [
              -97.07177734375, -87.0729256542539, -77.07407396475779, -67.0752222752617, -57.07637058576559,
              -47.07751889626948, -37.07866720677338, -27.079815517277282, -17.080963827781176, -7.082112138285069,
              2.9167395512110375, 12.91559124070713, 22.914442930203236, 32.91329461969934, 42.912146309195435,
              52.910997998691556, 62.90984968818765, 72.90870137768374, 82.90755306717986, 92.90640475667595,
              102.90525644617207, 112.90410813566817, 122.90295982516426, 132.90181151466038, 142.90066320415647,
              152.89951489365257, 162.8983665831487, 172.8972182726448, 182.89606996214087, 192.894921651637,
              202.8937733411331,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2212239.7785618794,
            upper: 2247317.267485718,
            lower: 2177705.602716287,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -97.07177734375, -19.37021827697754, -7.524991512298584, 3.191896438598633, 9.826252937316895,
              18.724708557128906, 37.928245544433594, 58.265071868896484, 202.8937530517578,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "91953",
                value: 9.0,
                rank: 0,
              },
              {
                estimate: "91473",
                value: 7.0,
                rank: 1,
              },
              {
                estimate: "91073",
                value: 10.0,
                rank: 2,
              },
              {
                estimate: "91038",
                value: 8.0,
                rank: 3,
              },
              {
                estimate: "89743",
                value: 11.0,
                rank: 4,
              },
              {
                estimate: "89597",
                value: 12.0,
                rank: 5,
              },
              {
                estimate: "89387",
                value: 6.0,
                rank: 6,
              },
              {
                estimate: "89135",
                value: 14.0,
                rank: 7,
              },
              {
                estimate: "89047",
                value: 13.0,
                rank: 8,
              },
              {
                estimate: "87970",
                value: 15.0,
                rank: 9,
              },
              {
                estimate: "87621",
                value: 16.0,
                rank: 10,
              },
              {
                estimate: "87368",
                value: 17.4485563462,
                rank: 11,
              },
              {
                estimate: "87368",
                value: 22.9802315498,
                rank: 12,
              },
              {
                estimate: "87368",
                value: 7.6656268228,
                rank: 13,
              },
              {
                estimate: "87368",
                value: 1.9853303796000001,
                rank: 14,
              },
              {
                estimate: "87368",
                value: 14.9889318822,
                rank: 15,
              },
              {
                estimate: "87368",
                value: 17.9352056058,
                rank: 16,
              },
              {
                estimate: "87368",
                value: 2.2973686764,
                rank: 17,
              },
              {
                estimate: "87368",
                value: 13.3086493076,
                rank: 18,
              },
              {
                estimate: "87368",
                value: 53.8789109044,
                rank: 19,
              },
              {
                estimate: "87368",
                value: 12.3322406567,
                rank: 20,
              },
              {
                estimate: "87368",
                value: 15.3715308927,
                rank: 21,
              },
              {
                estimate: "87368",
                value: 4.7243918406,
                rank: 22,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "91953",
              jsonValue: "9.0",
            },
            {
              estimate: "91473",
              jsonValue: "7.0",
            },
            {
              estimate: "91073",
              jsonValue: "10.0",
            },
            {
              estimate: "91038",
              jsonValue: "8.0",
            },
            {
              estimate: "89743",
              jsonValue: "11.0",
            },
            {
              estimate: "89597",
              jsonValue: "12.0",
            },
            {
              estimate: "89387",
              jsonValue: "6.0",
            },
            {
              estimate: "89135",
              jsonValue: "14.0",
            },
            {
              estimate: "89047",
              jsonValue: "13.0",
            },
            {
              estimate: "87970",
              jsonValue: "15.0",
            },
            {
              estimate: "87621",
              jsonValue: "16.0",
            },
            {
              estimate: "87368",
              jsonValue: "17.4485563462",
            },
            {
              estimate: "87368",
              jsonValue: "22.9802315498",
            },
            {
              estimate: "87368",
              jsonValue: "7.6656268228",
            },
            {
              estimate: "87368",
              jsonValue: "1.9853303796",
            },
            {
              estimate: "87368",
              jsonValue: "14.9889318822",
            },
            {
              estimate: "87368",
              jsonValue: "17.9352056058",
            },
            {
              estimate: "87368",
              jsonValue: "2.2973686764",
            },
            {
              estimate: "87368",
              jsonValue: "13.3086493076",
            },
            {
              estimate: "87368",
              jsonValue: "53.8789109044",
            },
            {
              estimate: "87368",
              jsonValue: "12.3322406567",
            },
            {
              estimate: "87368",
              jsonValue: "15.3715308927",
            },
            {
              estimate: "87368",
              jsonValue: "4.7243918406",
            },
          ],
        },
        uniqueCount: {
          estimate: 2155344.2867502784,
          upper: 2190806.1250032354,
          lower: 2121076.8500269125,
        },
      },
      num_op_rev_tl: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -99.05702647803373,
          max: 193.05208953156955,
          mean: 8.462825852646782,
          stddev: 10.854295463207377,
          histogram: {
            start: -99.0570297241211,
            end: 193.05211281106872,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "4096",
              "64",
              "2",
              "29696",
              "206848",
              "1076227",
              "636928",
              "220160",
              "77824",
              "30720",
              "8192",
              "4096",
              "1024",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 193.05209350585938,
            min: -99.0570297241211,
            bins: [
              -99.0570297241211, -89.32005830628144, -79.58308688844177, -69.84611547060211, -60.10914405276245,
              -50.37217263492279, -40.635201217083136, -30.898229799243467, -21.16125838140381, -11.424286963564157,
              -1.6873155457244877, 8.049655872115167, 17.786627289954822, 27.52359870779449, 37.26057012563416,
              46.9975415434738, 56.73451296131347, 66.47148437915314, 76.20845579699278, 85.94542721483245,
              95.68239863267212, 105.41937005051176, 115.15634146835143, 124.8933128861911, 134.63028430403074,
              144.3672557218704, 154.10422713971008, 163.84119855754972, 173.57816997538941, 183.31514139322906,
              193.0521128110687,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2240301.2017403287,
            upper: 2275824.0416609435,
            lower: 2205328.5793526606,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -99.0570297241211, -13.356417655944824, -4.963550567626953, 2.0285234451293945, 6.5895304679870605,
              12.99907398223877, 28.16048812866211, 44.74940490722656, 193.05209350585938,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "94436",
                value: 6.0,
                rank: 0,
              },
              {
                estimate: "93800",
                value: 7.0,
                rank: 1,
              },
              {
                estimate: "93572",
                value: 4.0,
                rank: 2,
              },
              {
                estimate: "91972",
                value: 5.0,
                rank: 3,
              },
              {
                estimate: "91416",
                value: 8.0,
                rank: 4,
              },
              {
                estimate: "90578",
                value: 9.0,
                rank: 5,
              },
              {
                estimate: "89745",
                value: 3.0,
                rank: 6,
              },
              {
                estimate: "88320",
                value: 11.0,
                rank: 7,
              },
              {
                estimate: "88314",
                value: 10.0,
                rank: 8,
              },
              {
                estimate: "87566",
                value: 12.0,
                rank: 9,
              },
              {
                estimate: "86598",
                value: 2.0,
                rank: 10,
              },
              {
                estimate: "86430",
                value: 13.0,
                rank: 11,
              },
              {
                estimate: "86127",
                value: 1.9259314545000001,
                rank: 12,
              },
              {
                estimate: "86127",
                value: 1.7574520519,
                rank: 13,
              },
              {
                estimate: "86127",
                value: -13.8669827854,
                rank: 14,
              },
              {
                estimate: "86127",
                value: 7.4954115082,
                rank: 15,
              },
              {
                estimate: "86127",
                value: -7.8969446811,
                rank: 16,
              },
              {
                estimate: "86127",
                value: 13.3657311498,
                rank: 17,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "94436",
              jsonValue: "6.0",
            },
            {
              estimate: "93800",
              jsonValue: "7.0",
            },
            {
              estimate: "93572",
              jsonValue: "4.0",
            },
            {
              estimate: "91972",
              jsonValue: "5.0",
            },
            {
              estimate: "91416",
              jsonValue: "8.0",
            },
            {
              estimate: "90578",
              jsonValue: "9.0",
            },
            {
              estimate: "89745",
              jsonValue: "3.0",
            },
            {
              estimate: "88320",
              jsonValue: "11.0",
            },
            {
              estimate: "88314",
              jsonValue: "10.0",
            },
            {
              estimate: "87566",
              jsonValue: "12.0",
            },
            {
              estimate: "86598",
              jsonValue: "2.0",
            },
            {
              estimate: "86430",
              jsonValue: "13.0",
            },
            {
              estimate: "86127",
              jsonValue: "1.9259314545",
            },
            {
              estimate: "86127",
              jsonValue: "1.7574520519",
            },
            {
              estimate: "86127",
              jsonValue: "-13.8669827854",
            },
            {
              estimate: "86127",
              jsonValue: "7.4954115082",
            },
            {
              estimate: "86127",
              jsonValue: "-7.8969446811",
            },
            {
              estimate: "86127",
              jsonValue: "13.3657311498",
            },
          ],
        },
        uniqueCount: {
          estimate: 2204876.31949832,
          upper: 2241153.1073370343,
          lower: 2169821.3817671537,
        },
      },
      tot_cur_bal: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -7287647.509561852,
          max: 11350111.363001607,
          mean: 143863.47891786616,
          stddev: 269722.86972430383,
          histogram: {
            start: -7287647.5,
            end: 11350112.1350111,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "64",
              "7168",
              "1642500",
              "576513",
              "56320",
              "9216",
              "4096",
              "0",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 11350111.0,
            min: -7287647.5,
            bins: [
              -7287647.5, -6666388.84549963, -6045130.19099926, -5423871.536498889, -4802612.88199852,
              -4181354.22749815, -3560095.5729977796, -2938836.9184974097, -2317578.2639970398, -1696319.6094966698,
              -1075060.9549963, -453802.30049593, 167456.35400444083, 788715.0085048107, 1409973.6630051807,
              2031232.3175055496, 2652490.9720059205, 3273749.6265062913, 3895008.2810066603, 4516266.935507031,
              5137525.5900074, 5758784.244507771, 6380042.89900814, 7001301.553508511, 7622560.208008882,
              8243818.862509251, 8865077.517009621, 9486336.17150999, 10107594.826010361, 10728853.48051073,
              11350112.1350111,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2238110.4570076503,
            upper: 2273598.5285439435,
            lower: 2203172.063976127,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -7287647.5, -263836.0, -62057.48046875, 11467.0283203125, 55893.9765625, 199075.265625, 642670.375,
              1222949.375, 11350111.0,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "87256",
                value: -1695.0224391205,
                rank: 0,
              },
              {
                estimate: "87256",
                value: 36748.9190537727,
                rank: 1,
              },
              {
                estimate: "87256",
                value: 617485.0408532871,
                rank: 2,
              },
              {
                estimate: "87256",
                value: 25039.0387082414,
                rank: 3,
              },
              {
                estimate: "87256",
                value: -123120.3587537778,
                rank: 4,
              },
              {
                estimate: "87256",
                value: 26625.9440801363,
                rank: 5,
              },
              {
                estimate: "87256",
                value: 22194.5063817477,
                rank: 6,
              },
              {
                estimate: "87256",
                value: 14289.4331046696,
                rank: 7,
              },
              {
                estimate: "87256",
                value: 361844.7027562779,
                rank: 8,
              },
              {
                estimate: "87256",
                value: 90014.9760051095,
                rank: 9,
              },
              {
                estimate: "87256",
                value: 181600.6056236169,
                rank: 10,
              },
              {
                estimate: "87256",
                value: 12014.2849217109,
                rank: 11,
              },
              {
                estimate: "87256",
                value: -394543.6966007567,
                rank: 12,
              },
              {
                estimate: "87256",
                value: 36688.9858739291,
                rank: 13,
              },
              {
                estimate: "87256",
                value: 565670.8064969255,
                rank: 14,
              },
              {
                estimate: "87256",
                value: 139430.8704652815,
                rank: 15,
              },
              {
                estimate: "87256",
                value: 47131.1947530353,
                rank: 16,
              },
              {
                estimate: "87256",
                value: 1174920.360262438,
                rank: 17,
              },
              {
                estimate: "87256",
                value: 356258.5062989565,
                rank: 18,
              },
              {
                estimate: "87256",
                value: 343504.9240650309,
                rank: 19,
              },
              {
                estimate: "87256",
                value: 176710.5459461974,
                rank: 20,
              },
              {
                estimate: "87256",
                value: 394355.6571869612,
                rank: 21,
              },
              {
                estimate: "87256",
                value: 46165.6473906278,
                rank: 22,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "87256",
              jsonValue: "-1695.0224391205",
            },
            {
              estimate: "87256",
              jsonValue: "36748.9190537727",
            },
            {
              estimate: "87256",
              jsonValue: "617485.0408532871",
            },
            {
              estimate: "87256",
              jsonValue: "25039.0387082414",
            },
            {
              estimate: "87256",
              jsonValue: "-123120.3587537778",
            },
            {
              estimate: "87256",
              jsonValue: "26625.9440801363",
            },
            {
              estimate: "87256",
              jsonValue: "22194.5063817477",
            },
            {
              estimate: "87256",
              jsonValue: "14289.4331046696",
            },
            {
              estimate: "87256",
              jsonValue: "361844.7027562779",
            },
            {
              estimate: "87256",
              jsonValue: "90014.9760051095",
            },
            {
              estimate: "87256",
              jsonValue: "181600.6056236169",
            },
            {
              estimate: "87256",
              jsonValue: "12014.2849217109",
            },
            {
              estimate: "87256",
              jsonValue: "-394543.6966007567",
            },
            {
              estimate: "87256",
              jsonValue: "36688.9858739291",
            },
            {
              estimate: "87256",
              jsonValue: "565670.8064969255",
            },
            {
              estimate: "87256",
              jsonValue: "139430.8704652815",
            },
            {
              estimate: "87256",
              jsonValue: "47131.1947530353",
            },
            {
              estimate: "87256",
              jsonValue: "1174920.360262438",
            },
            {
              estimate: "87256",
              jsonValue: "356258.5062989565",
            },
            {
              estimate: "87256",
              jsonValue: "343504.9240650309",
            },
            {
              estimate: "87256",
              jsonValue: "176710.5459461974",
            },
            {
              estimate: "87256",
              jsonValue: "394355.6571869612",
            },
            {
              estimate: "87256",
              jsonValue: "46165.6473906278",
            },
          ],
        },
        uniqueCount: {
          estimate: 2210734.733841433,
          upper: 2247107.910059086,
          lower: 2175586.654219184,
        },
      },
      total_bc_limit: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -824078.2676112646,
          max: 3648458.0511209145,
          mean: 23242.36083895217,
          stddev: 40773.02058142717,
          histogram: {
            start: -824078.25,
            end: 3648458.3648458,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "2112",
              "2095108",
              "190464",
              "12289",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 3648458.0,
            min: -824078.25,
            bins: [
              -824078.25, -674993.6961718067, -525909.1423436133, -376824.58851542, -227740.03468722664,
              -78655.4808590333, 70429.07296916004, 219513.62679735338, 368598.1806255467, 517682.73445374006,
              666767.2882819334, 815851.8421101267, 964936.3959383201, 1114020.9497665134, 1263105.5035947068,
              1412190.0574229, 1561274.6112510934, 1710359.1650792868, 1859443.7189074801, 2008528.2727356735,
              2157612.826563867, 2306697.38039206, 2455781.9342202535, 2604866.488048447, 2753951.04187664,
              2903035.5957048335, 3052120.149533027, 3201204.70336122, 3350289.2571894135, 3499373.8110176064,
              3648458.3648458,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2162948.553672757,
            upper: 2197243.7623706413,
            lower: 2129184.5300499457,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -824078.25, -35960.39453125, -11009.5791015625, 2769.64208984375, 12485.048828125, 31771.578125,
              94613.515625, 189981.9375, 3648458.0,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "87256",
                value: 25842.2793906639,
                rank: 0,
              },
              {
                estimate: "87256",
                value: -3543.112238139,
                rank: 1,
              },
              {
                estimate: "87256",
                value: 42580.3664624253,
                rank: 2,
              },
              {
                estimate: "87256",
                value: 11059.4732093167,
                rank: 3,
              },
              {
                estimate: "87256",
                value: 4463.2219347876,
                rank: 4,
              },
              {
                estimate: "87256",
                value: 246665.1523663451,
                rank: 5,
              },
              {
                estimate: "87256",
                value: 7293.1824766764,
                rank: 6,
              },
              {
                estimate: "87256",
                value: 48011.298268164,
                rank: 7,
              },
              {
                estimate: "87256",
                value: 20045.7368722408,
                rank: 8,
              },
              {
                estimate: "87256",
                value: -10499.6042905436,
                rank: 9,
              },
              {
                estimate: "87256",
                value: 2926.3184602518,
                rank: 10,
              },
              {
                estimate: "87256",
                value: 6986.8281319933,
                rank: 11,
              },
              {
                estimate: "87256",
                value: 3408.9702458203,
                rank: 12,
              },
              {
                estimate: "87256",
                rank: 13,
                value: 0.0,
              },
              {
                estimate: "87256",
                value: 17355.6532970075,
                rank: 14,
              },
              {
                estimate: "87256",
                value: 14614.717533943,
                rank: 15,
              },
              {
                estimate: "87256",
                value: 11955.5813460349,
                rank: 16,
              },
              {
                estimate: "87256",
                value: 10338.8493539744,
                rank: 17,
              },
              {
                estimate: "87256",
                value: 1828.3182796783,
                rank: 18,
              },
              {
                estimate: "87256",
                value: 17571.1580862083,
                rank: 19,
              },
              {
                estimate: "87256",
                value: -5116.8920600408,
                rank: 20,
              },
              {
                estimate: "87256",
                value: -4720.8183460861,
                rank: 21,
              },
              {
                estimate: "87256",
                value: 63712.9258945871,
                rank: 22,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "87256",
              jsonValue: "25842.2793906639",
            },
            {
              estimate: "87256",
              jsonValue: "-3543.112238139",
            },
            {
              estimate: "87256",
              jsonValue: "42580.3664624253",
            },
            {
              estimate: "87256",
              jsonValue: "11059.4732093167",
            },
            {
              estimate: "87256",
              jsonValue: "4463.2219347876",
            },
            {
              estimate: "87256",
              jsonValue: "246665.1523663451",
            },
            {
              estimate: "87256",
              jsonValue: "7293.1824766764",
            },
            {
              estimate: "87256",
              jsonValue: "48011.298268164",
            },
            {
              estimate: "87256",
              jsonValue: "20045.7368722408",
            },
            {
              estimate: "87256",
              jsonValue: "-10499.6042905436",
            },
            {
              estimate: "87256",
              jsonValue: "2926.3184602518",
            },
            {
              estimate: "87256",
              jsonValue: "6986.8281319933",
            },
            {
              estimate: "87256",
              jsonValue: "3408.9702458203",
            },
            {
              estimate: "87256",
              jsonValue: "0.0",
            },
            {
              estimate: "87256",
              jsonValue: "17355.6532970075",
            },
            {
              estimate: "87256",
              jsonValue: "14614.717533943",
            },
            {
              estimate: "87256",
              jsonValue: "11955.5813460349",
            },
            {
              estimate: "87256",
              jsonValue: "10338.8493539744",
            },
            {
              estimate: "87256",
              jsonValue: "1828.3182796783",
            },
            {
              estimate: "87256",
              jsonValue: "17571.1580862083",
            },
            {
              estimate: "87256",
              jsonValue: "-5116.8920600408",
            },
            {
              estimate: "87256",
              jsonValue: "-4720.8183460861",
            },
            {
              estimate: "87256",
              jsonValue: "63712.9258945871",
            },
          ],
        },
        uniqueCount: {
          estimate: 2219543.3489219323,
          upper: 2256061.4531147294,
          lower: 2184255.222690078,
        },
      },
      acc_open_past_24mths: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -83.54674092788228,
          max: 150.29195188255366,
          mean: 4.840580519173475,
          stddev: 6.758406817664045,
          histogram: {
            start: -83.54673767089844,
            end: 150.29196144032744,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "2048",
              "12352",
              "31746",
              "870401",
              "1007618",
              "271360",
              "73728",
              "21504",
              "8192",
              "1024",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 150.2919464111328,
            min: -83.54673767089844,
            bins: [
              -83.54673767089844, -75.75211436719091, -67.95749106348337, -60.16286775977585, -52.368244456068325,
              -44.57362115236079, -36.77899784865326, -28.984374544945737, -21.189751241238206, -13.395127937530674,
              -5.6005046338231494, 2.194118669884375, 9.988741973591914, 17.78336527729944, 25.577988581006963,
              33.3726118847145, 41.167235188422026, 48.96185849212955, 56.75648179583709, 64.5511050995446,
              72.34572840325214, 80.14035170695968, 87.93497501066719, 95.72959831437473, 103.52422161808227,
              111.31884492178978, 119.11346822549731, 126.90809152920485, 134.70271483291236, 142.4973381366199,
              150.29196144032744,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2113555.3012505346,
            upper: 2147066.6104707113,
            lower: 2080563.0239985927,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -83.54673767089844, -7.511879920959473, -2.683046340942383, 0.7283302545547485, 3.324873685836792,
              7.433119297027588, 17.26325035095215, 26.884592056274414, 150.2919464111328,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "102437",
                value: 3.0,
                rank: 0,
              },
              {
                estimate: "100573",
                value: 5.0,
                rank: 1,
              },
              {
                estimate: "100431",
                value: 4.0,
                rank: 2,
              },
              {
                estimate: "99311",
                value: 2.0,
                rank: 3,
              },
              {
                estimate: "96387",
                value: 1.0,
                rank: 4,
              },
              {
                estimate: "96167",
                value: 6.0,
                rank: 5,
              },
              {
                estimate: "95408",
                rank: 6,
                value: 0.0,
              },
              {
                estimate: "94433",
                value: 7.0,
                rank: 7,
              },
              {
                estimate: "93177",
                value: 8.0,
                rank: 8,
              },
              {
                estimate: "91700",
                value: 9.0,
                rank: 9,
              },
              {
                estimate: "90152",
                value: 10.0,
                rank: 10,
              },
              {
                estimate: "89861",
                value: 11.0,
                rank: 11,
              },
              {
                estimate: "89438",
                value: 4.0268659991,
                rank: 12,
              },
              {
                estimate: "89438",
                value: 2.52728787,
                rank: 13,
              },
              {
                estimate: "89438",
                value: -0.004049682000000001,
                rank: 14,
              },
              {
                estimate: "89438",
                value: 14.0986772022,
                rank: 15,
              },
              {
                estimate: "89438",
                value: 5.0836817138,
                rank: 16,
              },
              {
                estimate: "89438",
                value: 4.0173917242,
                rank: 17,
              },
              {
                estimate: "89438",
                value: -1.9399457632,
                rank: 18,
              },
              {
                estimate: "89438",
                value: 20.6598355212,
                rank: 19,
              },
              {
                estimate: "89438",
                value: 1.7280175979,
                rank: 20,
              },
              {
                estimate: "89438",
                value: 1.2316020872,
                rank: 21,
              },
              {
                estimate: "89438",
                value: -0.1345040348,
                rank: 22,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "102437",
              jsonValue: "3.0",
            },
            {
              estimate: "100573",
              jsonValue: "5.0",
            },
            {
              estimate: "100431",
              jsonValue: "4.0",
            },
            {
              estimate: "99311",
              jsonValue: "2.0",
            },
            {
              estimate: "96387",
              jsonValue: "1.0",
            },
            {
              estimate: "96167",
              jsonValue: "6.0",
            },
            {
              estimate: "95408",
              jsonValue: "0.0",
            },
            {
              estimate: "94433",
              jsonValue: "7.0",
            },
            {
              estimate: "93177",
              jsonValue: "8.0",
            },
            {
              estimate: "91700",
              jsonValue: "9.0",
            },
            {
              estimate: "90152",
              jsonValue: "10.0",
            },
            {
              estimate: "89861",
              jsonValue: "11.0",
            },
            {
              estimate: "89438",
              jsonValue: "4.0268659991",
            },
            {
              estimate: "89438",
              jsonValue: "2.52728787",
            },
            {
              estimate: "89438",
              jsonValue: "-0.004049682",
            },
            {
              estimate: "89438",
              jsonValue: "14.0986772022",
            },
            {
              estimate: "89438",
              jsonValue: "5.0836817138",
            },
            {
              estimate: "89438",
              jsonValue: "4.0173917242",
            },
            {
              estimate: "89438",
              jsonValue: "-1.9399457632",
            },
            {
              estimate: "89438",
              jsonValue: "20.6598355212",
            },
            {
              estimate: "89438",
              jsonValue: "1.7280175979",
            },
            {
              estimate: "89438",
              jsonValue: "1.2316020872",
            },
            {
              estimate: "89438",
              jsonValue: "-0.1345040348",
            },
          ],
        },
        uniqueCount: {
          estimate: 2174871.8821400506,
          upper: 2210655.007545802,
          lower: 2140293.979639365,
        },
      },
      out_prncp: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -66051.5685494853,
          max: 91852.6252885842,
          mean: 1229.9202250667213,
          stddev: 5092.060098452466,
          histogram: {
            start: -66051.5703125,
            end: 91852.6341852625,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "1088",
              "5120",
              "10240",
              "17410",
              "2052099",
              "64512",
              "59392",
              "43008",
              "21504",
              "13312",
              "8192",
              "0",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 91852.625,
            min: -66051.5703125,
            bins: [
              -66051.5703125, -60788.09682924125, -55524.6233459825, -50261.149862723745, -44997.676379465,
              -39734.20289620625, -34470.7294129475, -29207.25592968875, -23943.78244643, -18680.30896317125,
              -13416.8354799125, -8153.361996653752, -2889.8885133949952, 2373.5849698637467, 7637.058453122503,
              12900.531936381245, 18164.00541964, 23427.478902898758, 28690.9523861575, 33954.425869416256,
              39217.899352675, 44481.372835933755, 49744.8463191925, 55008.31980245125, 60271.79328571001,
              65535.26676896875, 70798.7402522275, 76062.21373548626, 81325.687218745, 86589.16070200375,
              91852.63418526249,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 447626.6710902622,
            upper: 454698.6835660845,
            lower: 440663.79230888264,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -66051.5703125, -5171.16357421875, 0.0, 0.0, -0.0, 0.0, 10327.939453125, 24197.55078125, 91852.625,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "1841031",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "19123",
                value: -2471.353847199,
                rank: 1,
              },
              {
                estimate: "19123",
                value: -30.8290384818,
                rank: 2,
              },
              {
                estimate: "19123",
                value: 401.0736290551,
                rank: 3,
              },
              {
                estimate: "19123",
                value: 2217.2494687826,
                rank: 4,
              },
              {
                estimate: "19123",
                value: 6.5736945944,
                rank: 5,
              },
              {
                estimate: "19123",
                value: 5.2386907451,
                rank: 6,
              },
              {
                estimate: "19123",
                value: 6554.6448379381,
                rank: 7,
              },
              {
                estimate: "19123",
                value: -69.2994401535,
                rank: 8,
              },
              {
                estimate: "19123",
                value: 337.1891758312,
                rank: 9,
              },
              {
                estimate: "19123",
                value: 3.3987410673,
                rank: 10,
              },
              {
                estimate: "19123",
                value: 4043.8263922594,
                rank: 11,
              },
              {
                estimate: "19123",
                value: 0.1401604272,
                rank: 12,
              },
              {
                estimate: "19123",
                value: -7747.5860223065,
                rank: 13,
              },
              {
                estimate: "19123",
                value: 0.6852416564,
                rank: 14,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "1841031",
              jsonValue: "0.0",
            },
            {
              estimate: "19123",
              jsonValue: "-2471.353847199",
            },
            {
              estimate: "19123",
              jsonValue: "-30.8290384818",
            },
            {
              estimate: "19123",
              jsonValue: "401.0736290551",
            },
            {
              estimate: "19123",
              jsonValue: "2217.2494687826",
            },
            {
              estimate: "19123",
              jsonValue: "6.5736945944",
            },
            {
              estimate: "19123",
              jsonValue: "5.2386907451",
            },
            {
              estimate: "19123",
              jsonValue: "6554.6448379381",
            },
            {
              estimate: "19123",
              jsonValue: "-69.2994401535",
            },
            {
              estimate: "19123",
              jsonValue: "337.1891758312",
            },
            {
              estimate: "19123",
              jsonValue: "3.3987410673",
            },
            {
              estimate: "19123",
              jsonValue: "4043.8263922594",
            },
            {
              estimate: "19123",
              jsonValue: "0.1401604272",
            },
            {
              estimate: "19123",
              jsonValue: "-7747.5860223065",
            },
            {
              estimate: "19123",
              jsonValue: "0.6852416564",
            },
          ],
        },
        uniqueCount: {
          estimate: 446461.07271279726,
          upper: 453806.6881878331,
          lower: 439362.86726474843,
        },
      },
      disbursement_method: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2265091",
            NULL: "34882",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 2.0,
            upper: 2.0,
            lower: 2.0,
          },
          frequent: {
            items: [
              {
                value: "Cash",
                estimate: 2236849.0,
              },
              {
                value: "DirectPay",
                estimate: 28242.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "2236849",
              jsonValue: '"Cash"',
            },
            {
              estimate: "28242",
              jsonValue: '"DirectPay"',
            },
          ],
        },
        uniqueCount: {
          estimate: 2.000000004967054,
          upper: 2.000099863468538,
          lower: 2.0,
        },
      },
      revol_util: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            NULL: "1340",
            FRACTIONAL: "2298633",
          },
        },
        numberSummary: {
          count: "2298633",
          min: -349.73784607284324,
          max: 544.3604016554633,
          mean: 50.9514527017102,
          stddev: 61.2423749527376,
          histogram: {
            start: -349.73785400390625,
            end: 544.3604670336975,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "2048",
              "0",
              "4096",
              "256",
              "12288",
              "43528",
              "99328",
              "371712",
              "552960",
              "454656",
              "318464",
              "189440",
              "111617",
              "65536",
              "41984",
              "13312",
              "8192",
              "9216",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 544.3604125976562,
            min: -349.73785400390625,
            bins: [
              -349.73785400390625, -319.9345766359861, -290.131299268066, -260.32802190014587, -230.52474453222572,
              -200.7214671643056, -170.9181897963855, -141.11491242846535, -111.31163506054523, -81.50835769262511,
              -51.705080324704966, -21.90180295678482, 7.901474411135268, 37.704751779055414, 67.50802914697556,
              97.31130651489565, 127.1145838828158, 156.91786125073594, 186.72113861865603, 216.52441598657617,
              246.32769335449632, 276.13097072241646, 305.9342480903366, 335.73752545825664, 365.5408028261768,
              395.34408019409693, 425.1473575620171, 454.9506349299372, 484.75391229785737, 514.5571896657774,
              544.3604670336975,
            ],
            n: "2298633",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2149158.5543220937,
            upper: 2183234.9077578248,
            lower: 2115609.9929608293,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -349.73785400390625, -79.42030334472656, -31.215381622314453, 9.991585731506348, 41.374088287353516,
              83.23579406738281, 165.46804809570312, 232.65286254882812, 544.3604125976562,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "91946",
                value: 239.4917082468,
                rank: 0,
              },
              {
                estimate: "91946",
                value: 89.0867176225,
                rank: 1,
              },
              {
                estimate: "91946",
                value: 3.1415458224,
                rank: 2,
              },
              {
                estimate: "91946",
                value: 0.3180384059,
                rank: 3,
              },
              {
                estimate: "91946",
                value: 36.4978482633,
                rank: 4,
              },
              {
                estimate: "91946",
                value: 9.5593274905,
                rank: 5,
              },
              {
                estimate: "91946",
                value: 55.0510678564,
                rank: 6,
              },
              {
                estimate: "91946",
                value: -34.5438322988,
                rank: 7,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "91946",
              jsonValue: "239.4917082468",
            },
            {
              estimate: "91946",
              jsonValue: "89.0867176225",
            },
            {
              estimate: "91946",
              jsonValue: "3.1415458224",
            },
            {
              estimate: "91946",
              jsonValue: "0.3180384059",
            },
            {
              estimate: "91946",
              jsonValue: "36.4978482633",
            },
            {
              estimate: "91946",
              jsonValue: "9.5593274905",
            },
            {
              estimate: "91946",
              jsonValue: "55.0510678564",
            },
            {
              estimate: "91946",
              jsonValue: "-34.5438322988",
            },
          ],
        },
        uniqueCount: {
          estimate: 2208415.7448416944,
          upper: 2244750.766777767,
          lower: 2173304.53441445,
        },
      },
      loan_amnt: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -116376.48043284386,
          max: 210809.6221298516,
          mean: 15326.041863316445,
          stddev: 19526.68453778444,
          histogram: {
            start: -116376.4765625,
            end: 210809.6460809625,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "2112",
              "4096",
              "10240",
              "25600",
              "95236",
              "458752",
              "743425",
              "434176",
              "253952",
              "120832",
              "69632",
              "38912",
              "21504",
              "8192",
              "9216",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 210809.625,
            min: -116376.4765625,
            bins: [
              -116376.4765625, -105470.27247438458, -94564.06838626917, -83657.86429815376, -72751.66021003833,
              -61845.456121922914, -50939.2520338075, -40033.04794569209, -29126.843857576663, -18220.63976946124,
              -7314.435681345829, 3591.768406769581, 14497.972494885005, 25404.17658300043, 36310.380671115825,
              47216.58475923125, 58122.788847346674, 69028.9929354621, 79935.19702357752, 90841.40111169292,
              101747.60519980834, 112653.80928792377, 123560.01337603916, 134466.2174641546, 145372.42155227,
              156278.62564038543, 167184.82972850086, 178091.03381661628, 188997.23790473165, 199903.44199284707,
              210809.6460809625,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2072070.2341651018,
            upper: 2104923.151368713,
            lower: 2039726.1416154094,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -116376.4765625, -22864.072265625, -9091.791015625, 3320.5029296875, 11200.0, 23815.42578125,
              51969.8515625, 78127.921875, 210809.625,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "110926",
                value: 34909.5481059957,
                rank: 0,
              },
              {
                estimate: "97053",
                value: -770.1079507111,
                rank: 1,
              },
              {
                estimate: "97052",
                value: 8634.8777685029,
                rank: 2,
              },
              {
                estimate: "97052",
                value: 21709.4905942236,
                rank: 3,
              },
              {
                estimate: "97051",
                value: 4051.0785277829,
                rank: 4,
              },
              {
                estimate: "97051",
                value: 14118.8898922762,
                rank: 5,
              },
              {
                estimate: "86520",
                value: 10000.0,
                rank: 6,
              },
              {
                estimate: "85582",
                value: 20000.0,
                rank: 7,
              },
              {
                estimate: "84969",
                value: 15000.0,
                rank: 8,
              },
              {
                estimate: "84527",
                value: 12000.0,
                rank: 9,
              },
              {
                estimate: "83624",
                value: 5000.0,
                rank: 10,
              },
              {
                estimate: "83282",
                value: 35000.0,
                rank: 11,
              },
              {
                estimate: "83231",
                value: 17359.2233021878,
                rank: 12,
              },
              {
                estimate: "83231",
                value: 6678.8158261255,
                rank: 13,
              },
              {
                estimate: "83231",
                value: -1967.1374326111,
                rank: 14,
              },
              {
                estimate: "83231",
                value: 21936.0615708296,
                rank: 15,
              },
              {
                estimate: "83231",
                value: -9985.2108390386,
                rank: 16,
              },
              {
                estimate: "83231",
                value: -33915.9491024551,
                rank: 17,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "110926",
              jsonValue: "34909.5481059957",
            },
            {
              estimate: "97053",
              jsonValue: "-770.1079507111",
            },
            {
              estimate: "97052",
              jsonValue: "8634.8777685029",
            },
            {
              estimate: "97052",
              jsonValue: "21709.4905942236",
            },
            {
              estimate: "97051",
              jsonValue: "4051.0785277829",
            },
            {
              estimate: "97051",
              jsonValue: "14118.8898922762",
            },
            {
              estimate: "86520",
              jsonValue: "10000.0",
            },
            {
              estimate: "85582",
              jsonValue: "20000.0",
            },
            {
              estimate: "84969",
              jsonValue: "15000.0",
            },
            {
              estimate: "84527",
              jsonValue: "12000.0",
            },
            {
              estimate: "83624",
              jsonValue: "5000.0",
            },
            {
              estimate: "83282",
              jsonValue: "35000.0",
            },
            {
              estimate: "83231",
              jsonValue: "17359.2233021878",
            },
            {
              estimate: "83231",
              jsonValue: "6678.8158261255",
            },
            {
              estimate: "83231",
              jsonValue: "-1967.1374326111",
            },
            {
              estimate: "83231",
              jsonValue: "21936.0615708296",
            },
            {
              estimate: "83231",
              jsonValue: "-9985.2108390386",
            },
            {
              estimate: "83231",
              jsonValue: "-33915.9491024551",
            },
          ],
        },
        uniqueCount: {
          estimate: 2103025.099024543,
          upper: 2137626.1306842975,
          lower: 2069589.4757918715,
        },
      },
      all_util: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299833",
            NULL: "140",
          },
        },
        numberSummary: {
          count: "2299833",
          min: -332.3587519070353,
          max: 639.9182025513636,
          mean: 60.44241410542949,
          stddev: 66.58463755791205,
          histogram: {
            start: -332.3587646484375,
            end: 639.9182768824463,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "128",
              "304",
              "14848",
              "22528",
              "68610",
              "177156",
              "397313",
              "496641",
              "450560",
              "296960",
              "176128",
              "104448",
              "51201",
              "25600",
              "12288",
              "5120",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 639.918212890625,
            min: -332.3587646484375,
            bins: [
              -332.3587646484375, -299.9495299307414, -267.5402952130452, -235.13106049534912, -202.72182577765298,
              -170.31259105995684, -137.90335634226074, -105.4941216245646, -73.08488690686846, -40.675652189172354,
              -8.266417471476188, 24.14281724621992, 56.55205196391603, 88.9612866816122, 121.3705213993083,
              153.77975611700447, 186.18899083470058, 218.59822555239668, 251.0074602700928, 283.416694987789,
              315.8259297054851, 348.23516442318123, 380.64439914087734, 413.05363385857345, 445.46286857626956,
              477.8721032939658, 510.2813380116619, 542.690572729358, 575.0998074470541, 607.5090421647502,
              639.9182768824464,
            ],
            n: "2299833",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2223460.640216509,
            upper: 2258716.210697602,
            lower: 2188751.1436832915,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -332.3587646484375, -88.15055847167969, -39.156532287597656, 16.552631378173828, 54.87205505371094,
              97.96649932861328, 178.629150390625, 245.3497772216797, 639.918212890625,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "9319",
                value: 75.188055403,
                rank: 0,
              },
              {
                estimate: "9319",
                value: 123.4278413169,
                rank: 1,
              },
              {
                estimate: "9319",
                value: 3.3225483705,
                rank: 2,
              },
              {
                estimate: "9319",
                value: 21.190573366,
                rank: 3,
              },
              {
                estimate: "9319",
                value: 232.8217141626,
                rank: 4,
              },
              {
                estimate: "9319",
                value: -28.8209638096,
                rank: 5,
              },
              {
                estimate: "9319",
                value: 96.3234190635,
                rank: 6,
              },
              {
                estimate: "9319",
                value: 35.8293706325,
                rank: 7,
              },
              {
                estimate: "9319",
                value: 5.7068852926,
                rank: 8,
              },
              {
                estimate: "9319",
                value: 70.4593103907,
                rank: 9,
              },
              {
                estimate: "9319",
                value: 3.4119887379,
                rank: 10,
              },
              {
                estimate: "9319",
                value: -10.1511134013,
                rank: 11,
              },
              {
                estimate: "9319",
                value: -23.9729867452,
                rank: 12,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "9319",
              jsonValue: "75.188055403",
            },
            {
              estimate: "9319",
              jsonValue: "123.4278413169",
            },
            {
              estimate: "9319",
              jsonValue: "3.3225483705",
            },
            {
              estimate: "9319",
              jsonValue: "21.190573366",
            },
            {
              estimate: "9319",
              jsonValue: "232.8217141626",
            },
            {
              estimate: "9319",
              jsonValue: "-28.8209638096",
            },
            {
              estimate: "9319",
              jsonValue: "96.3234190635",
            },
            {
              estimate: "9319",
              jsonValue: "35.8293706325",
            },
            {
              estimate: "9319",
              jsonValue: "5.7068852926",
            },
            {
              estimate: "9319",
              jsonValue: "70.4593103907",
            },
            {
              estimate: "9319",
              jsonValue: "3.4119887379",
            },
            {
              estimate: "9319",
              jsonValue: "-10.1511134013",
            },
            {
              estimate: "9319",
              jsonValue: "-23.9729867452",
            },
          ],
        },
        uniqueCount: {
          estimate: 2233145.3292271025,
          upper: 2269887.226541243,
          lower: 2197640.9475216866,
        },
      },
      num_bc_tl: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -96.07159418296625,
          max: 169.01550325123225,
          mean: 7.877953398140904,
          stddev: 10.383210388383908,
          histogram: {
            start: -96.07159423828125,
            end: 169.01551983123778,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "5120",
              "3136",
              "8192",
              "43010",
              "440322",
              "1072129",
              "455680",
              "164864",
              "68608",
              "25600",
              "9216",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 169.0155029296875,
            min: -96.07159423828125,
            bins: [
              -96.07159423828125, -87.23535710263062, -78.39911996697998, -69.56288283132935, -60.72664569567871,
              -51.89040856002808, -43.054171424377444, -34.21793428872681, -25.381697153076175, -16.54546001742554,
              -7.709222881774906, 1.1270142538757284, 9.963251389526363, 18.799488525176997, 27.63572566082763,
              36.471962796478266, 45.3081999321289, 54.144437067779535, 62.98067420343017, 71.8169113390808,
              80.65314847473144, 89.48938561038207, 98.3256227460327, 107.16185988168334, 115.99809701733398,
              124.83433415298461, 133.67057128863524, 142.50680842428588, 151.3430455599365, 160.17928269558718,
              169.01551983123778,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2263256.3447543806,
            upper: 2299143.4960522614,
            lower: 2227925.059054551,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -96.07159423828125, -13.644078254699707, -4.719761371612549, 1.694109320640564, 5.891077518463135, 12.0,
              26.534900665283203, 42.38479232788086, 169.0155029296875,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "92987",
                value: 5.0,
                rank: 0,
              },
              {
                estimate: "92881",
                value: 4.0,
                rank: 1,
              },
              {
                estimate: "91757",
                value: 6.0,
                rank: 2,
              },
              {
                estimate: "91654",
                value: 3.0,
                rank: 3,
              },
              {
                estimate: "91506",
                value: 8.0,
                rank: 4,
              },
              {
                estimate: "90930",
                value: 7.0,
                rank: 5,
              },
              {
                estimate: "90795",
                value: 10.0,
                rank: 6,
              },
              {
                estimate: "89636",
                value: 9.0,
                rank: 7,
              },
              {
                estimate: "89489",
                value: 2.0,
                rank: 8,
              },
              {
                estimate: "88141",
                value: 11.0,
                rank: 9,
              },
              {
                estimate: "86577",
                value: 12.0,
                rank: 10,
              },
              {
                estimate: "86280",
                value: 17.5656818373,
                rank: 11,
              },
              {
                estimate: "86280",
                value: 3.8820198872000002,
                rank: 12,
              },
              {
                estimate: "86280",
                value: -2.4585659776,
                rank: 13,
              },
              {
                estimate: "86280",
                value: -14.985021744,
                rank: 14,
              },
              {
                estimate: "86280",
                value: 0.9890335298,
                rank: 15,
              },
              {
                estimate: "86280",
                value: 24.0105087945,
                rank: 16,
              },
              {
                estimate: "86280",
                value: -1.5219430351,
                rank: 17,
              },
              {
                estimate: "86280",
                value: 29.9289402617,
                rank: 18,
              },
              {
                estimate: "86280",
                value: 2.6263732435,
                rank: 19,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "92987",
              jsonValue: "5.0",
            },
            {
              estimate: "92881",
              jsonValue: "4.0",
            },
            {
              estimate: "91757",
              jsonValue: "6.0",
            },
            {
              estimate: "91654",
              jsonValue: "3.0",
            },
            {
              estimate: "91506",
              jsonValue: "8.0",
            },
            {
              estimate: "90930",
              jsonValue: "7.0",
            },
            {
              estimate: "90795",
              jsonValue: "10.0",
            },
            {
              estimate: "89636",
              jsonValue: "9.0",
            },
            {
              estimate: "89489",
              jsonValue: "2.0",
            },
            {
              estimate: "88141",
              jsonValue: "11.0",
            },
            {
              estimate: "86577",
              jsonValue: "12.0",
            },
            {
              estimate: "86280",
              jsonValue: "17.5656818373",
            },
            {
              estimate: "86280",
              jsonValue: "3.8820198872",
            },
            {
              estimate: "86280",
              jsonValue: "-2.4585659776",
            },
            {
              estimate: "86280",
              jsonValue: "-14.985021744",
            },
            {
              estimate: "86280",
              jsonValue: "0.9890335298",
            },
            {
              estimate: "86280",
              jsonValue: "24.0105087945",
            },
            {
              estimate: "86280",
              jsonValue: "-1.5219430351",
            },
            {
              estimate: "86280",
              jsonValue: "29.9289402617",
            },
            {
              estimate: "86280",
              jsonValue: "2.6263732435",
            },
          ],
        },
        uniqueCount: {
          estimate: 2210545.8034438714,
          upper: 2246915.871193313,
          lower: 2175400.7275925204,
        },
      },
      delinq_amnt: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -125450.1902002847,
          max: 342084.0698774177,
          mean: 25.756613813600755,
          stddev: 1411.7863069124007,
          histogram: {
            start: -125450.1875,
            end: 342084.0967084062,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "1088",
              "2298885",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 342084.0625,
            min: -125450.1875,
            bins: [
              -125450.1875, -109865.71135971979, -94281.23521943958, -78696.75907915938, -63112.28293887917,
              -47527.80679859896, -31943.330658318766, -16358.854518038555, -774.3783777583449, 14810.097762521851,
              30394.573902802076, 45979.05004308227, 61563.52618336247, 77148.0023236427, 92732.47846392289,
              108316.95460420311, 123901.43074448331, 139485.9068847635, 155070.3830250437, 170654.85916532396,
              186239.33530560415, 201823.81144588435, 217408.28758616454, 232992.76372644474, 248577.23986672494,
              264161.7160070052, 279746.1921472854, 295330.6682875656, 310915.1444278458, 326499.620568126,
              342084.0967084062,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 13314.225253280149,
            upper: 13490.591838799306,
            lower: 13140.130341182567,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [-125450.1875, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 342084.0625],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "2286089",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "607",
                value: 500.0,
                rank: 1,
              },
              {
                estimate: "604",
                value: 581.0,
                rank: 2,
              },
              {
                estimate: "604",
                value: 4270.0,
                rank: 3,
              },
              {
                estimate: "599",
                value: 486.0,
                rank: 4,
              },
              {
                estimate: "598",
                value: 333.0,
                rank: 5,
              },
              {
                estimate: "597",
                value: 1334.0,
                rank: 6,
              },
              {
                estimate: "596",
                value: 484.0,
                rank: 7,
              },
              {
                estimate: "595",
                value: 35.0,
                rank: 8,
              },
              {
                estimate: "591",
                value: 17580.0,
                rank: 9,
              },
              {
                estimate: "589",
                value: 151.0,
                rank: 10,
              },
              {
                estimate: "584",
                value: 25673.0,
                rank: 11,
              },
              {
                estimate: "564",
                value: 242.8881412933,
                rank: 12,
              },
            ],
            longs: [],
          },
          isDiscrete: true,
        },
        frequentItems: {
          items: [
            {
              estimate: "2286089",
              jsonValue: "0.0",
            },
            {
              estimate: "607",
              jsonValue: "500.0",
            },
            {
              estimate: "604",
              jsonValue: "581.0",
            },
            {
              estimate: "604",
              jsonValue: "4270.0",
            },
            {
              estimate: "599",
              jsonValue: "486.0",
            },
            {
              estimate: "598",
              jsonValue: "333.0",
            },
            {
              estimate: "597",
              jsonValue: "1334.0",
            },
            {
              estimate: "596",
              jsonValue: "484.0",
            },
            {
              estimate: "595",
              jsonValue: "35.0",
            },
            {
              estimate: "591",
              jsonValue: "17580.0",
            },
            {
              estimate: "589",
              jsonValue: "151.0",
            },
            {
              estimate: "584",
              jsonValue: "25673.0",
            },
            {
              estimate: "564",
              jsonValue: "242.8881412933",
            },
          ],
        },
        uniqueCount: {
          estimate: 13199.779228918289,
          upper: 13416.95494366055,
          lower: 12989.918278967065,
        },
      },
      installment: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -5213.241412636224,
          max: 6783.963638248987,
          mean: 452.78751785189354,
          stddev: 586.210317117938,
          histogram: {
            start: -5213.2412109375,
            end: 6783.964545583886,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "64",
              "0",
              "3072",
              "1024",
              "12288",
              "48128",
              "271363",
              "912385",
              "568321",
              "254976",
              "120832",
              "56320",
              "25600",
              "13312",
              "8192",
              "0",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 6783.9638671875,
            min: -5213.2412109375,
            bins: [
              -5213.2412109375, -4813.334352386787, -4413.427493836074, -4013.520635285361, -3613.613776734648,
              -3213.706918183935, -2813.8000596332226, -2413.8932010825097, -2013.9863425317967, -1614.0794839810837,
              -1214.1726254303708, -814.2657668796583, -414.3589083289453, -14.452049778232322, 385.45480877248065,
              785.3616673231936, 1185.2685258739066, 1585.1753844246196, 1985.0822429753325, 2384.9891015260455,
              2784.8959600767585, 3184.8028186274714, 3584.7096771781835, 3984.6165357288974, 4384.523394279609,
              4784.430252830323, 5184.337111381035, 5584.243969931749, 5984.150828482461, 6384.057687033175,
              6783.964545583887,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2186787.3754352354,
            upper: 2221460.9200136075,
            lower: 2152650.881424357,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -5213.2412109375, -798.453857421875, -269.7184753417969, 100.94792938232422, 338.0791015625,
              693.1581420898438, 1553.367919921875, 2385.76904296875, 6783.9638671875,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "87256",
                value: 102.1215175604,
                rank: 0,
              },
              {
                estimate: "87256",
                value: 306.2915118215,
                rank: 1,
              },
              {
                estimate: "87256",
                value: 1326.953394944,
                rank: 2,
              },
              {
                estimate: "87256",
                value: 216.1512266117,
                rank: 3,
              },
              {
                estimate: "87256",
                value: 252.6494684905,
                rank: 4,
              },
              {
                estimate: "87256",
                value: 369.217547875,
                rank: 5,
              },
              {
                estimate: "87256",
                value: 870.2027970765,
                rank: 6,
              },
              {
                estimate: "87256",
                value: 473.6770127015,
                rank: 7,
              },
              {
                estimate: "87256",
                value: 500.6407308438,
                rank: 8,
              },
              {
                estimate: "87256",
                value: 6.4579699772,
                rank: 9,
              },
              {
                estimate: "87256",
                value: 474.585192641,
                rank: 10,
              },
              {
                estimate: "87256",
                value: 1895.5842877743,
                rank: 11,
              },
              {
                estimate: "87256",
                value: 427.1973933985,
                rank: 12,
              },
              {
                estimate: "87256",
                value: -34.5327129738,
                rank: 13,
              },
              {
                estimate: "87256",
                value: 374.8600432786,
                rank: 14,
              },
              {
                estimate: "87256",
                value: 121.0559102374,
                rank: 15,
              },
              {
                estimate: "87256",
                value: 342.6956790391,
                rank: 16,
              },
              {
                estimate: "87256",
                value: -22.3035519716,
                rank: 17,
              },
              {
                estimate: "87256",
                value: -105.5468656412,
                rank: 18,
              },
              {
                estimate: "87256",
                value: -162.6971905187,
                rank: 19,
              },
              {
                estimate: "87256",
                value: 139.2100037132,
                rank: 20,
              },
              {
                estimate: "87256",
                value: 910.5630645308,
                rank: 21,
              },
              {
                estimate: "87256",
                value: 84.61460413020001,
                rank: 22,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "87256",
              jsonValue: "102.1215175604",
            },
            {
              estimate: "87256",
              jsonValue: "306.2915118215",
            },
            {
              estimate: "87256",
              jsonValue: "1326.953394944",
            },
            {
              estimate: "87256",
              jsonValue: "216.1512266117",
            },
            {
              estimate: "87256",
              jsonValue: "252.6494684905",
            },
            {
              estimate: "87256",
              jsonValue: "369.217547875",
            },
            {
              estimate: "87256",
              jsonValue: "870.2027970765",
            },
            {
              estimate: "87256",
              jsonValue: "473.6770127015",
            },
            {
              estimate: "87256",
              jsonValue: "500.6407308438",
            },
            {
              estimate: "87256",
              jsonValue: "6.4579699772",
            },
            {
              estimate: "87256",
              jsonValue: "474.585192641",
            },
            {
              estimate: "87256",
              jsonValue: "1895.5842877743",
            },
            {
              estimate: "87256",
              jsonValue: "427.1973933985",
            },
            {
              estimate: "87256",
              jsonValue: "-34.5327129738",
            },
            {
              estimate: "87256",
              jsonValue: "374.8600432786",
            },
            {
              estimate: "87256",
              jsonValue: "121.0559102374",
            },
            {
              estimate: "87256",
              jsonValue: "342.6956790391",
            },
            {
              estimate: "87256",
              jsonValue: "-22.3035519716",
            },
            {
              estimate: "87256",
              jsonValue: "-105.5468656412",
            },
            {
              estimate: "87256",
              jsonValue: "-162.6971905187",
            },
            {
              estimate: "87256",
              jsonValue: "139.2100037132",
            },
            {
              estimate: "87256",
              jsonValue: "910.5630645308",
            },
            {
              estimate: "87256",
              jsonValue: "84.6146041302",
            },
          ],
        },
        uniqueCount: {
          estimate: 2274216.6454064893,
          upper: 2311634.288298792,
          lower: 2238059.278126112,
        },
      },
      total_rec_prncp: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -118931.06497128223,
          max: 212448.34404940007,
          mean: 12144.645571665358,
          stddev: 17132.534856448743,
          histogram: {
            start: -118931.0625,
            end: 212448.36499483438,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "1024",
              "64",
              "3072",
              "2",
              "21504",
              "60416",
              "564225",
              "856066",
              "404480",
              "190464",
              "91136",
              "47104",
              "30720",
              "13312",
              "12288",
              "0",
              "0",
              "0",
              "0",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 212448.34375,
            min: -118931.0625,
            bins: [
              -118931.0625, -107885.08158350553, -96839.10066701104, -85793.11975051656, -74747.13883402207,
              -63701.1579175276, -52655.177001033124, -41609.196084538635, -30563.21516804416, -19517.234251549686,
              -8471.253335055197, 2574.727581439278, 13620.708497933752, 24666.689414428227, 35712.67033092273,
              46758.651247417205, 57804.63216391168, 68850.61308040615, 79896.59399690063, 90942.57491339513,
              101988.5558298896, 113034.53674638408, 124080.51766287856, 135126.49857937303, 146172.4794958675,
              157218.460412362, 168264.44132885645, 179310.42224535096, 190356.40316184546, 201402.3840783399,
              212448.3649948344,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2171498.3505622796,
            upper: 2205929.2494791015,
            lower: 2137600.74038024,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -118931.0625, -20875.349609375, -6644.14892578125, 1881.8187255859375, 8060.5302734375, 18415.92578125,
              45016.88671875, 71443.3515625, 212448.34375,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "87906",
                value: 10000.0,
                rank: 0,
              },
              {
                estimate: "87430",
                value: 15000.0,
                rank: 1,
              },
              {
                estimate: "87342",
                value: 20000.0,
                rank: 2,
              },
              {
                estimate: "87215",
                value: 7872.9079085234,
                rank: 3,
              },
              {
                estimate: "87215",
                value: 23258.1127814879,
                rank: 4,
              },
              {
                estimate: "87215",
                value: -559.0828724917,
                rank: 5,
              },
              {
                estimate: "87215",
                value: 2482.4627738876,
                rank: 6,
              },
              {
                estimate: "87215",
                value: 18996.6124120356,
                rank: 7,
              },
              {
                estimate: "87215",
                value: 10876.4038318598,
                rank: 8,
              },
              {
                estimate: "87215",
                value: 5449.1101896397,
                rank: 9,
              },
              {
                estimate: "87215",
                value: 8502.5755326558,
                rank: 10,
              },
              {
                estimate: "87215",
                value: 22231.409645208,
                rank: 11,
              },
              {
                estimate: "87215",
                value: 11678.8358161911,
                rank: 12,
              },
              {
                estimate: "87215",
                value: 8956.278710143,
                rank: 13,
              },
              {
                estimate: "87215",
                value: -5241.6223454651,
                rank: 14,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "87906",
              jsonValue: "10000.0",
            },
            {
              estimate: "87430",
              jsonValue: "15000.0",
            },
            {
              estimate: "87342",
              jsonValue: "20000.0",
            },
            {
              estimate: "87215",
              jsonValue: "7872.9079085234",
            },
            {
              estimate: "87215",
              jsonValue: "23258.1127814879",
            },
            {
              estimate: "87215",
              jsonValue: "-559.0828724917",
            },
            {
              estimate: "87215",
              jsonValue: "2482.4627738876",
            },
            {
              estimate: "87215",
              jsonValue: "18996.6124120356",
            },
            {
              estimate: "87215",
              jsonValue: "10876.4038318598",
            },
            {
              estimate: "87215",
              jsonValue: "5449.1101896397",
            },
            {
              estimate: "87215",
              jsonValue: "8502.5755326558",
            },
            {
              estimate: "87215",
              jsonValue: "22231.409645208",
            },
            {
              estimate: "87215",
              jsonValue: "11678.8358161911",
            },
            {
              estimate: "87215",
              jsonValue: "8956.278710143",
            },
            {
              estimate: "87215",
              jsonValue: "-5241.6223454651",
            },
          ],
        },
        uniqueCount: {
          estimate: 2173027.107182377,
          upper: 2208779.880541096,
          lower: 2138478.534431708,
        },
      },
      recoveries: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -36689.34762042574,
          max: 76804.04798107578,
          mean: 199.92752343571303,
          stddev: 1298.8911780038493,
          histogram: {
            start: -36689.34765625,
            end: 76804.05455540468,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "1024",
              "0",
              "64",
              "0",
              "2187269",
              "86016",
              "16384",
              "0",
              "5120",
              "0",
              "0",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 76804.046875,
            min: -36689.34765625,
            bins: [
              -36689.34765625, -32906.234249194844, -29123.120842139688, -25340.00743508453, -21556.894028029375,
              -17773.78062097422, -13990.667213919063, -10207.553806863907, -6424.440399808751, -2641.326992753595,
              1141.7864143015613, 4924.899821356717, 8708.013228411874, 12491.12663546703, 16274.240042522186,
              20057.353449577342, 23840.466856632498, 27623.580263687654, 31406.69367074281, 35189.807077797974,
              38972.92048485312, 42756.03389190827, 46539.147298963435, 50322.2607060186, 54105.37411307375,
              57888.487520128896, 61671.60092718406, 65454.71433423922, 69237.82774129437, 73020.94114834952,
              76804.05455540468,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 233121.12360149587,
            upper: 236788.74227280513,
            lower: 229509.85997348692,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -36689.34765625, -395.2185974121094, 0.0, 0.0, 0.0, 0.0, 1080.7672119140625, 5082.734375, 76804.046875,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "2060887",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "9962",
                value: 1918.2284674557,
                rank: 1,
              },
              {
                estimate: "9962",
                value: 10766.2498658778,
                rank: 2,
              },
              {
                estimate: "9962",
                value: 2920.9170809068,
                rank: 3,
              },
              {
                estimate: "9962",
                value: -390.6712631036,
                rank: 4,
              },
              {
                estimate: "9962",
                value: -13.0696752657,
                rank: 5,
              },
              {
                estimate: "9962",
                value: 1090.5638087327,
                rank: 6,
              },
              {
                estimate: "9962",
                value: 1716.1104924347,
                rank: 7,
              },
              {
                estimate: "9962",
                value: 2458.889810139,
                rank: 8,
              },
              {
                estimate: "9962",
                value: 833.2816551456,
                rank: 9,
              },
              {
                estimate: "9962",
                value: 47722.4633683485,
                rank: 10,
              },
              {
                estimate: "9962",
                value: 3165.3942142538,
                rank: 11,
              },
              {
                estimate: "9962",
                value: 1913.05935371,
                rank: 12,
              },
              {
                estimate: "9962",
                value: 73.363683159,
                rank: 13,
              },
              {
                estimate: "9962",
                value: 1690.5204502316,
                rank: 14,
              },
              {
                estimate: "9962",
                value: 1192.8357099076,
                rank: 15,
              },
              {
                estimate: "9962",
                value: 3279.3429169599,
                rank: 16,
              },
              {
                estimate: "9962",
                value: -336.3834957216,
                rank: 17,
              },
              {
                estimate: "9962",
                value: 1404.4473250467,
                rank: 18,
              },
              {
                estimate: "9962",
                value: -275.0605911935,
                rank: 19,
              },
              {
                estimate: "9962",
                value: 478.1312474377,
                rank: 20,
              },
              {
                estimate: "9962",
                value: 540.7114625152,
                rank: 21,
              },
              {
                estimate: "9962",
                value: 2052.7889579271,
                rank: 22,
              },
            ],
            longs: [],
          },
          isDiscrete: true,
        },
        frequentItems: {
          items: [
            {
              estimate: "2060887",
              jsonValue: "0.0",
            },
            {
              estimate: "9962",
              jsonValue: "1918.2284674557",
            },
            {
              estimate: "9962",
              jsonValue: "10766.2498658778",
            },
            {
              estimate: "9962",
              jsonValue: "2920.9170809068",
            },
            {
              estimate: "9962",
              jsonValue: "-390.6712631036",
            },
            {
              estimate: "9962",
              jsonValue: "-13.0696752657",
            },
            {
              estimate: "9962",
              jsonValue: "1090.5638087327",
            },
            {
              estimate: "9962",
              jsonValue: "1716.1104924347",
            },
            {
              estimate: "9962",
              jsonValue: "2458.889810139",
            },
            {
              estimate: "9962",
              jsonValue: "833.2816551456",
            },
            {
              estimate: "9962",
              jsonValue: "47722.4633683485",
            },
            {
              estimate: "9962",
              jsonValue: "3165.3942142538",
            },
            {
              estimate: "9962",
              jsonValue: "1913.05935371",
            },
            {
              estimate: "9962",
              jsonValue: "73.363683159",
            },
            {
              estimate: "9962",
              jsonValue: "1690.5204502316",
            },
            {
              estimate: "9962",
              jsonValue: "1192.8357099076",
            },
            {
              estimate: "9962",
              jsonValue: "3279.3429169599",
            },
            {
              estimate: "9962",
              jsonValue: "-336.3834957216",
            },
            {
              estimate: "9962",
              jsonValue: "1404.4473250467",
            },
            {
              estimate: "9962",
              jsonValue: "-275.0605911935",
            },
            {
              estimate: "9962",
              jsonValue: "478.1312474377",
            },
            {
              estimate: "9962",
              jsonValue: "540.7114625152",
            },
            {
              estimate: "9962",
              jsonValue: "2052.7889579271",
            },
          ],
        },
        uniqueCount: {
          estimate: 226726.03028703274,
          upper: 230456.34931921682,
          lower: 223121.35332467375,
        },
      },
      num_accts_ever_120_pd: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -102.76126486139896,
          max: 307.68393054931823,
          mean: 0.5747646815042281,
          stddev: 2.404442728662468,
          histogram: {
            start: -102.76126861572266,
            end: 307.6839602117523,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "3136",
              "2254853",
              "37888",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 307.6839294433594,
            min: -102.76126861572266,
            bins: [
              -102.76126861572266, -89.07976098814015, -75.39825336055766, -61.716745732975156, -48.03523810539266,
              -34.35373047781016, -20.672222850227655, -6.990715222645164, 6.690792404937341, 20.372300032519846,
              34.053807660102336, 47.73531528768484, 61.416822915267346, 75.09833054284985, 88.77983817043233,
              102.46134579801483, 116.14285342559734, 129.82436105317984, 143.50586868076235, 157.18737630834482,
              170.86888393592733, 184.55039156350983, 198.23189919109234, 211.91340681867484, 225.59491444625735,
              239.27642207383985, 252.95792970142236, 266.6394373290048, 280.3209449565873, 294.0024525841698,
              307.6839602117523,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 536962.7965836314,
            upper: 545452.6366700695,
            lower: 528604.0711605857,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -102.76126861572266, -1.538038730621338, 0.0, 0.0, 0.0, -0.0, 3.3754405975341797, 10.319746017456055,
              307.6839294433594,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "1744280",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "34430",
                value: 1.0,
                rank: 1,
              },
              {
                estimate: "26885",
                value: 2.0,
                rank: 2,
              },
              {
                estimate: "24147",
                value: 3.0,
                rank: 3,
              },
              {
                estimate: "23517",
                value: 4.0,
                rank: 4,
              },
              {
                estimate: "22705",
                value: 5.0,
                rank: 5,
              },
              {
                estimate: "22616",
                value: 7.0,
                rank: 6,
              },
              {
                estimate: "22584",
                value: 10.0,
                rank: 7,
              },
              {
                estimate: "22550",
                value: 6.0,
                rank: 8,
              },
              {
                estimate: "22485",
                value: 8.0,
                rank: 9,
              },
              {
                estimate: "22308",
                value: 9.0,
                rank: 10,
              },
              {
                estimate: "22304",
                value: 12.0,
                rank: 11,
              },
              {
                estimate: "22282",
                value: 6.8965529238,
                rank: 12,
              },
              {
                estimate: "22282",
                value: 0.6808981606000001,
                rank: 13,
              },
              {
                estimate: "22282",
                value: 1.0384421582,
                rank: 14,
              },
              {
                estimate: "22282",
                value: 9.3960136178,
                rank: 15,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "1744280",
              jsonValue: "0.0",
            },
            {
              estimate: "34430",
              jsonValue: "1.0",
            },
            {
              estimate: "26885",
              jsonValue: "2.0",
            },
            {
              estimate: "24147",
              jsonValue: "3.0",
            },
            {
              estimate: "23517",
              jsonValue: "4.0",
            },
            {
              estimate: "22705",
              jsonValue: "5.0",
            },
            {
              estimate: "22616",
              jsonValue: "7.0",
            },
            {
              estimate: "22584",
              jsonValue: "10.0",
            },
            {
              estimate: "22550",
              jsonValue: "6.0",
            },
            {
              estimate: "22485",
              jsonValue: "8.0",
            },
            {
              estimate: "22308",
              jsonValue: "9.0",
            },
            {
              estimate: "22304",
              jsonValue: "12.0",
            },
            {
              estimate: "22282",
              jsonValue: "6.8965529238",
            },
            {
              estimate: "22282",
              jsonValue: "0.6808981606",
            },
            {
              estimate: "22282",
              jsonValue: "1.0384421582",
            },
            {
              estimate: "22282",
              jsonValue: "9.3960136178",
            },
          ],
        },
        uniqueCount: {
          estimate: 533200.3078620778,
          upper: 541973.042311079,
          lower: 524723.05069121,
        },
      },
      bc_open_to_buy: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2278678",
            NULL: "21295",
          },
        },
        numberSummary: {
          count: "2278678",
          min: -414549.10449797264,
          max: 1001398.6904238618,
          mean: 10977.394160568761,
          stddev: 25166.72916974048,
          histogram: {
            start: -414549.09375,
            end: 1001398.7876398688,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "256",
              "6160",
              "1611782",
              "557056",
              "68608",
              "17408",
              "9216",
              "0",
              "4096",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 1001398.6875,
            min: -414549.09375,
            bins: [
              -414549.09375, -367350.83103700436, -320152.5683240087, -272954.3056110131, -225756.04289801748,
              -178557.78018502184, -131359.51747202623, -84161.25475903059, -36962.99204603495, 10235.270666960685,
              57433.53337995632, 104631.79609295196, 151830.05880594754, 199028.32151894318, 246226.58423193882,
              293424.84694493446, 340623.1096579301, 387821.37237092573, 435019.63508392137, 482217.897796917,
              529416.1605099126, 576614.4232229083, 623812.6859359039, 671010.9486488996, 718209.2113618951,
              765407.4740748908, 812605.7367878864, 859803.9995008821, 907002.2622138776, 954200.5249268734,
              1001398.7876398689,
            ],
            n: "2278678",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2176353.0248506158,
            upper: 2210860.9702544794,
            lower: 2142379.562664026,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -414549.09375, -16470.88671875, -4029.74072265625, 373.9443359375, 3517.115234375, 12712.0849609375,
              51531.74609375, 116004.328125, 1001398.6875,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "18800",
                value: 1398.8399969877,
                rank: 0,
              },
              {
                estimate: "18800",
                value: 2333.2046168068,
                rank: 1,
              },
              {
                estimate: "18800",
                value: 12088.3567079997,
                rank: 2,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "18800",
              jsonValue: "1398.8399969877",
            },
            {
              estimate: "18800",
              jsonValue: "2333.2046168068",
            },
            {
              estimate: "18800",
              jsonValue: "12088.3567079997",
            },
          ],
        },
        uniqueCount: {
          estimate: 2111074.142696575,
          upper: 2145807.6051176433,
          lower: 2077510.549145467,
        },
      },
      mths_since_last_delinq: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "1189854",
            NULL: "1110119",
          },
        },
        numberSummary: {
          count: "1189854",
          min: -273.53721731178774,
          max: 515.5689005552643,
          mean: 33.668602416044784,
          stddev: 45.77725738984074,
          histogram: {
            start: -273.5372314453125,
            end: 515.5689602482971,
            counts: [
              "0",
              "0",
              "0",
              "128",
              "0",
              "64",
              "2332",
              "4608",
              "21506",
              "64512",
              "408576",
              "307712",
              "165376",
              "98816",
              "51712",
              "32256",
              "14848",
              "8704",
              "6656",
              "2048",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 515.5689086914062,
            min: -273.5372314453125,
            bins: [
              -273.5372314453125, -247.2336917221922, -220.93015199907185, -194.62661227595154, -168.3230725528312,
              -142.0195328297109, -115.71599310659059, -89.41245338347025, -63.10891366034994, -36.80537393722963,
              -10.501834214109294, 15.801705509011015, 42.105245232131324, 68.40878495525163, 94.712324678372,
              121.01586440149231, 147.31940412461262, 173.62294384773293, 199.92648357085324, 226.2300232939736,
              252.5335630170939, 278.83710274021416, 305.14064246333453, 331.4441821864549, 357.74772190957515,
              384.0512616326955, 410.35480135581577, 436.65834107893613, 462.9618808020565, 489.26542052517675,
              515.5689602482971,
            ],
            n: "1189854",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 1166598.621728929,
            upper: 1185081.1750468467,
            lower: 1148402.1036607053,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -273.5372314453125, -56.0676155090332, -18.397661209106445, 4.9716339111328125, 22.256860733032227,
              53.25870895385742, 123.49371337890625, 179.70008850097656, 515.5689086914062,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "47596",
                value: 33.5908776055,
                rank: 0,
              },
              {
                estimate: "47596",
                value: 113.7231594109,
                rank: 1,
              },
              {
                estimate: "47596",
                value: 112.2587271432,
                rank: 2,
              },
              {
                estimate: "47596",
                value: 0.166678707,
                rank: 3,
              },
              {
                estimate: "47596",
                value: 53.8456399445,
                rank: 4,
              },
              {
                estimate: "47596",
                value: 45.2105083806,
                rank: 5,
              },
              {
                estimate: "47596",
                value: -1.6802688027000001,
                rank: 6,
              },
              {
                estimate: "47596",
                value: 50.5868411712,
                rank: 7,
              },
              {
                estimate: "47596",
                value: 17.491776854,
                rank: 8,
              },
              {
                estimate: "47596",
                value: 51.4288331454,
                rank: 9,
              },
              {
                estimate: "47596",
                value: 80.3655213809,
                rank: 10,
              },
              {
                estimate: "47596",
                value: 11.599359669,
                rank: 11,
              },
              {
                estimate: "47596",
                value: 18.9191634428,
                rank: 12,
              },
              {
                estimate: "47596",
                value: 14.6066284133,
                rank: 13,
              },
              {
                estimate: "47596",
                value: 15.8589907526,
                rank: 14,
              },
              {
                estimate: "47596",
                value: 11.8024581248,
                rank: 15,
              },
              {
                estimate: "47596",
                value: 199.4613615868,
                rank: 16,
              },
              {
                estimate: "47596",
                value: 54.1714801165,
                rank: 17,
              },
              {
                estimate: "47596",
                value: 49.7170583975,
                rank: 18,
              },
              {
                estimate: "47596",
                value: 0.4868081321,
                rank: 19,
              },
              {
                estimate: "47596",
                value: 32.8823048881,
                rank: 20,
              },
              {
                estimate: "47596",
                value: 54.9287010467,
                rank: 21,
              },
              {
                estimate: "47596",
                value: 249.1335411447,
                rank: 22,
              },
              {
                estimate: "47596",
                value: 4.2555994728,
                rank: 23,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "47596",
              jsonValue: "33.5908776055",
            },
            {
              estimate: "47596",
              jsonValue: "113.7231594109",
            },
            {
              estimate: "47596",
              jsonValue: "112.2587271432",
            },
            {
              estimate: "47596",
              jsonValue: "0.166678707",
            },
            {
              estimate: "47596",
              jsonValue: "53.8456399445",
            },
            {
              estimate: "47596",
              jsonValue: "45.2105083806",
            },
            {
              estimate: "47596",
              jsonValue: "-1.6802688027",
            },
            {
              estimate: "47596",
              jsonValue: "50.5868411712",
            },
            {
              estimate: "47596",
              jsonValue: "17.491776854",
            },
            {
              estimate: "47596",
              jsonValue: "51.4288331454",
            },
            {
              estimate: "47596",
              jsonValue: "80.3655213809",
            },
            {
              estimate: "47596",
              jsonValue: "11.599359669",
            },
            {
              estimate: "47596",
              jsonValue: "18.9191634428",
            },
            {
              estimate: "47596",
              jsonValue: "14.6066284133",
            },
            {
              estimate: "47596",
              jsonValue: "15.8589907526",
            },
            {
              estimate: "47596",
              jsonValue: "11.8024581248",
            },
            {
              estimate: "47596",
              jsonValue: "199.4613615868",
            },
            {
              estimate: "47596",
              jsonValue: "54.1714801165",
            },
            {
              estimate: "47596",
              jsonValue: "49.7170583975",
            },
            {
              estimate: "47596",
              jsonValue: "0.4868081321",
            },
            {
              estimate: "47596",
              jsonValue: "32.8823048881",
            },
            {
              estimate: "47596",
              jsonValue: "54.9287010467",
            },
            {
              estimate: "47596",
              jsonValue: "249.1335411447",
            },
            {
              estimate: "47596",
              jsonValue: "4.2555994728",
            },
          ],
        },
        uniqueCount: {
          estimate: 1111558.5160949281,
          upper: 1129846.9670624938,
          lower: 1093886.0443006274,
        },
      },
      earliest_cr_line: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2299973",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 594.0,
            upper: 594.0,
            lower: 594.0,
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "92000",
              jsonValue: '"Jun-2006"',
            },
            {
              estimate: "91999",
              jsonValue: '"Aug-1993"',
            },
            {
              estimate: "91999",
              jsonValue: '"Jul-1989"',
            },
            {
              estimate: "91999",
              jsonValue: '"Mar-2007"',
            },
            {
              estimate: "91999",
              jsonValue: '"Oct-1999"',
            },
            {
              estimate: "91999",
              jsonValue: '"Feb-2002"',
            },
            {
              estimate: "91999",
              jsonValue: '"Sep-2002"',
            },
            {
              estimate: "91999",
              jsonValue: '"Jan-2005"',
            },
            {
              estimate: "91999",
              jsonValue: '"Jan-2008"',
            },
            {
              estimate: "91999",
              jsonValue: '"Nov-1999"',
            },
            {
              estimate: "91999",
              jsonValue: '"May-1989"',
            },
            {
              estimate: "91999",
              jsonValue: '"Aug-2012"',
            },
            {
              estimate: "91999",
              jsonValue: '"Apr-2001"',
            },
            {
              estimate: "91999",
              jsonValue: '"Sep-1996"',
            },
            {
              estimate: "91999",
              jsonValue: '"Nov-2004"',
            },
            {
              estimate: "91999",
              jsonValue: '"Jul-1998"',
            },
            {
              estimate: "91999",
              jsonValue: '"Jan-1997"',
            },
            {
              estimate: "91999",
              jsonValue: '"May-2011"',
            },
            {
              estimate: "91999",
              jsonValue: '"Jan-2004"',
            },
            {
              estimate: "91999",
              jsonValue: '"Jul-2006"',
            },
            {
              estimate: "91999",
              jsonValue: '"Apr-1988"',
            },
            {
              estimate: "91999",
              jsonValue: '"Mar-2006"',
            },
          ],
        },
        uniqueCount: {
          estimate: 598.6200597490824,
          upper: 608.4691441224213,
          lower: 589.1027055403412,
        },
      },
      issue_d: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            ratio: 1.0,
            type: "UNKNOWN",
          },
          typeCounts: {
            UNKNOWN: "2299973",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 25.0,
            upper: 25.0,
            lower: 25.0,
          },
          frequent: {
            items: [
              {
                value: "2020-12-15",
                estimate: 122111.0,
              },
              {
                value: "2020-12-14",
                estimate: 120508.0,
              },
              {
                value: "2020-12-21",
                estimate: 120366.0,
              },
              {
                value: "2020-12-22",
                estimate: 119612.0,
              },
              {
                value: "2020-12-16",
                estimate: 118575.0,
              },
              {
                value: "2020-12-09",
                estimate: 115652.0,
              },
              {
                value: "2020-12-07",
                estimate: 113766.0,
              },
              {
                value: "2020-12-23",
                estimate: 113389.0,
              },
              {
                value: "2020-12-08",
                estimate: 112681.0,
              },
              {
                value: "2020-12-10",
                estimate: 108580.0,
              },
              {
                value: "2020-12-17",
                estimate: 106222.0,
              },
              {
                value: "2020-12-24",
                estimate: 97123.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "122111",
              jsonValue: "1607990400000",
            },
            {
              estimate: "120508",
              jsonValue: "1607904000000",
            },
            {
              estimate: "120366",
              jsonValue: "1608508800000",
            },
            {
              estimate: "119612",
              jsonValue: "1608595200000",
            },
            {
              estimate: "118575",
              jsonValue: "1608076800000",
            },
            {
              estimate: "115652",
              jsonValue: "1607472000000",
            },
            {
              estimate: "113766",
              jsonValue: "1607299200000",
            },
            {
              estimate: "113389",
              jsonValue: "1608681600000",
            },
            {
              estimate: "112681",
              jsonValue: "1607385600000",
            },
            {
              estimate: "108580",
              jsonValue: "1607558400000",
            },
            {
              estimate: "106222",
              jsonValue: "1608163200000",
            },
            {
              estimate: "97123",
              jsonValue: "1608768000000",
            },
          ],
        },
        uniqueCount: {
          estimate: 25.000001490116226,
          upper: 25.001249721456077,
          lower: 25.0,
        },
      },
      mths_since_recent_revol_delinq: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "816777",
            NULL: "1483196",
          },
        },
        numberSummary: {
          count: "816777",
          min: -261.7651252882128,
          max: 575.4757066761596,
          mean: 34.87781040297079,
          stddev: 47.3039781369954,
          histogram: {
            start: -261.76513671875,
            end: 575.4757655553833,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "8",
              "256",
              "3456",
              "14080",
              "46080",
              "286465",
              "211968",
              "114688",
              "62976",
              "36352",
              "18688",
              "12288",
              "4608",
              "2560",
              "0",
              "256",
              "2048",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 575.4757080078125,
            min: -261.76513671875,
            bins: [
              -261.76513671875, -233.85710664294555, -205.9490765671411, -178.04104649133666, -150.1330164155322,
              -122.22498633972779, -94.31695626392334, -66.4089261881189, -38.50089611231445, -10.592866036510003,
              17.315164039294416, 45.22319411509886, 73.13122419090331, 101.03925426670776, 128.9472843425122,
              156.85531441831665, 184.7633444941211, 212.67137456992555, 240.57940464573, 268.48743472153444,
              296.39546479733883, 324.30349487314334, 352.2115249489477, 380.11955502475223, 408.0275851005566,
              435.9356151763611, 463.8436452521655, 491.75167532797, 519.6597054037744, 547.5677354795789,
              575.4757655553833,
            ],
            n: "816777",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 791550.969744176,
            upper: 804081.2892799139,
            lower: 779214.4057635418,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -261.76513671875, -55.38964080810547, -20.012928009033203, 5.1114678382873535, 23.388202667236328,
              55.5571174621582, 128.33145141601562, 188.6366729736328, 575.4757080078125,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "29887",
                value: -52.6386743003,
                rank: 0,
              },
              {
                estimate: "29887",
                value: 10.3673181138,
                rank: 1,
              },
              {
                estimate: "29887",
                value: 47.1316961093,
                rank: 2,
              },
              {
                estimate: "29887",
                value: 26.1080785532,
                rank: 3,
              },
              {
                estimate: "29887",
                value: 6.6314709946,
                rank: 4,
              },
              {
                estimate: "29887",
                value: 5.8521858412,
                rank: 5,
              },
              {
                estimate: "29887",
                value: 22.5265651514,
                rank: 6,
              },
              {
                estimate: "29887",
                value: 40.606886296,
                rank: 7,
              },
              {
                estimate: "29887",
                value: 22.8949416489,
                rank: 8,
              },
              {
                estimate: "29887",
                value: 10.1436373477,
                rank: 9,
              },
              {
                estimate: "29887",
                value: 218.0748719577,
                rank: 10,
              },
              {
                estimate: "29887",
                value: 70.3215882527,
                rank: 11,
              },
              {
                estimate: "29887",
                value: 4.3570420608,
                rank: 12,
              },
              {
                estimate: "29887",
                value: 4.3517003934,
                rank: 13,
              },
              {
                estimate: "29887",
                value: 90.1456787844,
                rank: 14,
              },
              {
                estimate: "29887",
                value: 31.3310420231,
                rank: 15,
              },
              {
                estimate: "29887",
                value: 36.9249595932,
                rank: 16,
              },
              {
                estimate: "29887",
                value: 24.9277214551,
                rank: 17,
              },
              {
                estimate: "29887",
                value: 14.969153736700001,
                rank: 18,
              },
              {
                estimate: "29887",
                value: 171.5648594744,
                rank: 19,
              },
              {
                estimate: "29887",
                value: -41.314382852,
                rank: 20,
              },
              {
                estimate: "29887",
                value: 0.3038981818,
                rank: 21,
              },
              {
                estimate: "29887",
                value: 58.9047937439,
                rank: 22,
              },
              {
                estimate: "29887",
                value: 6.5716944958,
                rank: 23,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "29887",
              jsonValue: "-52.6386743003",
            },
            {
              estimate: "29887",
              jsonValue: "10.3673181138",
            },
            {
              estimate: "29887",
              jsonValue: "47.1316961093",
            },
            {
              estimate: "29887",
              jsonValue: "26.1080785532",
            },
            {
              estimate: "29887",
              jsonValue: "6.6314709946",
            },
            {
              estimate: "29887",
              jsonValue: "5.8521858412",
            },
            {
              estimate: "29887",
              jsonValue: "22.5265651514",
            },
            {
              estimate: "29887",
              jsonValue: "40.606886296",
            },
            {
              estimate: "29887",
              jsonValue: "22.8949416489",
            },
            {
              estimate: "29887",
              jsonValue: "10.1436373477",
            },
            {
              estimate: "29887",
              jsonValue: "218.0748719577",
            },
            {
              estimate: "29887",
              jsonValue: "70.3215882527",
            },
            {
              estimate: "29887",
              jsonValue: "4.3570420608",
            },
            {
              estimate: "29887",
              jsonValue: "4.3517003934",
            },
            {
              estimate: "29887",
              jsonValue: "90.1456787844",
            },
            {
              estimate: "29887",
              jsonValue: "31.3310420231",
            },
            {
              estimate: "29887",
              jsonValue: "36.9249595932",
            },
            {
              estimate: "29887",
              jsonValue: "24.9277214551",
            },
            {
              estimate: "29887",
              jsonValue: "14.9691537367",
            },
            {
              estimate: "29887",
              jsonValue: "171.5648594744",
            },
            {
              estimate: "29887",
              jsonValue: "-41.314382852",
            },
            {
              estimate: "29887",
              jsonValue: "0.3038981818",
            },
            {
              estimate: "29887",
              jsonValue: "58.9047937439",
            },
            {
              estimate: "29887",
              jsonValue: "6.5716944958",
            },
          ],
        },
        uniqueCount: {
          estimate: 786691.2975615925,
          upper: 799634.7144071679,
          lower: 774183.8320834623,
        },
      },
      zip_code: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2299973",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 872.0,
            upper: 872.0,
            lower: 872.0,
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "87257",
              jsonValue: '"201xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"018xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"265xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"601xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"431xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"432xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"787xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"113xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"462xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"280xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"820xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"441xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"986xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"371xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"016xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"070xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"233xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"212xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"600xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"945xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"761xx"',
            },
            {
              estimate: "87256",
              jsonValue: '"273xx"',
            },
          ],
        },
        uniqueCount: {
          estimate: 875.0613905660648,
          upper: 889.4587588586493,
          lower: 861.1489446451884,
        },
      },
      num_actv_rev_tl: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -114.22581314577901,
          max: 148.31116266625548,
          mean: 5.8051634939960355,
          stddev: 7.64224726212388,
          histogram: {
            start: -114.22581481933594,
            end: 148.31117205767822,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "4160",
              "13312",
              "284675",
              "1366018",
              "463872",
              "120832",
              "26624",
              "12288",
              "0",
              "0",
              "8192",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 148.3111572265625,
            min: -114.22581481933594,
            bins: [
              -114.22581481933594, -105.47458192343547, -96.723349027535, -87.97211613163452, -79.22088323573405,
              -70.46965033983358, -61.718417443933106, -52.967184548032634, -44.21595165213216, -35.46471875623169,
              -26.713485860331218, -17.962252964430746, -9.211020068530274, -0.4597871726298024, 8.29144572327067,
              17.04267861917114, 25.793911515071613, 34.545144410972085, 43.29637730687256, 52.04761020277303,
              60.7988430986735, 69.55007599457397, 78.30130889047444, 87.05254178637492, 95.80377468227539,
              104.55500757817586, 113.30624047407633, 122.0574733699768, 130.80870626587728, 139.55993916177775,
              148.31117205767822,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2228102.4917465644,
            upper: 2263431.7310930244,
            lower: 2193320.468465876,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -114.22581481933594, -8.405773162841797, -3.1571686267852783, 1.3076515197753906, 4.38886833190918,
              8.886211395263672, 19.695589065551758, 33.62606430053711, 148.3111572265625,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "98230",
                value: 4.0,
                rank: 0,
              },
              {
                estimate: "96757",
                value: 5.0,
                rank: 1,
              },
              {
                estimate: "96568",
                value: 3.0,
                rank: 2,
              },
              {
                estimate: "93215",
                value: 6.0,
                rank: 3,
              },
              {
                estimate: "92969",
                value: 2.0,
                rank: 4,
              },
              {
                estimate: "91816",
                value: 7.0,
                rank: 5,
              },
              {
                estimate: "89663",
                value: 8.0,
                rank: 6,
              },
              {
                estimate: "87205",
                value: 9.0,
                rank: 7,
              },
              {
                estimate: "86898",
                value: 1.0,
                rank: 8,
              },
              {
                estimate: "86279",
                value: 10.0,
                rank: 9,
              },
              {
                estimate: "85484",
                value: 11.0,
                rank: 10,
              },
              {
                estimate: "84787",
                value: 12.0,
                rank: 11,
              },
              {
                estimate: "84689",
                value: 10.9329396425,
                rank: 12,
              },
              {
                estimate: "84689",
                value: 5.2203483513,
                rank: 13,
              },
              {
                estimate: "84689",
                value: 12.8663807634,
                rank: 14,
              },
              {
                estimate: "84689",
                value: -0.7966382783,
                rank: 15,
              },
              {
                estimate: "84689",
                value: 2.183366875,
                rank: 16,
              },
              {
                estimate: "84689",
                value: 26.9234042569,
                rank: 17,
              },
              {
                estimate: "84689",
                value: -1.2333601604,
                rank: 18,
              },
              {
                estimate: "84689",
                value: 5.6175792315,
                rank: 19,
              },
              {
                estimate: "84689",
                value: 3.4184759556,
                rank: 20,
              },
              {
                estimate: "84689",
                value: 3.5115651718,
                rank: 21,
              },
              {
                estimate: "84689",
                value: -1.8839899628,
                rank: 22,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "98230",
              jsonValue: "4.0",
            },
            {
              estimate: "96757",
              jsonValue: "5.0",
            },
            {
              estimate: "96568",
              jsonValue: "3.0",
            },
            {
              estimate: "93215",
              jsonValue: "6.0",
            },
            {
              estimate: "92969",
              jsonValue: "2.0",
            },
            {
              estimate: "91816",
              jsonValue: "7.0",
            },
            {
              estimate: "89663",
              jsonValue: "8.0",
            },
            {
              estimate: "87205",
              jsonValue: "9.0",
            },
            {
              estimate: "86898",
              jsonValue: "1.0",
            },
            {
              estimate: "86279",
              jsonValue: "10.0",
            },
            {
              estimate: "85484",
              jsonValue: "11.0",
            },
            {
              estimate: "84787",
              jsonValue: "12.0",
            },
            {
              estimate: "84689",
              jsonValue: "10.9329396425",
            },
            {
              estimate: "84689",
              jsonValue: "5.2203483513",
            },
            {
              estimate: "84689",
              jsonValue: "12.8663807634",
            },
            {
              estimate: "84689",
              jsonValue: "-0.7966382783",
            },
            {
              estimate: "84689",
              jsonValue: "2.183366875",
            },
            {
              estimate: "84689",
              jsonValue: "26.9234042569",
            },
            {
              estimate: "84689",
              jsonValue: "-1.2333601604",
            },
            {
              estimate: "84689",
              jsonValue: "5.6175792315",
            },
            {
              estimate: "84689",
              jsonValue: "3.4184759556",
            },
            {
              estimate: "84689",
              jsonValue: "3.5115651718",
            },
            {
              estimate: "84689",
              jsonValue: "-1.8839899628",
            },
          ],
        },
        uniqueCount: {
          estimate: 2218542.7543052477,
          upper: 2255044.395733988,
          lower: 2183270.5363498223,
        },
      },
      num_bc_sats: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -67.53536627511221,
          max: 132.68424992344723,
          mean: 4.8772747478432805,
          stddev: 6.626933054684046,
          histogram: {
            start: -67.53536987304688,
            end: 132.68426314635468,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "3072",
              "1024",
              "4162",
              "17408",
              "224256",
              "1283074",
              "538625",
              "150528",
              "48128",
              "17408",
              "8192",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 132.6842498779297,
            min: -67.53536987304688,
            bins: [
              -67.53536987304688, -60.86138210573349, -54.187394338420106, -47.513406571106714, -40.839418803793336,
              -34.165431036479944, -27.49144326916656, -20.817455501853175, -14.14346773453979, -7.469479967226405,
              -0.7954921999130136, 5.878495567400364, 12.552483334713756, 19.226471102027133, 25.900458869340525,
              32.5744466366539, 39.248434403967295, 45.92242217128069, 52.596409938594064, 59.270397705907456,
              65.94438547322085, 72.61837324053423, 79.2923610078476, 85.96634877516098, 92.64033654247439,
              99.31432430978776, 105.98831207710114, 112.66229984441455, 119.33628761172793, 126.0102753790413,
              132.68426314635468,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2229106.7657865086,
            upper: 2264451.943542132,
            lower: 2194309.051196192,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -67.53536987304688, -7.646890640258789, -2.72507381439209, 1.0, 3.5926673412323, 7.410587310791016,
              16.791210174560547, 27.9577579498291, 132.6842498779297,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "99879",
                value: 3.0,
                rank: 0,
              },
              {
                estimate: "98334",
                value: 4.0,
                rank: 1,
              },
              {
                estimate: "97406",
                value: 2.0,
                rank: 2,
              },
              {
                estimate: "94583",
                value: 5.0,
                rank: 3,
              },
              {
                estimate: "92422",
                value: 6.0,
                rank: 4,
              },
              {
                estimate: "90239",
                value: 7.0,
                rank: 5,
              },
              {
                estimate: "89996",
                value: 1.0,
                rank: 6,
              },
              {
                estimate: "87202",
                value: 8.0,
                rank: 7,
              },
              {
                estimate: "86847",
                value: 9.0,
                rank: 8,
              },
              {
                estimate: "85238",
                value: 10.0,
                rank: 9,
              },
              {
                estimate: "84675",
                value: 11.0,
                rank: 10,
              },
              {
                estimate: "84653",
                value: 13.0,
                rank: 11,
              },
              {
                estimate: "84408",
                value: -1.5797701878,
                rank: 12,
              },
              {
                estimate: "84408",
                value: 28.3205591463,
                rank: 13,
              },
              {
                estimate: "84408",
                value: 11.5853144152,
                rank: 14,
              },
              {
                estimate: "84408",
                value: 11.5541908961,
                rank: 15,
              },
              {
                estimate: "84408",
                value: 0.6942791746,
                rank: 16,
              },
              {
                estimate: "84408",
                value: 3.9985276358,
                rank: 17,
              },
              {
                estimate: "84408",
                value: 5.6510872,
                rank: 18,
              },
              {
                estimate: "84408",
                value: 8.2381297204,
                rank: 19,
              },
              {
                estimate: "84408",
                value: 1.2526240077,
                rank: 20,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "99879",
              jsonValue: "3.0",
            },
            {
              estimate: "98334",
              jsonValue: "4.0",
            },
            {
              estimate: "97406",
              jsonValue: "2.0",
            },
            {
              estimate: "94583",
              jsonValue: "5.0",
            },
            {
              estimate: "92422",
              jsonValue: "6.0",
            },
            {
              estimate: "90239",
              jsonValue: "7.0",
            },
            {
              estimate: "89996",
              jsonValue: "1.0",
            },
            {
              estimate: "87202",
              jsonValue: "8.0",
            },
            {
              estimate: "86847",
              jsonValue: "9.0",
            },
            {
              estimate: "85238",
              jsonValue: "10.0",
            },
            {
              estimate: "84675",
              jsonValue: "11.0",
            },
            {
              estimate: "84653",
              jsonValue: "13.0",
            },
            {
              estimate: "84408",
              jsonValue: "-1.5797701878",
            },
            {
              estimate: "84408",
              jsonValue: "28.3205591463",
            },
            {
              estimate: "84408",
              jsonValue: "11.5853144152",
            },
            {
              estimate: "84408",
              jsonValue: "11.5541908961",
            },
            {
              estimate: "84408",
              jsonValue: "0.6942791746",
            },
            {
              estimate: "84408",
              jsonValue: "3.9985276358",
            },
            {
              estimate: "84408",
              jsonValue: "5.6510872",
            },
            {
              estimate: "84408",
              jsonValue: "8.2381297204",
            },
            {
              estimate: "84408",
              jsonValue: "1.2526240077",
            },
          ],
        },
        uniqueCount: {
          estimate: 2230071.9908777527,
          upper: 2266763.322614917,
          lower: 2194616.471633923,
        },
      },
      loan_status: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2299973",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 7.0,
            upper: 7.0,
            lower: 7.0,
          },
          frequent: {
            items: [
              {
                value: "Fully Paid",
                estimate: 1403352.0,
              },
              {
                value: "Current",
                estimate: 475161.0,
              },
              {
                value: "Charged Off",
                estimate: 388881.0,
              },
              {
                value: "Late (31-120 days)",
                estimate: 19353.0,
              },
              {
                value: "In Grace Period",
                estimate: 9870.0,
              },
              {
                value: "Late (16-30 days)",
                estimate: 3260.0,
              },
              {
                value: "Default",
                estimate: 96.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "1403352",
              jsonValue: '"Fully Paid"',
            },
            {
              estimate: "475161",
              jsonValue: '"Current"',
            },
            {
              estimate: "388881",
              jsonValue: '"Charged Off"',
            },
            {
              estimate: "19353",
              jsonValue: '"Late (31-120 days)"',
            },
            {
              estimate: "9870",
              jsonValue: '"In Grace Period"',
            },
            {
              estimate: "3260",
              jsonValue: '"Late (16-30 days)"',
            },
            {
              estimate: "96",
              jsonValue: '"Default"',
            },
          ],
        },
        uniqueCount: {
          estimate: 7.000000104308129,
          upper: 7.000349609067664,
          lower: 7.0,
        },
      },
      total_pymnt: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2287226",
            NULL: "12747",
          },
        },
        numberSummary: {
          count: "2287226",
          min: -222787.7143968012,
          max: 237634.7648781488,
          mean: 15096.470559981663,
          stddev: 20506.776817064747,
          histogram: {
            start: -222787.71875,
            end: 237634.78938847655,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "576",
              "4096",
              "2064",
              "18466",
              "98308",
              "811009",
              "770050",
              "319488",
              "142337",
              "60416",
              "34816",
              "13312",
              "12288",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 237634.765625,
            min: -222787.71875,
            bins: [
              -222787.71875, -207440.30181205078, -192092.88487410155, -176745.46793615236, -161398.05099820314,
              -146050.6340602539, -130703.21712230469, -115355.80018435548, -100008.38324640626, -84660.96630845705,
              -69313.54937050783, -53966.1324325586, -38618.71549460938, -23271.298556660156, -7923.881618710962,
              7423.535319238261, 22770.952257187484, 38118.36919513671, 53465.7861330859, 68813.20307103515,
              84160.62000898435, 99508.0369469336, 114855.4538848828, 130202.87082283199, 145550.28776078124,
              160897.70469873043, 176245.1216366797, 191592.53857462888, 206939.95551257808, 222287.37245052733,
              237634.78938847652,
            ],
            n: "2287226",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2214716.7439138875,
            upper: 2249833.5437089824,
            lower: 2180143.866649204,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -222787.71875, -23368.326171875, -8354.2060546875, 2767.138427734375, 10546.2109375, 23211.18359375,
              54446.78125, 86943.4921875, 237634.765625,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "9321",
                value: -17841.9233530121,
                rank: 0,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "9321",
              jsonValue: "-17841.9233530121",
            },
          ],
        },
        uniqueCount: {
          estimate: 2228435.5897449884,
          upper: 2265099.9978056694,
          lower: 2193006.0873526726,
        },
      },
      "pred_credit_risk (output)": {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -1079.9710537847939,
          max: 1155.2995743549127,
          mean: 7.210881053889387,
          stddev: 210.44136102124844,
          histogram: {
            start: -1079.9710693359375,
            end: 1155.299676076831,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "1088",
              "0",
              "6144",
              "13312",
              "29696",
              "64514",
              "112640",
              "173057",
              "228352",
              "284673",
              "401409",
              "289792",
              "245760",
              "182272",
              "124928",
              "68608",
              "38912",
              "17408",
              "8192",
              "5120",
              "0",
              "4096",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 1155.299560546875,
            min: -1079.9710693359375,
            bins: [
              -1079.9710693359375, -1005.4620444888452, -930.9530196417529, -856.4439947946607, -781.9349699475683,
              -707.4259451004762, -632.9169202533839, -558.4078954062916, -483.8988705591993, -409.389845712107,
              -334.8808208650147, -260.3717960179224, -185.86277117083023, -111.35374632373794, -36.844721476645645,
              37.66430337044676, 112.17332821753894, 186.68235306463112, 261.1913779117235, 335.7004027588157,
              410.2094276059081, 484.7184524530003, 559.2274773000927, 633.7365021471849, 708.245526994277,
              782.7545518413694, 857.2635766884616, 931.772601535554, 1006.2816263826462, 1080.7906512297386,
              1155.299676076831,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2280433.5046081264,
            upper: 2316593.2673515137,
            lower: 2244833.833867179,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -1079.9710693359375, -473.3813781738281, -333.37060546875, -127.98072814941406, 8.016019821166992,
              144.92047119140625, 363.2667236328125, 508.0799865722656, 1155.299560546875,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "87256",
                value: -50.3563305279,
                rank: 0,
              },
              {
                estimate: "87256",
                value: -205.5223717076,
                rank: 1,
              },
              {
                estimate: "87256",
                value: 268.0946750169,
                rank: 2,
              },
              {
                estimate: "87256",
                value: -31.4662297756,
                rank: 3,
              },
              {
                estimate: "87256",
                value: -416.068162397,
                rank: 4,
              },
              {
                estimate: "87256",
                value: 228.547378379,
                rank: 5,
              },
              {
                estimate: "87256",
                value: 74.7908093964,
                rank: 6,
              },
              {
                estimate: "87256",
                value: -200.6318215693,
                rank: 7,
              },
              {
                estimate: "87256",
                value: 112.1460233757,
                rank: 8,
              },
              {
                estimate: "87256",
                value: 162.0918080416,
                rank: 9,
              },
              {
                estimate: "87256",
                value: 42.8885855683,
                rank: 10,
              },
              {
                estimate: "87256",
                value: -401.0453116339,
                rank: 11,
              },
              {
                estimate: "87256",
                value: 109.0095072938,
                rank: 12,
              },
              {
                estimate: "87256",
                value: -129.2570288157,
                rank: 13,
              },
              {
                estimate: "87256",
                value: 38.7298078904,
                rank: 14,
              },
              {
                estimate: "87256",
                value: -78.2877145132,
                rank: 15,
              },
              {
                estimate: "87256",
                value: -437.3793010568,
                rank: 16,
              },
              {
                estimate: "87256",
                value: 192.0115712277,
                rank: 17,
              },
              {
                estimate: "87256",
                value: 6.3126758627,
                rank: 18,
              },
              {
                estimate: "87256",
                value: 35.3623194132,
                rank: 19,
              },
              {
                estimate: "87256",
                value: -476.796981033,
                rank: 20,
              },
              {
                estimate: "87256",
                value: 192.7654070503,
                rank: 21,
              },
              {
                estimate: "87256",
                value: 131.3328788858,
                rank: 22,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "87256",
              jsonValue: "-50.3563305279",
            },
            {
              estimate: "87256",
              jsonValue: "-205.5223717076",
            },
            {
              estimate: "87256",
              jsonValue: "268.0946750169",
            },
            {
              estimate: "87256",
              jsonValue: "-31.4662297756",
            },
            {
              estimate: "87256",
              jsonValue: "-416.068162397",
            },
            {
              estimate: "87256",
              jsonValue: "228.547378379",
            },
            {
              estimate: "87256",
              jsonValue: "74.7908093964",
            },
            {
              estimate: "87256",
              jsonValue: "-200.6318215693",
            },
            {
              estimate: "87256",
              jsonValue: "112.1460233757",
            },
            {
              estimate: "87256",
              jsonValue: "162.0918080416",
            },
            {
              estimate: "87256",
              jsonValue: "42.8885855683",
            },
            {
              estimate: "87256",
              jsonValue: "-401.0453116339",
            },
            {
              estimate: "87256",
              jsonValue: "109.0095072938",
            },
            {
              estimate: "87256",
              jsonValue: "-129.2570288157",
            },
            {
              estimate: "87256",
              jsonValue: "38.7298078904",
            },
            {
              estimate: "87256",
              jsonValue: "-78.2877145132",
            },
            {
              estimate: "87256",
              jsonValue: "-437.3793010568",
            },
            {
              estimate: "87256",
              jsonValue: "192.0115712277",
            },
            {
              estimate: "87256",
              jsonValue: "6.3126758627",
            },
            {
              estimate: "87256",
              jsonValue: "35.3623194132",
            },
            {
              estimate: "87256",
              jsonValue: "-476.796981033",
            },
            {
              estimate: "87256",
              jsonValue: "192.7654070503",
            },
            {
              estimate: "87256",
              jsonValue: "131.3328788858",
            },
          ],
        },
        uniqueCount: {
          estimate: 2225714.5609578616,
          upper: 2262334.199984044,
          lower: 2190328.3197198585,
        },
      },
      funded_amnt_inv: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -118899.50832670648,
          max: 204605.59658268196,
          mean: 15276.724191717098,
          stddev: 19753.07989769484,
          histogram: {
            start: -118899.5078125,
            end: 204605.61421055938,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "4160",
              "12288",
              "13312",
              "60416",
              "250882",
              "770048",
              "533505",
              "292865",
              "163840",
              "91137",
              "47104",
              "30720",
              "16384",
              "9216",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 204605.59375,
            min: -118899.5078125,
            bins: [
              -118899.5078125, -108116.00374506469, -97332.49967762937, -86548.99561019406, -75765.49154275874,
              -64981.98747532343, -54198.483407888125, -43414.9793404528, -32631.475273017495, -21847.971205582187,
              -11064.467138146865, -280.9630707115575, 10502.54099672375, 21286.045064159058, 32069.549131594395,
              42853.0531990297, 53636.55726646501, 64420.06133390032, 75203.56540133563, 85987.06946877096,
              96770.57353620627, 107554.07760364158, 118337.58167107688, 129121.08573851219, 139904.5898059475,
              150688.09387338284, 161471.59794081812, 172255.10200825345, 183038.6060756888, 193822.11014312407,
              204605.6142105594,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2242875.5614063838,
            upper: 2278439.2579018595,
            lower: 2207862.715859686,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -118899.5078125, -26116.701171875, -8869.5234375, 3125.0, 11123.259765625, 23745.10546875, 52873.27734375,
              79776.8671875, 204605.59375,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "89942",
                value: 10000.0,
                rank: 0,
              },
              {
                estimate: "89151",
                value: 20000.0,
                rank: 1,
              },
              {
                estimate: "88244",
                value: 15000.0,
                rank: 2,
              },
              {
                estimate: "88149",
                value: 12000.0,
                rank: 3,
              },
              {
                estimate: "87271",
                value: 5000.0,
                rank: 4,
              },
              {
                estimate: "87037",
                value: 8000.0,
                rank: 5,
              },
              {
                estimate: "87028",
                value: 11628.3973474378,
                rank: 6,
              },
              {
                estimate: "87028",
                value: 10290.2099314096,
                rank: 7,
              },
              {
                estimate: "87028",
                value: 14153.0632817111,
                rank: 8,
              },
              {
                estimate: "87028",
                value: 26134.2581803143,
                rank: 9,
              },
              {
                estimate: "87028",
                value: 13735.4398915934,
                rank: 10,
              },
              {
                estimate: "87028",
                value: 71459.4870258051,
                rank: 11,
              },
              {
                estimate: "87028",
                value: 2302.2903783422,
                rank: 12,
              },
              {
                estimate: "87028",
                value: 18687.4409334794,
                rank: 13,
              },
              {
                estimate: "87028",
                value: 7672.658238273,
                rank: 14,
              },
              {
                estimate: "87028",
                value: 18332.8387278016,
                rank: 15,
              },
              {
                estimate: "87028",
                value: 32077.3833011251,
                rank: 16,
              },
              {
                estimate: "87028",
                value: 26811.0445205774,
                rank: 17,
              },
              {
                estimate: "87028",
                value: 24154.6991690391,
                rank: 18,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "89942",
              jsonValue: "10000.0",
            },
            {
              estimate: "89151",
              jsonValue: "20000.0",
            },
            {
              estimate: "88244",
              jsonValue: "15000.0",
            },
            {
              estimate: "88149",
              jsonValue: "12000.0",
            },
            {
              estimate: "87271",
              jsonValue: "5000.0",
            },
            {
              estimate: "87037",
              jsonValue: "8000.0",
            },
            {
              estimate: "87028",
              jsonValue: "11628.3973474378",
            },
            {
              estimate: "87028",
              jsonValue: "10290.2099314096",
            },
            {
              estimate: "87028",
              jsonValue: "14153.0632817111",
            },
            {
              estimate: "87028",
              jsonValue: "26134.2581803143",
            },
            {
              estimate: "87028",
              jsonValue: "13735.4398915934",
            },
            {
              estimate: "87028",
              jsonValue: "71459.4870258051",
            },
            {
              estimate: "87028",
              jsonValue: "2302.2903783422",
            },
            {
              estimate: "87028",
              jsonValue: "18687.4409334794",
            },
            {
              estimate: "87028",
              jsonValue: "7672.658238273",
            },
            {
              estimate: "87028",
              jsonValue: "18332.8387278016",
            },
            {
              estimate: "87028",
              jsonValue: "32077.3833011251",
            },
            {
              estimate: "87028",
              jsonValue: "26811.0445205774",
            },
            {
              estimate: "87028",
              jsonValue: "24154.6991690391",
            },
          ],
        },
        uniqueCount: {
          estimate: 2216645.1559804482,
          upper: 2253115.576260309,
          lower: 2181403.1076043895,
        },
      },
      last_fico_range_low: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -2883.748869732626,
          max: 4293.830734033962,
          mean: 664.1402215986125,
          stddev: 685.6467723863675,
          histogram: {
            start: -2883.748779296875,
            end: 4293.830995789307,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "2112",
              "9216",
              "17408",
              "21504",
              "51202",
              "91136",
              "151553",
              "267264",
              "268288",
              "355328",
              "321537",
              "250880",
              "193536",
              "130049",
              "77824",
              "47104",
              "25600",
              "13312",
              "4096",
              "0",
              "1024",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 4293.83056640625,
            min: -2883.748779296875,
            bins: [
              -2883.748779296875, -2644.4961201273354, -2405.2434609577963, -2165.9908017882567, -1926.7381426187173,
              -1687.485483449178, -1448.2328242796384, -1208.980165110099, -969.7275059405597, -730.4748467710201,
              -491.222187601481, -251.9695284319414, -12.716869262401815, 226.5357899071373, 465.7884490766769,
              705.041108246216, 944.2937674157556, 1183.5464265852952, 1422.7990857548348, 1662.0517449243735,
              1901.304404093913, 2140.5570632634526, 2379.809722432992, 2619.062381602532, 2858.3150407720714,
              3097.56769994161, 3336.8203591111496, 3576.073018280689, 3815.325677450229, 4054.5783366197684,
              4293.830995789307,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2165894.183239502,
            upper: 2200236.140781416,
            lower: 2132084.1355386777,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -2883.748779296875, -990.1796264648438, -458.24810791015625, 185.2867431640625, 659.1947021484375,
              1099.5645751953125, 1802.0792236328125, 2268.77490234375, 4293.83056640625,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "87379",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "87373",
                value: -36.4740310301,
                rank: 1,
              },
              {
                estimate: "87373",
                value: 203.5151033754,
                rank: 2,
              },
              {
                estimate: "87373",
                value: 753.2266031255,
                rank: 3,
              },
              {
                estimate: "87373",
                value: 619.3094276412,
                rank: 4,
              },
              {
                estimate: "87373",
                value: 1068.5570910143,
                rank: 5,
              },
              {
                estimate: "87373",
                value: 1178.0835075896,
                rank: 6,
              },
              {
                estimate: "87373",
                value: 442.1481270967,
                rank: 7,
              },
              {
                estimate: "87373",
                value: 393.7918776547,
                rank: 8,
              },
              {
                estimate: "87373",
                value: 1482.7639219147,
                rank: 9,
              },
              {
                estimate: "87373",
                value: 351.8490794915,
                rank: 10,
              },
              {
                estimate: "87373",
                value: 1509.7060675987,
                rank: 11,
              },
              {
                estimate: "87373",
                value: 212.5370043769,
                rank: 12,
              },
              {
                estimate: "87373",
                value: -188.388459662,
                rank: 13,
              },
              {
                estimate: "87373",
                value: 982.2842477999,
                rank: 14,
              },
              {
                estimate: "87373",
                value: 277.3063444895,
                rank: 15,
              },
              {
                estimate: "87373",
                value: 1509.4208124662,
                rank: 16,
              },
              {
                estimate: "87373",
                value: 1524.9071344871,
                rank: 17,
              },
              {
                estimate: "87373",
                value: 534.8836348936,
                rank: 18,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "87379",
              jsonValue: "0.0",
            },
            {
              estimate: "87373",
              jsonValue: "-36.4740310301",
            },
            {
              estimate: "87373",
              jsonValue: "203.5151033754",
            },
            {
              estimate: "87373",
              jsonValue: "753.2266031255",
            },
            {
              estimate: "87373",
              jsonValue: "619.3094276412",
            },
            {
              estimate: "87373",
              jsonValue: "1068.5570910143",
            },
            {
              estimate: "87373",
              jsonValue: "1178.0835075896",
            },
            {
              estimate: "87373",
              jsonValue: "442.1481270967",
            },
            {
              estimate: "87373",
              jsonValue: "393.7918776547",
            },
            {
              estimate: "87373",
              jsonValue: "1482.7639219147",
            },
            {
              estimate: "87373",
              jsonValue: "351.8490794915",
            },
            {
              estimate: "87373",
              jsonValue: "1509.7060675987",
            },
            {
              estimate: "87373",
              jsonValue: "212.5370043769",
            },
            {
              estimate: "87373",
              jsonValue: "-188.388459662",
            },
            {
              estimate: "87373",
              jsonValue: "982.2842477999",
            },
            {
              estimate: "87373",
              jsonValue: "277.3063444895",
            },
            {
              estimate: "87373",
              jsonValue: "1509.4208124662",
            },
            {
              estimate: "87373",
              jsonValue: "1524.9071344871",
            },
            {
              estimate: "87373",
              jsonValue: "534.8836348936",
            },
          ],
        },
        uniqueCount: {
          estimate: 2133682.9226084687,
          upper: 2168788.366852238,
          lower: 2099759.876073545,
        },
      },
      mo_sin_rcnt_rev_tl_op: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -568.3344002391646,
          max: 1012.2642066531698,
          mean: 13.139340167439675,
          stddev: 26.999820958775516,
          histogram: {
            start: -568.3344116210938,
            end: 1012.2643224178283,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "7234",
              "1509378",
              "689153",
              "64512",
              "20480",
              "5120",
              "0",
              "0",
              "0",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 1012.2642211914062,
            min: -568.3344116210938,
            bins: [
              -568.3344116210938, -515.6477871531297, -462.9611626851656, -410.27453821720155, -357.5879137492375,
              -304.9012892812734, -252.21466481330935, -199.5280403453453, -146.84141587738122, -94.15479140941716,
              -41.46816694145309, 11.218457526510974, 63.90508199447504, 116.5917064624391, 169.27833093040317,
              221.96495539836724, 274.6515798663313, 327.33820433429537, 380.02482880225944, 432.7114532702235,
              485.39807773818757, 538.0847022061516, 590.7713266741157, 643.4579511420798, 696.1445756100438,
              748.8312000780079, 801.517824545972, 854.204449013936, 906.8910734819001, 959.5776979498642,
              1012.2643224178282,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2168650.53994941,
            upper: 2203036.24246565,
            lower: 2134797.425470084,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -568.3344116210938, -22.581138610839844, -5.316562175750732, 1.3204576969146729, 5.875685691833496,
              16.318071365356445, 57.27971649169922, 124.36116790771484, 1012.2642211914062,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "92481",
                value: 3.0,
                rank: 0,
              },
              {
                estimate: "90884",
                value: 2.0,
                rank: 1,
              },
              {
                estimate: "90806",
                value: 4.0,
                rank: 2,
              },
              {
                estimate: "89423",
                value: 5.0,
                rank: 3,
              },
              {
                estimate: "88644",
                value: 1.0,
                rank: 4,
              },
              {
                estimate: "88561",
                value: 6.0,
                rank: 5,
              },
              {
                estimate: "88307",
                value: 7.0,
                rank: 6,
              },
              {
                estimate: "87898",
                value: 8.0,
                rank: 7,
              },
              {
                estimate: "87627",
                value: 10.0,
                rank: 8,
              },
              {
                estimate: "87339",
                value: 9.0,
                rank: 9,
              },
              {
                estimate: "87262",
                value: 13.3814235102,
                rank: 10,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "92481",
              jsonValue: "3.0",
            },
            {
              estimate: "90884",
              jsonValue: "2.0",
            },
            {
              estimate: "90806",
              jsonValue: "4.0",
            },
            {
              estimate: "89423",
              jsonValue: "5.0",
            },
            {
              estimate: "88644",
              jsonValue: "1.0",
            },
            {
              estimate: "88561",
              jsonValue: "6.0",
            },
            {
              estimate: "88307",
              jsonValue: "7.0",
            },
            {
              estimate: "87898",
              jsonValue: "8.0",
            },
            {
              estimate: "87627",
              jsonValue: "10.0",
            },
            {
              estimate: "87339",
              jsonValue: "9.0",
            },
            {
              estimate: "87262",
              jsonValue: "13.3814235102",
            },
          ],
        },
        uniqueCount: {
          estimate: 2175961.683836214,
          upper: 2211762.739728391,
          lower: 2141366.4547715564,
        },
      },
      mths_since_recent_bc_dlq: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            NULL: "1721348",
            FRACTIONAL: "578625",
          },
        },
        numberSummary: {
          count: "578625",
          min: -250.54673452987376,
          max: 516.4810381276321,
          mean: 38.320355234525245,
          stddev: 50.199173360965354,
          histogram: {
            start: -250.54673767089844,
            end: 516.4810697145081,
            counts: [
              "0",
              "0",
              "32",
              "2",
              "768",
              "2",
              "4112",
              "6404",
              "19456",
              "104192",
              "173572",
              "108036",
              "68608",
              "38656",
              "22785",
              "14848",
              "8704",
              "3328",
              "2048",
              "2048",
              "1024",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 516.4810180664062,
            min: -250.54673767089844,
            bins: [
              -250.54673767089844, -224.97914409138488, -199.41155051187133, -173.8439569323578, -148.27636335284424,
              -122.70876977333069, -97.14117619381716, -71.5735826143036, -46.00598903479005, -20.438395455276492,
              5.129198124237064, 30.69679170375059, 56.26438528326412, 81.8319788627777, 107.39957244229123,
              132.96716602180481, 158.53475960131834, 184.10235318083187, 209.66994676034545, 235.23754033985898,
              260.80513391937257, 286.3727274988861, 311.9403210783996, 337.50791465791315, 363.0755082374267,
              388.6431018169403, 414.21069539645384, 439.77828897596737, 465.3458825554809, 490.9134761349944,
              516.4810697145081,
            ],
            n: "578625",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 554960.7544030921,
            upper: 563736.2343522293,
            lower: 546320.8174775945,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -250.54673767089844, -64.6797866821289, -21.30082893371582, 6.245987892150879, 27.099409103393555,
              61.91588592529297, 136.13829040527344, 197.82276916503906, 516.4810180664062,
            ],
          },
          isDiscrete: false,
        },
        uniqueCount: {
          estimate: 545285.6661974537,
          upper: 554257.2408155883,
          lower: 536616.2660568606,
        },
      },
      pymnt_plan: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2299973",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 2.0,
            upper: 2.0,
            lower: 2.0,
          },
          frequent: {
            items: [
              {
                value: "n",
                estimate: 2299612.0,
              },
              {
                value: "y",
                estimate: 361.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "2299612",
              jsonValue: '"n"',
            },
            {
              estimate: "361",
              jsonValue: '"y"',
            },
          ],
        },
        uniqueCount: {
          estimate: 2.000000004967054,
          upper: 2.000099863468538,
          lower: 2.0,
        },
      },
      bc_util: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            NULL: "22167",
            FRACTIONAL: "2277806",
          },
        },
        numberSummary: {
          count: "2277806",
          min: -557.8924802854992,
          max: 860.8684052777886,
          mean: 76.46535999991474,
          stddev: 96.76744543151257,
          histogram: {
            start: -557.8924560546875,
            end: 860.8684942899658,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "384",
              "0",
              "6144",
              "12330",
              "29696",
              "86016",
              "396292",
              "591872",
              "457728",
              "275456",
              "179200",
              "105472",
              "59392",
              "36864",
              "20480",
              "8192",
              "8192",
              "0",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 860.868408203125,
            min: -557.8924560546875,
            bins: [
              -557.8924560546875, -510.6004243765324, -463.30839269837725, -416.0163610202222, -368.72432934206705,
              -321.4322976639119, -274.14026598575686, -226.84823430760173, -179.5562026294466, -132.26417095129148,
              -84.97213927313635, -37.68010759498122, 9.61192408317379, 56.903955761328916, 104.19598743948404,
              151.48801911763917, 198.7800507957943, 246.07208247394942, 293.36411415210455, 340.6561458302597,
              387.9481775084148, 435.2402091865698, 482.53224086472505, 529.8242725428802, 577.1163042210351,
              624.4083358991902, 671.7003675773453, 718.9923992555005, 766.2844309336556, 813.5764626118107,
              860.8684942899658,
            ],
            n: "2277806",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2171236.7635216573,
            upper: 2205663.5109009384,
            lower: 2137343.2405142128,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -557.8924560546875, -119.34078216552734, -45.313194274902344, 12.637314796447754, 58.44187545776367,
              123.80245208740234, 262.8714599609375, 385.40850830078125, 860.868408203125,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "88656",
                value: -42.3943283738,
                rank: 0,
              },
              {
                estimate: "88656",
                value: 20.003336053,
                rank: 1,
              },
              {
                estimate: "88656",
                value: 160.5469380716,
                rank: 2,
              },
              {
                estimate: "88656",
                value: 5.0635361966,
                rank: 3,
              },
              {
                estimate: "88656",
                value: 154.9254548882,
                rank: 4,
              },
              {
                estimate: "88656",
                value: 87.0753486862,
                rank: 5,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "88656",
              jsonValue: "-42.3943283738",
            },
            {
              estimate: "88656",
              jsonValue: "20.003336053",
            },
            {
              estimate: "88656",
              jsonValue: "160.5469380716",
            },
            {
              estimate: "88656",
              jsonValue: "5.0635361966",
            },
            {
              estimate: "88656",
              jsonValue: "154.9254548882",
            },
            {
              estimate: "88656",
              jsonValue: "87.0753486862",
            },
          ],
        },
        uniqueCount: {
          estimate: 2145619.671481937,
          upper: 2180921.5108262105,
          lower: 2111506.844692778,
        },
      },
      total_il_high_credit_limit: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -1502493.6019411,
          max: 2876442.8152629705,
          mean: 44923.28348991342,
          stddev: 77824.84524372312,
          histogram: {
            start: -1502493.625,
            end: 2876443.037644275,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "4160",
              "55296",
              "1922052",
              "263169",
              "38912",
              "8192",
              "4096",
              "0",
              "0",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 2876442.75,
            min: -1502493.625,
            bins: [
              -1502493.625, -1356529.0695785242, -1210564.5141570484, -1064599.9587355726, -918635.4033140967,
              -772670.847892621, -626706.292471145, -480741.73704966926, -334777.1816281935, -188812.6262067177,
              -42848.0707852419, 103116.48463623389, 249081.0400577099, 395045.5954791857, 541010.1509006615,
              686974.7063221373, 832939.261743613, 978903.8171650888, 1124868.3725865646, 1270832.9280080404,
              1416797.4834295162, 1562762.038850992, 1708726.5942724678, 1854691.1496939436, 2000655.7051154198,
              2146620.2605368956, 2292584.8159583714, 2438549.371379847, 2584513.926801323, 2730478.4822227983,
              2876443.0376442745,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 1958411.2822979519,
            upper: 1989460.3661084638,
            lower: 1927843.057593069,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -1502493.625, -78009.3828125, -23652.5390625, 101.7560806274414, 24586.990234375, 64766.0234375,
              183009.265625, 352914.15625, 2876442.75,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "253674",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "85263",
                value: 30258.9131826359,
                rank: 1,
              },
              {
                estimate: "85263",
                value: 24441.7219578978,
                rank: 2,
              },
              {
                estimate: "85263",
                value: 19045.8998187365,
                rank: 3,
              },
              {
                estimate: "85263",
                value: 26075.1426484867,
                rank: 4,
              },
              {
                estimate: "85263",
                value: 30737.6404044724,
                rank: 5,
              },
              {
                estimate: "85263",
                value: 22537.8997411159,
                rank: 6,
              },
              {
                estimate: "85263",
                value: 354184.2207171653,
                rank: 7,
              },
              {
                estimate: "85263",
                value: 19631.15576601,
                rank: 8,
              },
              {
                estimate: "85263",
                value: 12365.2186090389,
                rank: 9,
              },
              {
                estimate: "85263",
                value: 150938.4761888142,
                rank: 10,
              },
              {
                estimate: "85263",
                value: 2613.0818209006,
                rank: 11,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "253674",
              jsonValue: "0.0",
            },
            {
              estimate: "85263",
              jsonValue: "30258.9131826359",
            },
            {
              estimate: "85263",
              jsonValue: "24441.7219578978",
            },
            {
              estimate: "85263",
              jsonValue: "19045.8998187365",
            },
            {
              estimate: "85263",
              jsonValue: "26075.1426484867",
            },
            {
              estimate: "85263",
              jsonValue: "30737.6404044724",
            },
            {
              estimate: "85263",
              jsonValue: "22537.8997411159",
            },
            {
              estimate: "85263",
              jsonValue: "354184.2207171653",
            },
            {
              estimate: "85263",
              jsonValue: "19631.15576601",
            },
            {
              estimate: "85263",
              jsonValue: "12365.2186090389",
            },
            {
              estimate: "85263",
              jsonValue: "150938.4761888142",
            },
            {
              estimate: "85263",
              jsonValue: "2613.0818209006",
            },
          ],
        },
        uniqueCount: {
          estimate: 2000490.0771056057,
          upper: 2033404.102014325,
          lower: 1968684.6400093837,
        },
      },
      annual_inc: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -14155618.148562692,
          max: 31748723.32814128,
          mean: 78736.06436171144,
          stddev: 134458.5509652637,
          histogram: {
            start: -14155618.0,
            end: 31748727.1748724,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "2295877",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 31748724.0,
            min: -14155618.0,
            bins: [
              -14155618.0, -12625473.160837587, -11095328.321675174, -9565183.48251276, -8035038.643350347,
              -6504893.804187934, -4974748.96502552, -3444604.125863107, -1914459.286700694, -384314.44753828086,
              1145830.3916241322, 2675975.230786547, 4206120.06994896, 5736264.909111373, 7266409.748273786,
              8796554.5874362, 10326699.426598612, 11856844.265761025, 13386989.104923438, 14917133.944085851,
              16447278.783248264, 17977423.622410677, 19507568.461573094, 21037713.300735503, 22567858.13989792,
              24098002.97906033, 25628147.818222746, 27158292.657385156, 28688437.496547572, 30218582.33570998,
              31748727.1748724,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 1670824.1778491966,
            upper: 1697309.0871930362,
            lower: 1644749.3674030795,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -14155618.0, -104574.2265625, -32392.99609375, 23015.220703125, 65000.0, 128400.7578125, 225970.859375,
              417395.34375, 31748724.0,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "133327",
                value: 135954.1162644805,
                rank: 0,
              },
              {
                estimate: "132256",
                value: 133566.6340444061,
                rank: 1,
              },
              {
                estimate: "119164",
                value: 133692.2723547379,
                rank: 2,
              },
              {
                estimate: "109248",
                value: 42246.0957223763,
                rank: 3,
              },
              {
                estimate: "108906",
                value: 7889.84292416,
                rank: 4,
              },
              {
                estimate: "108905",
                value: 78499.6141478433,
                rank: 5,
              },
              {
                estimate: "108542",
                value: 8412.2271428686,
                rank: 6,
              },
              {
                estimate: "108541",
                value: 42099.0508519649,
                rank: 7,
              },
              {
                estimate: "108540",
                value: 77951.5464589708,
                rank: 8,
              },
              {
                estimate: "98701",
                value: 164181.2531654219,
                rank: 9,
              },
              {
                estimate: "98000",
                value: 136385.2836124664,
                rank: 10,
              },
              {
                estimate: "95064",
                value: 68961.0156325812,
                rank: 11,
              },
              {
                estimate: "95064",
                value: 102598.4286222426,
                rank: 12,
              },
              {
                estimate: "95064",
                value: 32058.5228472235,
                rank: 13,
              },
              {
                estimate: "95064",
                value: -2054.6774476682,
                rank: 14,
              },
              {
                estimate: "95064",
                value: 14628.1294964559,
                rank: 15,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "133327",
              jsonValue: "135954.1162644805",
            },
            {
              estimate: "132256",
              jsonValue: "133566.6340444061",
            },
            {
              estimate: "119164",
              jsonValue: "133692.2723547379",
            },
            {
              estimate: "109248",
              jsonValue: "42246.0957223763",
            },
            {
              estimate: "108906",
              jsonValue: "7889.84292416",
            },
            {
              estimate: "108905",
              jsonValue: "78499.6141478433",
            },
            {
              estimate: "108542",
              jsonValue: "8412.2271428686",
            },
            {
              estimate: "108541",
              jsonValue: "42099.0508519649",
            },
            {
              estimate: "108540",
              jsonValue: "77951.5464589708",
            },
            {
              estimate: "98701",
              jsonValue: "164181.2531654219",
            },
            {
              estimate: "98000",
              jsonValue: "136385.2836124664",
            },
            {
              estimate: "95064",
              jsonValue: "68961.0156325812",
            },
            {
              estimate: "95064",
              jsonValue: "102598.4286222426",
            },
            {
              estimate: "95064",
              jsonValue: "32058.5228472235",
            },
            {
              estimate: "95064",
              jsonValue: "-2054.6774476682",
            },
            {
              estimate: "95064",
              jsonValue: "14628.1294964559",
            },
          ],
        },
        uniqueCount: {
          estimate: 1672471.1547179914,
          upper: 1699988.2905815933,
          lower: 1645880.832318796,
        },
      },
      inq_last_6mths: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -13.20417335713504,
          max: 25.025989261596717,
          mean: 0.5688587612632812,
          stddev: 1.351971627817433,
          histogram: {
            start: -13.20417308807373,
            end: 25.025992035069656,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "3136",
              "9216",
              "60418",
              "1654787",
              "338944",
              "134144",
              "51200",
              "22528",
              "12288",
              "5120",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 25.025989532470703,
            min: -13.20417308807373,
            bins: [
              -13.20417308807373, -11.929834250635619, -10.655495413197505, -9.381156575759391, -8.10681773832128,
              -6.832478900883166, -5.5581400634450535, -4.28380122600694, -3.009462388568828, -1.735123551130716,
              -0.4607847136926022, 0.8135541237455115, 2.0878929611836234, 3.3622317986217354, 4.636570636059851,
              5.910909473497963, 7.185248310936075, 8.459587148374187, 9.733925985812299, 11.008264823250414,
              12.282603660688526, 13.556942498126638, 14.831281335564753, 16.105620173002865, 17.379959010440977,
              18.65429784787909, 19.9286366853172, 21.202975522755317, 22.477314360193432, 23.75165319763154,
              25.025992035069656,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 855143.347614538,
            upper: 868682.9175966437,
            lower: 841813.1799599263,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -13.20417308807373, -1.3855648040771484, -0.16659121215343475, 0.0, 0.0, 0.8061606884002686,
              3.059906244277954, 6.004872798919678, 25.025989532470703,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "1410884",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "59776",
                value: 1.0,
                rank: 1,
              },
              {
                estimate: "43296",
                value: 2.0,
                rank: 2,
              },
              {
                estimate: "38154",
                value: 3.0,
                rank: 3,
              },
              {
                estimate: "36198",
                value: 4.0,
                rank: 4,
              },
              {
                estimate: "35885",
                value: 5.0,
                rank: 5,
              },
              {
                estimate: "35568",
                value: 2.1377419524,
                rank: 6,
              },
              {
                estimate: "35568",
                value: 1.9974620599000001,
                rank: 7,
              },
              {
                estimate: "35568",
                value: 1.0001024346,
                rank: 8,
              },
              {
                estimate: "35568",
                value: 1.260250192,
                rank: 9,
              },
              {
                estimate: "35568",
                value: 3.0117914508,
                rank: 10,
              },
              {
                estimate: "35568",
                value: 4.5556700993,
                rank: 11,
              },
              {
                estimate: "35568",
                value: 1.9489522358,
                rank: 12,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "1410884",
              jsonValue: "0.0",
            },
            {
              estimate: "59776",
              jsonValue: "1.0",
            },
            {
              estimate: "43296",
              jsonValue: "2.0",
            },
            {
              estimate: "38154",
              jsonValue: "3.0",
            },
            {
              estimate: "36198",
              jsonValue: "4.0",
            },
            {
              estimate: "35885",
              jsonValue: "5.0",
            },
            {
              estimate: "35568",
              jsonValue: "2.1377419524",
            },
            {
              estimate: "35568",
              jsonValue: "1.9974620599",
            },
            {
              estimate: "35568",
              jsonValue: "1.0001024346",
            },
            {
              estimate: "35568",
              jsonValue: "1.260250192",
            },
            {
              estimate: "35568",
              jsonValue: "3.0117914508",
            },
            {
              estimate: "35568",
              jsonValue: "4.5556700993",
            },
            {
              estimate: "35568",
              jsonValue: "1.9489522358",
            },
          ],
        },
        uniqueCount: {
          estimate: 845587.6213879511,
          upper: 859500.058320973,
          lower: 832143.7736982339,
        },
      },
      url: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2299973",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 48211.25473144852,
            upper: 48943.67983512327,
            lower: 47489.68791390898,
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=75458175"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=74535905"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=75282481"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=74615936"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=73087637"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=74541637"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=75244988"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=74704498"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=75749896"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=75244215"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=73745851"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=74652319"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=74684177"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=75179488"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=71003484"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=75303363"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=73649161"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=74461260"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=72552408"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=73469489"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=75199393"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=72847490"',
            },
            {
              estimate: "87256",
              jsonValue: '"https:\\/\\/lendingclub.com\\/browse\\/loanDetail.action?loan_id=74613117"',
            },
          ],
        },
        uniqueCount: {
          estimate: 48147.376890236585,
          upper: 48939.54475969565,
          lower: 47381.89028045238,
        },
      },
      verification_status: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2299973",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 3.0,
            upper: 3.0,
            lower: 3.0,
          },
          frequent: {
            items: [
              {
                value: "Source Verified",
                estimate: 939379.0,
              },
              {
                value: "Not Verified",
                estimate: 763507.0,
              },
              {
                value: "Verified",
                estimate: 597087.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "939379",
              jsonValue: '"Source Verified"',
            },
            {
              estimate: "763507",
              jsonValue: '"Not Verified"',
            },
            {
              estimate: "597087",
              jsonValue: '"Verified"',
            },
          ],
        },
        uniqueCount: {
          estimate: 3.000000014901161,
          upper: 3.0001498026537594,
          lower: 3.0,
        },
      },
      tax_liens: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -87.76156456576912,
          max: 128.1937192912652,
          mean: 0.07112322450525345,
          stddev: 0.6691281948896389,
          histogram: {
            start: -87.76156616210938,
            end: 128.19373840531006,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "64",
              "2295813",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 128.1937255859375,
            min: -87.76156616210938,
            bins: [
              -87.76156616210938, -80.56305600986207, -73.36454585761474, -66.16603570536743, -58.96752555312012,
              -51.7690154008728, -44.570505248625494, -37.37199509637818, -30.173484944130863, -22.974974791883554,
              -15.776464639636231, -8.577954487388922, -1.3794443351416135, 5.8190658171057095, 13.017575969353018,
              20.21608612160034, 27.41459627384765, 34.61310642609496, 41.81161657834227, 49.010126730589604,
              56.20863688283691, 63.40714703508422, 70.60565718733153, 77.80416733957884, 85.00267749182615,
              92.20118764407349, 99.3996977963208, 106.5982079485681, 113.79671810081541, 120.99522825306272,
              128.19373840531006,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 94335.86682326437,
            upper: 95800.67092343503,
            lower: 92893.26991723172,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [-87.76156616210938, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.572418212890625, 128.1937255859375],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "2200990",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "6660",
                value: 1.0,
                rank: 1,
              },
              {
                estimate: "4879",
                value: 2.0,
                rank: 2,
              },
              {
                estimate: "4207",
                value: 3.0,
                rank: 3,
              },
              {
                estimate: "4075",
                value: 4.0,
                rank: 4,
              },
              {
                estimate: "3987",
                value: 6.0,
                rank: 5,
              },
              {
                estimate: "3980",
                value: 5.0,
                rank: 6,
              },
              {
                estimate: "3956",
                value: 0.0329761759,
                rank: 7,
              },
              {
                estimate: "3956",
                value: 3.4469135687,
                rank: 8,
              },
              {
                estimate: "3956",
                value: 0.7303620091,
                rank: 9,
              },
              {
                estimate: "3956",
                value: 1.0496831614,
                rank: 10,
              },
              {
                estimate: "3956",
                value: 1.1932185357,
                rank: 11,
              },
            ],
            longs: [],
          },
          isDiscrete: true,
        },
        frequentItems: {
          items: [
            {
              estimate: "2200990",
              jsonValue: "0.0",
            },
            {
              estimate: "6660",
              jsonValue: "1.0",
            },
            {
              estimate: "4879",
              jsonValue: "2.0",
            },
            {
              estimate: "4207",
              jsonValue: "3.0",
            },
            {
              estimate: "4075",
              jsonValue: "4.0",
            },
            {
              estimate: "3987",
              jsonValue: "6.0",
            },
            {
              estimate: "3980",
              jsonValue: "5.0",
            },
            {
              estimate: "3956",
              jsonValue: "0.0329761759",
            },
            {
              estimate: "3956",
              jsonValue: "3.4469135687",
            },
            {
              estimate: "3956",
              jsonValue: "0.7303620091",
            },
            {
              estimate: "3956",
              jsonValue: "1.0496831614",
            },
            {
              estimate: "3956",
              jsonValue: "1.1932185357",
            },
          ],
        },
        uniqueCount: {
          estimate: 94942.85344147298,
          upper: 96504.9463900163,
          lower: 93433.37384573431,
        },
      },
      tot_coll_amt: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -202233.72704672508,
          max: 533203.8862576943,
          mean: 271.1227954781919,
          stddev: 2896.3206705100592,
          histogram: {
            start: -202233.734375,
            end: 533203.9283203875,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "2290757",
              "4096",
              "5120",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 533203.875,
            min: -202233.734375,
            bins: [
              -202233.734375, -177719.1456184871, -153204.55686197418, -128689.96810546124, -104175.37934894834,
              -79660.79059243543, -55146.20183592249, -30631.61307940958, -6117.024322896672, 18397.564433616237,
              42912.153190129146, 67426.74194664205, 91941.33070315502, 116455.91945966793, 140970.50821618084,
              165485.09697269375, 189999.68572920666, 214514.27448571956, 239028.86324223247, 263543.4519987454,
              288058.0407552583, 312572.62951177126, 337087.2182682841, 361601.8070247971, 386116.39578131004,
              410630.9845378229, 435145.57329433586, 459660.1620508487, 484174.7508073617, 508689.3395638745,
              533203.9283203875,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 367867.3054066253,
            upper: 373673.4751670004,
            lower: 362150.64471307304,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [-202233.734375, -184.7022247314453, 0.0, -0.0, 0.0, 0.0, 939.0, 7738.21875, 533203.875],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "1916968",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "15959",
                value: -1.4875640632,
                rank: 1,
              },
              {
                estimate: "15959",
                value: 622.084415361,
                rank: 2,
              },
              {
                estimate: "15959",
                value: 781.6576246732,
                rank: 3,
              },
              {
                estimate: "15959",
                value: -312.9095465171,
                rank: 4,
              },
              {
                estimate: "15959",
                value: 30.3317846178,
                rank: 5,
              },
              {
                estimate: "15959",
                value: -1168.879361963,
                rank: 6,
              },
              {
                estimate: "15959",
                value: 252.9123738719,
                rank: 7,
              },
              {
                estimate: "15959",
                value: 6401.8844245465,
                rank: 8,
              },
              {
                estimate: "15959",
                value: 703.6764967725,
                rank: 9,
              },
              {
                estimate: "15959",
                value: 2462.2643075029,
                rank: 10,
              },
              {
                estimate: "15959",
                value: 216.5289687389,
                rank: 11,
              },
              {
                estimate: "15959",
                value: 1.1196146499,
                rank: 12,
              },
              {
                estimate: "15959",
                value: 800.091674016,
                rank: 13,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "1916968",
              jsonValue: "0.0",
            },
            {
              estimate: "15959",
              jsonValue: "-1.4875640632",
            },
            {
              estimate: "15959",
              jsonValue: "622.084415361",
            },
            {
              estimate: "15959",
              jsonValue: "781.6576246732",
            },
            {
              estimate: "15959",
              jsonValue: "-312.9095465171",
            },
            {
              estimate: "15959",
              jsonValue: "30.3317846178",
            },
            {
              estimate: "15959",
              jsonValue: "-1168.879361963",
            },
            {
              estimate: "15959",
              jsonValue: "252.9123738719",
            },
            {
              estimate: "15959",
              jsonValue: "6401.8844245465",
            },
            {
              estimate: "15959",
              jsonValue: "703.6764967725",
            },
            {
              estimate: "15959",
              jsonValue: "2462.2643075029",
            },
            {
              estimate: "15959",
              jsonValue: "216.5289687389",
            },
            {
              estimate: "15959",
              jsonValue: "1.1196146499",
            },
            {
              estimate: "15959",
              jsonValue: "800.091674016",
            },
          ],
        },
        uniqueCount: {
          estimate: 369290.7961129239,
          upper: 375366.73050566413,
          lower: 363419.50721206755,
        },
      },
      num_rev_tl_bal_gt_0: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -89.70874543379372,
          max: 181.0738063279805,
          mean: 5.711809839022684,
          stddev: 7.450070247428792,
          histogram: {
            start: -89.70874786376953,
            end: 181.07382487007598,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "2112",
              "18432",
              "427011",
              "1348610",
              "373760",
              "99328",
              "21504",
              "1024",
              "4096",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 181.0738067626953,
            min: -89.70874786376953,
            bins: [
              -89.70874786376953, -80.68266210597469, -71.65657634817983, -62.63049059038498, -53.60440483259013,
              -44.578319074795274, -35.55223331700043, -26.526147559205576, -17.500061801410723, -8.473976043615878,
              0.5521097141789824, 9.578195471973828, 18.604281229768674, 27.630366987563534, 36.65645274535838,
              45.68253850315324, 54.708624260948085, 63.73471001874293, 72.76079577653778, 81.78688153433265,
              90.8129672921275, 99.83905304992234, 108.86513880771719, 117.89122456551203, 126.91731032330688,
              135.94339608110175, 144.9694818388966, 153.99556759669144, 163.0216533544863, 172.04773911228114,
              181.073824870076,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2172912.4414196317,
            upper: 2207365.782775745,
            lower: 2138992.7367324107,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -89.70874786376953, -8.340421676635742, -3.367603063583374, 1.2539921998977661, 4.366695880889893,
              8.80538272857666, 19.391862869262695, 30.938377380371094, 181.0738067626953,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "98451",
                value: 4.0,
                rank: 0,
              },
              {
                estimate: "97285",
                value: 5.0,
                rank: 1,
              },
              {
                estimate: "96735",
                value: 3.0,
                rank: 2,
              },
              {
                estimate: "93190",
                value: 6.0,
                rank: 3,
              },
              {
                estimate: "92918",
                value: 2.0,
                rank: 4,
              },
              {
                estimate: "91745",
                value: 7.0,
                rank: 5,
              },
              {
                estimate: "89671",
                value: 8.0,
                rank: 6,
              },
              {
                estimate: "87280",
                value: 9.0,
                rank: 7,
              },
              {
                estimate: "86899",
                value: 1.0,
                rank: 8,
              },
              {
                estimate: "86285",
                value: 10.0,
                rank: 9,
              },
              {
                estimate: "85544",
                value: 11.0,
                rank: 10,
              },
              {
                estimate: "84648",
                value: 12.0,
                rank: 11,
              },
              {
                estimate: "84568",
                value: -3.2161156458,
                rank: 12,
              },
              {
                estimate: "84568",
                value: 17.381592708,
                rank: 13,
              },
              {
                estimate: "84568",
                value: 26.4324499889,
                rank: 14,
              },
              {
                estimate: "84568",
                value: -3.3511322313,
                rank: 15,
              },
              {
                estimate: "84568",
                value: 2.8046882241,
                rank: 16,
              },
              {
                estimate: "84568",
                value: 2.3339990868,
                rank: 17,
              },
              {
                estimate: "84568",
                value: 8.1238572581,
                rank: 18,
              },
              {
                estimate: "84568",
                value: 10.7919915768,
                rank: 19,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "98451",
              jsonValue: "4.0",
            },
            {
              estimate: "97285",
              jsonValue: "5.0",
            },
            {
              estimate: "96735",
              jsonValue: "3.0",
            },
            {
              estimate: "93190",
              jsonValue: "6.0",
            },
            {
              estimate: "92918",
              jsonValue: "2.0",
            },
            {
              estimate: "91745",
              jsonValue: "7.0",
            },
            {
              estimate: "89671",
              jsonValue: "8.0",
            },
            {
              estimate: "87280",
              jsonValue: "9.0",
            },
            {
              estimate: "86899",
              jsonValue: "1.0",
            },
            {
              estimate: "86285",
              jsonValue: "10.0",
            },
            {
              estimate: "85544",
              jsonValue: "11.0",
            },
            {
              estimate: "84648",
              jsonValue: "12.0",
            },
            {
              estimate: "84568",
              jsonValue: "-3.2161156458",
            },
            {
              estimate: "84568",
              jsonValue: "17.381592708",
            },
            {
              estimate: "84568",
              jsonValue: "26.4324499889",
            },
            {
              estimate: "84568",
              jsonValue: "-3.3511322313",
            },
            {
              estimate: "84568",
              jsonValue: "2.8046882241",
            },
            {
              estimate: "84568",
              jsonValue: "2.3339990868",
            },
            {
              estimate: "84568",
              jsonValue: "8.1238572581",
            },
            {
              estimate: "84568",
              jsonValue: "10.7919915768",
            },
          ],
        },
        uniqueCount: {
          estimate: 2214204.423633813,
          upper: 2250634.6865911395,
          lower: 2179001.180028649,
        },
      },
      num_rev_accts: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -154.62960099249543,
          max: 318.5509282982732,
          mean: 14.458914889754316,
          stddev: 18.57147310302088,
          histogram: {
            start: -154.62960815429688,
            end: 318.550965692984,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "3136",
              "9216",
              "56322",
              "487425",
              "1025025",
              "451585",
              "163840",
              "60416",
              "26624",
              "8192",
              "4096",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 318.5509338378906,
            min: -154.62960815429688,
            bins: [
              -154.62960815429688, -138.8569223593875, -123.08423656447815, -107.31155076956878, -91.53886497465942,
              -75.76617917975007, -59.993493384840704, -44.22080758993134, -28.448121795021976, -12.675436000112626,
              3.0972497947967383, 18.869935589706103, 34.64262138461547, 50.41530717952483, 66.1879929744342,
              81.96067876934356, 97.73336456425292, 113.50605035916226, 129.27873615407162, 145.051421948981,
              160.82410774389035, 176.59679353879972, 192.36947933370908, 208.14216512861844, 223.9148509235278,
              239.68753671843717, 255.46022251334654, 271.2329083082559, 287.00559410316527, 302.77827989807463,
              318.550965692984,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2186289.606589704,
            upper: 2220955.251288818,
            lower: 2152160.889983096,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -154.62960815429688, -21.217599868774414, -8.091651916503906, 3.3906450271606445, 11.051704406738281,
              22.269304275512695, 49.18085861206055, 78.21501159667969, 318.5509338378906,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "89331",
                value: 7.0,
                rank: 0,
              },
              {
                estimate: "89265",
                value: 8.0,
                rank: 1,
              },
              {
                estimate: "88957",
                value: 9.0,
                rank: 2,
              },
              {
                estimate: "88812",
                value: 12.0,
                rank: 3,
              },
              {
                estimate: "88664",
                value: 10.0,
                rank: 4,
              },
              {
                estimate: "88637",
                value: 13.0,
                rank: 5,
              },
              {
                estimate: "88190",
                value: 12.029596345,
                rank: 6,
              },
              {
                estimate: "88190",
                value: 0.6692356890000001,
                rank: 7,
              },
              {
                estimate: "88190",
                value: -1.3288565937,
                rank: 8,
              },
              {
                estimate: "88190",
                value: -16.896227240199998,
                rank: 9,
              },
              {
                estimate: "88190",
                value: 9.6259740772,
                rank: 10,
              },
              {
                estimate: "88190",
                value: 15.0,
                rank: 11,
              },
              {
                estimate: "88190",
                value: 0.336094379,
                rank: 12,
              },
              {
                estimate: "88190",
                value: 19.6099565366,
                rank: 13,
              },
              {
                estimate: "88190",
                value: 11.2134625527,
                rank: 14,
              },
              {
                estimate: "88190",
                value: -1.7212521982,
                rank: 15,
              },
              {
                estimate: "88190",
                value: 2.6318425792999998,
                rank: 16,
              },
              {
                estimate: "88190",
                value: 8.9354962879,
                rank: 17,
              },
              {
                estimate: "88190",
                value: -10.4877179467,
                rank: 18,
              },
              {
                estimate: "88190",
                value: 33.7078672165,
                rank: 19,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "89331",
              jsonValue: "7.0",
            },
            {
              estimate: "89265",
              jsonValue: "8.0",
            },
            {
              estimate: "88957",
              jsonValue: "9.0",
            },
            {
              estimate: "88812",
              jsonValue: "12.0",
            },
            {
              estimate: "88664",
              jsonValue: "10.0",
            },
            {
              estimate: "88637",
              jsonValue: "13.0",
            },
            {
              estimate: "88190",
              jsonValue: "12.029596345",
            },
            {
              estimate: "88190",
              jsonValue: "0.669235689",
            },
            {
              estimate: "88190",
              jsonValue: "-1.3288565937",
            },
            {
              estimate: "88190",
              jsonValue: "-16.8962272402",
            },
            {
              estimate: "88190",
              jsonValue: "9.6259740772",
            },
            {
              estimate: "88190",
              jsonValue: "15.0",
            },
            {
              estimate: "88190",
              jsonValue: "0.336094379",
            },
            {
              estimate: "88190",
              jsonValue: "19.6099565366",
            },
            {
              estimate: "88190",
              jsonValue: "11.2134625527",
            },
            {
              estimate: "88190",
              jsonValue: "-1.7212521982",
            },
            {
              estimate: "88190",
              jsonValue: "2.6318425793",
            },
            {
              estimate: "88190",
              jsonValue: "8.9354962879",
            },
            {
              estimate: "88190",
              jsonValue: "-10.4877179467",
            },
            {
              estimate: "88190",
              jsonValue: "33.7078672165",
            },
          ],
        },
        uniqueCount: {
          estimate: 2150259.2711944925,
          upper: 2185637.44578394,
          lower: 2116072.680231995,
        },
      },
      last_credit_pull_d: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            NULL: "48",
            STRING: "2299925",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 38.0,
            upper: 38.0,
            lower: 38.0,
          },
          frequent: {
            items: [
              {
                value: "Mar-2019",
                estimate: 1296482.0,
              },
              {
                value: "Feb-2019",
                estimate: 116070.0,
              },
              {
                value: "Jan-2019",
                estimate: 72958.0,
              },
              {
                value: "Jul-2018",
                estimate: 66570.0,
              },
              {
                value: "Oct-2018",
                estimate: 59595.0,
              },
              {
                value: "Aug-2018",
                estimate: 53335.0,
              },
              {
                value: "Dec-2018",
                estimate: 52913.0,
              },
              {
                value: "Nov-2018",
                estimate: 49190.0,
              },
              {
                value: "Sep-2018",
                estimate: 47028.0,
              },
              {
                value: "Mar-2018",
                estimate: 46589.0,
              },
              {
                value: "Apr-2018",
                estimate: 46588.0,
              },
              {
                value: "Oct-2017",
                estimate: 46586.0,
              },
              {
                value: "Jul-2017",
                estimate: 46583.0,
              },
              {
                value: "Nov-2017",
                estimate: 46583.0,
              },
              {
                value: "May-2018",
                estimate: 46583.0,
              },
              {
                value: "Aug-2017",
                estimate: 46583.0,
              },
              {
                value: "Sep-2017",
                estimate: 46583.0,
              },
              {
                value: "Feb-2018",
                estimate: 46583.0,
              },
              {
                value: "Dec-2017",
                estimate: 46583.0,
              },
              {
                value: "May-2017",
                estimate: 46583.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "1296482",
              jsonValue: '"Mar-2019"',
            },
            {
              estimate: "116070",
              jsonValue: '"Feb-2019"',
            },
            {
              estimate: "72958",
              jsonValue: '"Jan-2019"',
            },
            {
              estimate: "66570",
              jsonValue: '"Jul-2018"',
            },
            {
              estimate: "59595",
              jsonValue: '"Oct-2018"',
            },
            {
              estimate: "53335",
              jsonValue: '"Aug-2018"',
            },
            {
              estimate: "52913",
              jsonValue: '"Dec-2018"',
            },
            {
              estimate: "49190",
              jsonValue: '"Nov-2018"',
            },
            {
              estimate: "47028",
              jsonValue: '"Sep-2018"',
            },
            {
              estimate: "46588",
              jsonValue: '"Apr-2018"',
            },
            {
              estimate: "46586",
              jsonValue: '"Mar-2018"',
            },
            {
              estimate: "46578",
              jsonValue: '"May-2018"',
            },
            {
              estimate: "46578",
              jsonValue: '"Oct-2017"',
            },
            {
              estimate: "46577",
              jsonValue: '"Sep-2017"',
            },
            {
              estimate: "46575",
              jsonValue: '"May-2017"',
            },
            {
              estimate: "46575",
              jsonValue: '"Apr-2016"',
            },
            {
              estimate: "46575",
              jsonValue: '"Nov-2016"',
            },
            {
              estimate: "46575",
              jsonValue: '"Jun-2017"',
            },
            {
              estimate: "46575",
              jsonValue: '"Jul-2017"',
            },
            {
              estimate: "46575",
              jsonValue: '"Feb-2018"',
            },
            {
              estimate: "46575",
              jsonValue: '"Dec-2017"',
            },
            {
              estimate: "46575",
              jsonValue: '"Nov-2017"',
            },
            {
              estimate: "46575",
              jsonValue: '"Aug-2017"',
            },
          ],
        },
        uniqueCount: {
          estimate: 38.00000349183915,
          upper: 38.001900803536984,
          lower: 38.0,
        },
      },
      num_tl_30dpd: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -4.092859011949553,
          max: 8.617382465910595,
          mean: 0.004689184594644055,
          stddev: 0.0991430931485654,
          histogram: {
            start: -4.092858791351318,
            end: 8.617382911298751,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "64",
              "2290693",
              "0",
              "0",
              "8192",
              "0",
              "0",
              "0",
              "0",
              "1024",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 8.617382049560547,
            min: -4.092858791351318,
            bins: [
              -4.092858791351318, -3.6691840679296495, -3.2455093445079806, -2.8218346210863112, -2.3981598976646423,
              -1.9744851742429734, -1.550810450821304, -1.1271357273996352, -0.7034610039779663, -0.2797862805562974,
              0.1438884428653715, 0.5675631662870408, 0.9912378897087102, 1.4149126131303786, 1.838587336552048,
              2.2622620599737164, 2.6859367833953858, 3.109611506817055, 3.5332862302387236, 3.956960953660392,
              4.380635677082061, 4.804310400503731, 5.2279851239254, 5.651659847347069, 6.075334570768739,
              6.499009294190406, 6.922684017612076, 7.346358741033745, 7.770033464455414, 8.193708187877084,
              8.617382911298751,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 9963.364611834091,
            upper: 10085.272581941384,
            lower: 9842.903686788655,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [-4.092858791351318, -0.0, -0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 8.617382049560547],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "2289584",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "934",
                value: 1.0,
                rank: 1,
              },
              {
                estimate: "412",
                value: 1.0406440414,
                rank: 2,
              },
              {
                estimate: "412",
                value: 0.6319191678,
                rank: 3,
              },
            ],
            longs: [],
          },
          isDiscrete: true,
        },
        frequentItems: {
          items: [
            {
              estimate: "2289584",
              jsonValue: "0.0",
            },
            {
              estimate: "934",
              jsonValue: "1.0",
            },
            {
              estimate: "412",
              jsonValue: "1.0406440414",
            },
            {
              estimate: "412",
              jsonValue: "0.6319191678",
            },
          ],
        },
        uniqueCount: {
          estimate: 9889.719369267126,
          upper: 10052.434732560137,
          lower: 9732.484474228999,
        },
      },
      total_rec_int: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -64146.516635518776,
          max: 97958.81361297597,
          mean: 2730.605796432358,
          stddev: 4748.0132861543925,
          histogram: {
            start: -64146.515625,
            end: 97958.82229588125,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "2112",
              "30720",
              "776196",
              "1172481",
              "205824",
              "65536",
              "29696",
              "12288",
              "1024",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 97958.8125,
            min: -64146.515625,
            bins: [
              -64146.515625, -58743.004360970626, -53339.49309694125, -47935.98183291187, -42532.4705688825,
              -37128.959304853124, -31725.448040823747, -26321.93677679437, -20918.425512764996, -15514.914248735622,
              -10111.402984706248, -4707.891720676867, 695.6195433525063, 6099.13080738188, 11502.642071411261,
              16906.153335440627, 22309.66459947001, 27713.17586349939, 33116.687127528756, 38520.19839155814,
              43923.7096555875, 49327.220919616884, 54730.732183646265, 60134.24344767563, 65537.75471170501,
              70941.2659757344, 76344.77723976376, 81748.28850379313, 87151.79976782252, 92555.31103185189,
              97958.82229588125,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2189543.889181436,
            upper: 2224261.1812260253,
            lower: 2155364.325938805,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -64146.515625, -5495.3603515625, -1387.865478515625, 304.7905578613281, 1401.9271240234375,
              3668.55029296875, 11152.5361328125, 20712.08203125, 97958.8125,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "87256",
                value: -775.3175405013,
                rank: 0,
              },
              {
                estimate: "87256",
                value: 2026.5752704083,
                rank: 1,
              },
              {
                estimate: "87256",
                value: 2373.029133312,
                rank: 2,
              },
              {
                estimate: "87256",
                value: 756.0824136485,
                rank: 3,
              },
              {
                estimate: "87256",
                value: -261.7553464834,
                rank: 4,
              },
              {
                estimate: "87256",
                value: 5361.3354747602,
                rank: 5,
              },
              {
                estimate: "87256",
                value: 835.950438875,
                rank: 6,
              },
              {
                estimate: "87256",
                value: 1314.3407214746,
                rank: 7,
              },
              {
                estimate: "87256",
                value: 6513.5483994377,
                rank: 8,
              },
              {
                estimate: "87256",
                value: 9316.2499793389,
                rank: 9,
              },
              {
                estimate: "87256",
                value: 5316.5944847234,
                rank: 10,
              },
              {
                estimate: "87256",
                value: 258.2001681413,
                rank: 11,
              },
              {
                estimate: "87256",
                value: 6976.98174053,
                rank: 12,
              },
              {
                estimate: "87256",
                value: 3077.8402301634,
                rank: 13,
              },
              {
                estimate: "87256",
                value: 4107.0866739256,
                rank: 14,
              },
              {
                estimate: "87256",
                value: 2326.9775759676,
                rank: 15,
              },
              {
                estimate: "87256",
                value: 36.2070749294,
                rank: 16,
              },
              {
                estimate: "87256",
                value: -2138.1021755523,
                rank: 17,
              },
              {
                estimate: "87256",
                value: 113.1918563743,
                rank: 18,
              },
              {
                estimate: "87256",
                value: 1981.183011084,
                rank: 19,
              },
              {
                estimate: "87256",
                value: 1628.4397579566,
                rank: 20,
              },
              {
                estimate: "87256",
                value: 9538.0219039091,
                rank: 21,
              },
              {
                estimate: "87256",
                value: 3509.7024331548,
                rank: 22,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "87256",
              jsonValue: "-775.3175405013",
            },
            {
              estimate: "87256",
              jsonValue: "2026.5752704083",
            },
            {
              estimate: "87256",
              jsonValue: "2373.029133312",
            },
            {
              estimate: "87256",
              jsonValue: "756.0824136485",
            },
            {
              estimate: "87256",
              jsonValue: "-261.7553464834",
            },
            {
              estimate: "87256",
              jsonValue: "5361.3354747602",
            },
            {
              estimate: "87256",
              jsonValue: "835.950438875",
            },
            {
              estimate: "87256",
              jsonValue: "1314.3407214746",
            },
            {
              estimate: "87256",
              jsonValue: "6513.5483994377",
            },
            {
              estimate: "87256",
              jsonValue: "9316.2499793389",
            },
            {
              estimate: "87256",
              jsonValue: "5316.5944847234",
            },
            {
              estimate: "87256",
              jsonValue: "258.2001681413",
            },
            {
              estimate: "87256",
              jsonValue: "6976.98174053",
            },
            {
              estimate: "87256",
              jsonValue: "3077.8402301634",
            },
            {
              estimate: "87256",
              jsonValue: "4107.0866739256",
            },
            {
              estimate: "87256",
              jsonValue: "2326.9775759676",
            },
            {
              estimate: "87256",
              jsonValue: "36.2070749294",
            },
            {
              estimate: "87256",
              jsonValue: "-2138.1021755523",
            },
            {
              estimate: "87256",
              jsonValue: "113.1918563743",
            },
            {
              estimate: "87256",
              jsonValue: "1981.183011084",
            },
            {
              estimate: "87256",
              jsonValue: "1628.4397579566",
            },
            {
              estimate: "87256",
              jsonValue: "9538.0219039091",
            },
            {
              estimate: "87256",
              jsonValue: "3509.7024331548",
            },
          ],
        },
        uniqueCount: {
          estimate: 2199600.558813873,
          upper: 2235790.5446631312,
          lower: 2164629.499466561,
        },
      },
      mo_sin_old_rev_tl_op: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -1690.1365544292255,
          max: 2691.533623980357,
          mean: 186.5894263606672,
          stddev: 232.06624940260534,
          histogram: {
            start: -1690.1365966796875,
            end: 2691.533960559619,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "4096",
              "64",
              "0",
              "7168",
              "26624",
              "107523",
              "538624",
              "744448",
              "452609",
              "215040",
              "107521",
              "48128",
              "21504",
              "13312",
              "12288",
              "0",
              "0",
              "0",
              "0",
              "1024",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 2691.53369140625,
            min: -1690.1365966796875,
            bins: [
              -1690.1365966796875, -1544.0809114383774, -1398.025226197067, -1251.9695409557569, -1105.9138557144465,
              -959.8581704731364, -813.8024852318263, -667.746799990516, -521.6911147492058, -375.63542950789565,
              -229.5797442665853, -83.52405902527516, 62.531626216034965, 208.58731145734532, 354.64299669865545,
              500.6986819399658, 646.7543671812759, 792.8100524225861, 938.8657376638962, 1084.9214229052068,
              1230.977108146517, 1377.032793387827, 1523.0884786291372, 1669.1441638704473, 1815.1998491117574,
              1961.255534353068, 2107.311219594378, 2253.3669048356883, 2399.4225900769984, 2545.478275318309,
              2691.533960559619,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2189610.1552672777,
            upper: 2224328.498992945,
            lower: 2155429.5566481994,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -1690.1365966796875, -294.9580993652344, -111.13032531738281, 42.759761810302734, 147.92564392089844,
              291.813232421875, 611.738525390625, 981.4844360351562, 2691.53369140625,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "87256",
                value: 205.030413058,
                rank: 0,
              },
              {
                estimate: "87256",
                value: 394.275545963,
                rank: 1,
              },
              {
                estimate: "87256",
                value: 810.0654341312,
                rank: 2,
              },
              {
                estimate: "87256",
                value: 588.1220126692,
                rank: 3,
              },
              {
                estimate: "87256",
                value: 140.3647868939,
                rank: 4,
              },
              {
                estimate: "87256",
                value: 133.5099351917,
                rank: 5,
              },
              {
                estimate: "87256",
                value: 186.4903559626,
                rank: 6,
              },
              {
                estimate: "87256",
                value: 48.0398346816,
                rank: 7,
              },
              {
                estimate: "87256",
                value: 34.091440585,
                rank: 8,
              },
              {
                estimate: "87256",
                value: 342.8060601353,
                rank: 9,
              },
              {
                estimate: "87256",
                value: -107.8588795187,
                rank: 10,
              },
              {
                estimate: "87256",
                value: 120.108411096,
                rank: 11,
              },
              {
                estimate: "87256",
                value: 47.1327484298,
                rank: 12,
              },
              {
                estimate: "87256",
                value: 93.6731394357,
                rank: 13,
              },
              {
                estimate: "87256",
                value: 549.3436805584,
                rank: 14,
              },
              {
                estimate: "87256",
                value: 432.137622972,
                rank: 15,
              },
              {
                estimate: "87256",
                value: 362.3398551224,
                rank: 16,
              },
              {
                estimate: "87256",
                value: 188.8884614064,
                rank: 17,
              },
              {
                estimate: "87256",
                value: 102.647929112,
                rank: 18,
              },
              {
                estimate: "87256",
                value: 168.944658171,
                rank: 19,
              },
              {
                estimate: "87256",
                value: 24.6426391484,
                rank: 20,
              },
              {
                estimate: "87256",
                value: 1067.8100017188,
                rank: 21,
              },
              {
                estimate: "87256",
                value: 386.9107000378,
                rank: 22,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "87256",
              jsonValue: "205.030413058",
            },
            {
              estimate: "87256",
              jsonValue: "394.275545963",
            },
            {
              estimate: "87256",
              jsonValue: "810.0654341312",
            },
            {
              estimate: "87256",
              jsonValue: "588.1220126692",
            },
            {
              estimate: "87256",
              jsonValue: "140.3647868939",
            },
            {
              estimate: "87256",
              jsonValue: "133.5099351917",
            },
            {
              estimate: "87256",
              jsonValue: "186.4903559626",
            },
            {
              estimate: "87256",
              jsonValue: "48.0398346816",
            },
            {
              estimate: "87256",
              jsonValue: "34.091440585",
            },
            {
              estimate: "87256",
              jsonValue: "342.8060601353",
            },
            {
              estimate: "87256",
              jsonValue: "-107.8588795187",
            },
            {
              estimate: "87256",
              jsonValue: "120.108411096",
            },
            {
              estimate: "87256",
              jsonValue: "47.1327484298",
            },
            {
              estimate: "87256",
              jsonValue: "93.6731394357",
            },
            {
              estimate: "87256",
              jsonValue: "549.3436805584",
            },
            {
              estimate: "87256",
              jsonValue: "432.137622972",
            },
            {
              estimate: "87256",
              jsonValue: "362.3398551224",
            },
            {
              estimate: "87256",
              jsonValue: "188.8884614064",
            },
            {
              estimate: "87256",
              jsonValue: "102.647929112",
            },
            {
              estimate: "87256",
              jsonValue: "168.944658171",
            },
            {
              estimate: "87256",
              jsonValue: "24.6426391484",
            },
            {
              estimate: "87256",
              jsonValue: "1067.8100017188",
            },
            {
              estimate: "87256",
              jsonValue: "386.9107000378",
            },
          ],
        },
        uniqueCount: {
          estimate: 2193264.542117971,
          upper: 2229350.281605931,
          lower: 2158394.2179768807,
        },
      },
      total_pymnt_inv: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -174335.3816531842,
          max: 257647.57670051648,
          mean: 15063.072014193136,
          stddev: 20483.412268760145,
          histogram: {
            start: -174335.375,
            end: 257647.60388975783,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "1024",
              "0",
              "3136",
              "4096",
              "38912",
              "228354",
              "1007617",
              "555010",
              "241664",
              "112640",
              "51200",
              "30720",
              "13312",
              "4096",
              "4096",
              "0",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 257647.578125,
            min: -174335.375,
            bins: [
              -174335.375, -159935.9423703414, -145536.5097406828, -131137.0771110242, -116737.64448136563,
              -102338.21185170702, -87938.77922204843, -73539.34659238983, -59139.91396273124, -44740.48133307265,
              -30341.04870341404, -15941.61607375546, -1542.1834440968523, 12857.249185561726, 27256.681815220334,
              41656.11444487891, 56055.54707453752, 70454.97970419613, 84854.4123338547, 99253.84496351331,
              113653.27759317192, 128052.71022283047, 142452.14285248908, 156851.5754821477, 171251.0081118063,
              185650.4407414649, 200049.87337112345, 214449.30600078206, 228848.73863044067, 243248.17126009928,
              257647.60388975783,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2181897.766434498,
            upper: 2216493.7100920724,
            lower: 2147837.6702663116,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -174335.375, -24826.19921875, -8305.1552734375, 2798.93017578125, 10595.271484375, 23285.67578125,
              55124.04296875, 88325.2890625, 257647.578125,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "87256",
                value: 23891.068742301,
                rank: 0,
              },
              {
                estimate: "87256",
                value: 10432.9850743986,
                rank: 1,
              },
              {
                estimate: "87256",
                value: -37719.9414171301,
                rank: 2,
              },
              {
                estimate: "87256",
                value: 15540.9757148679,
                rank: 3,
              },
              {
                estimate: "87256",
                value: 7600.5103662934,
                rank: 4,
              },
              {
                estimate: "87256",
                value: 81734.8317370057,
                rank: 5,
              },
              {
                estimate: "87256",
                value: 11177.4737912659,
                rank: 6,
              },
              {
                estimate: "87256",
                value: 15306.8457763142,
                rank: 7,
              },
              {
                estimate: "87256",
                value: 6874.1075836029,
                rank: 8,
              },
              {
                estimate: "87256",
                value: 10149.5623107298,
                rank: 9,
              },
              {
                estimate: "87256",
                value: 11182.4179523644,
                rank: 10,
              },
              {
                estimate: "87256",
                value: 14415.2754772696,
                rank: 11,
              },
              {
                estimate: "87256",
                value: 35635.2237094404,
                rank: 12,
              },
              {
                estimate: "87256",
                value: 4218.2076058461,
                rank: 13,
              },
              {
                estimate: "87256",
                value: 1979.771412065,
                rank: 14,
              },
              {
                estimate: "87256",
                value: -20592.6127283861,
                rank: 15,
              },
              {
                estimate: "87256",
                value: 13467.2952521533,
                rank: 16,
              },
              {
                estimate: "87256",
                value: 1830.4811953745,
                rank: 17,
              },
              {
                estimate: "87256",
                value: 11899.0189190771,
                rank: 18,
              },
              {
                estimate: "87256",
                value: 9118.2474392128,
                rank: 19,
              },
              {
                estimate: "87256",
                value: 2541.3210235584,
                rank: 20,
              },
              {
                estimate: "87256",
                value: 16396.2434106309,
                rank: 21,
              },
              {
                estimate: "87256",
                value: 24816.8894915897,
                rank: 22,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "87256",
              jsonValue: "23891.068742301",
            },
            {
              estimate: "87256",
              jsonValue: "10432.9850743986",
            },
            {
              estimate: "87256",
              jsonValue: "-37719.9414171301",
            },
            {
              estimate: "87256",
              jsonValue: "15540.9757148679",
            },
            {
              estimate: "87256",
              jsonValue: "7600.5103662934",
            },
            {
              estimate: "87256",
              jsonValue: "81734.8317370057",
            },
            {
              estimate: "87256",
              jsonValue: "11177.4737912659",
            },
            {
              estimate: "87256",
              jsonValue: "15306.8457763142",
            },
            {
              estimate: "87256",
              jsonValue: "6874.1075836029",
            },
            {
              estimate: "87256",
              jsonValue: "10149.5623107298",
            },
            {
              estimate: "87256",
              jsonValue: "11182.4179523644",
            },
            {
              estimate: "87256",
              jsonValue: "14415.2754772696",
            },
            {
              estimate: "87256",
              jsonValue: "35635.2237094404",
            },
            {
              estimate: "87256",
              jsonValue: "4218.2076058461",
            },
            {
              estimate: "87256",
              jsonValue: "1979.771412065",
            },
            {
              estimate: "87256",
              jsonValue: "-20592.6127283861",
            },
            {
              estimate: "87256",
              jsonValue: "13467.2952521533",
            },
            {
              estimate: "87256",
              jsonValue: "1830.4811953745",
            },
            {
              estimate: "87256",
              jsonValue: "11899.0189190771",
            },
            {
              estimate: "87256",
              jsonValue: "9118.2474392128",
            },
            {
              estimate: "87256",
              jsonValue: "2541.3210235584",
            },
            {
              estimate: "87256",
              jsonValue: "16396.2434106309",
            },
            {
              estimate: "87256",
              jsonValue: "24816.8894915897",
            },
          ],
        },
        uniqueCount: {
          estimate: 2208635.5158722247,
          upper: 2244974.153696852,
          lower: 2173520.8113443265,
        },
      },
      pub_rec_bankruptcies: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -13.004764591451163,
          max: 31.502498684068502,
          mean: 0.1322253222623085,
          stddev: 0.5597265936846113,
          histogram: {
            start: -13.004764556884766,
            end: 31.502501776958848,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "4160",
              "2084869",
              "146432",
              "56320",
              "4096",
              "0",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 31.502498626708984,
            min: -13.004764556884766,
            bins: [
              -13.004764556884766, -11.521189012423312, -10.037613467961858, -8.554037923500402, -7.07046237903895,
              -5.586886834577496, -4.103311290116041, -2.6197357456545873, -1.1361602011931335, 0.34741534326832024,
              1.830990887729774, 3.314566432191228, 4.798141976652683, 6.281717521114135, 7.765293065575591,
              9.248868610037043, 10.732444154498499, 12.216019698959954, 13.699595243421406, 15.183170787882862,
              16.666746332344314, 18.15032187680577, 19.63389742126722, 21.117472965728673, 22.601048510190132,
              24.084624054651584, 25.568199599113036, 27.051775143574496, 28.535350688035948, 30.0189262324974,
              31.50250177695885,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 268981.54752552416,
            upper: 273218.310654552,
            lower: 264809.9628246754,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -13.004764556884766, -0.2655983567237854, 0.0, -0.0, 0.0, 0.0, 1.2497919797897339, 2.593512535095215,
              31.502498626708984,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "2023550",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "21821",
                value: 1.0,
                rank: 1,
              },
              {
                estimate: "11514",
                value: 2.0,
                rank: 2,
              },
              {
                estimate: "11209",
                value: 3.0,
                rank: 3,
              },
              {
                estimate: "11115",
                value: 5.0,
                rank: 4,
              },
              {
                estimate: "11039",
                value: 1.6059460041,
                rank: 5,
              },
              {
                estimate: "11039",
                value: 0.5685433965000001,
                rank: 6,
              },
              {
                estimate: "11039",
                value: 0.5702461375,
                rank: 7,
              },
              {
                estimate: "11039",
                value: 1.4760501837,
                rank: 8,
              },
            ],
            longs: [],
          },
          isDiscrete: true,
        },
        frequentItems: {
          items: [
            {
              estimate: "2023550",
              jsonValue: "0.0",
            },
            {
              estimate: "21821",
              jsonValue: "1.0",
            },
            {
              estimate: "11514",
              jsonValue: "2.0",
            },
            {
              estimate: "11209",
              jsonValue: "3.0",
            },
            {
              estimate: "11115",
              jsonValue: "5.0",
            },
            {
              estimate: "11039",
              jsonValue: "1.6059460041",
            },
            {
              estimate: "11039",
              jsonValue: "0.5685433965",
            },
            {
              estimate: "11039",
              jsonValue: "0.5702461375",
            },
            {
              estimate: "11039",
              jsonValue: "1.4760501837",
            },
          ],
        },
        uniqueCount: {
          estimate: 266904.8809482957,
          upper: 271296.2618405725,
          lower: 262661.4075620353,
        },
      },
      annual_inc_joint: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            NULL: "2246258",
            FRACTIONAL: "53715",
          },
        },
        numberSummary: {
          count: "53715",
          min: -737022.2177696632,
          max: 1165620.029098962,
          mean: 109948.69940016181,
          stddev: 124660.97880778574,
          histogram: {
            start: -737022.1875,
            end: 1165620.116562,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "16",
              "0",
              "66",
              "160",
              "256",
              "960",
              "2864",
              "7632",
              "13121",
              "11840",
              "7872",
              "4240",
              "2288",
              "1168",
              "608",
              "336",
              "16",
              "272",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 1165620.0,
            min: -737022.1875,
            bins: [
              -737022.1875, -673600.7773646, -610179.3672292, -546757.9570938, -483336.5469584, -419915.136823,
              -356493.7266876, -293072.3165522, -229650.9064168, -166229.49628139997, -102808.08614599996,
              -39386.67601059994, 24034.734124799957, 87456.14426019997, 150877.5543956, 214298.964531, 277720.3746664,
              341141.78480180004, 404563.19493720005, 467984.60507260007, 531406.0152080001, 594827.4253434001,
              658248.8354788001, 721670.2456142001, 785091.6557495999, 848513.0658849999, 911934.4760204,
              975355.8861558, 1038777.2962912, 1102198.7064266, 1165620.116562,
            ],
            n: "53715",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 51723.3542503439,
            upper: 52511.56863246264,
            lower: 50946.86230526093,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -737022.1875, -155928.8125, -66346.3828125, 31648.494140625, 96330.8984375, 174170.84375, 330999.9375,
              471819.46875, 1165620.0,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "2172",
                value: 95000.0,
                rank: 0,
              },
              {
                estimate: "2172",
                value: 165000.0,
                rank: 1,
              },
              {
                estimate: "2168",
                value: 103000.0,
                rank: 2,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "2172",
              jsonValue: "95000.0",
            },
            {
              estimate: "2172",
              jsonValue: "165000.0",
            },
            {
              estimate: "2168",
              jsonValue: "103000.0",
            },
          ],
        },
        uniqueCount: {
          estimate: 51886.52932910293,
          upper: 52740.21740199555,
          lower: 51061.59459755667,
        },
      },
      total_bal_ex_mort: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -1287605.633212913,
          max: 2727200.3954634354,
          mean: 52948.66701482187,
          stddev: 87699.50545000618,
          histogram: {
            start: -1287605.625,
            end: 2727200.77272005,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "2112",
              "0",
              "23552",
              "1438725",
              "698368",
              "98304",
              "26624",
              "8192",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 2727200.5,
            min: -1287605.625,
            bins: [
              -1287605.625, -1153778.7450759984, -1019951.8651519967, -886124.985227995, -752298.1053039933,
              -618471.2253799917, -484644.34545598994, -350817.4655319883, -216990.58560798666, -83163.70568398503,
              50663.17424001661, 184490.05416401825, 318316.9340880201, 452143.81401202176, 585970.6939360234,
              719797.573860025, 853624.4537840267, 987451.3337080283, 1121278.21363203, 1255105.0935560316,
              1388931.9734800332, 1522758.8534040349, 1656585.7333280365, 1790412.6132520381, 1924239.4931760402,
              2058066.3731000419, 2191893.2530240435, 2325720.132948045, 2459547.012872047, 2593373.8927960484,
              2727200.77272005,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2289539.1241885056,
            upper: 2325843.3983718366,
            lower: 2253797.1824263376,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -1287605.625, -93513.265625, -26534.431640625, 7718.947265625, 31775.939453125, 74924.109375,
              199808.03125, 389134.5, 2727200.5,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "87256",
                value: 176151.7512080681,
                rank: 0,
              },
              {
                estimate: "87256",
                value: 23953.0349997152,
                rank: 1,
              },
              {
                estimate: "87256",
                value: 180278.0169248464,
                rank: 2,
              },
              {
                estimate: "87256",
                value: 22232.9388237925,
                rank: 3,
              },
              {
                estimate: "87256",
                value: 23941.8994064844,
                rank: 4,
              },
              {
                estimate: "87256",
                value: 23127.1200855936,
                rank: 5,
              },
              {
                estimate: "87256",
                value: 203261.6912969277,
                rank: 6,
              },
              {
                estimate: "87256",
                value: -8598.3130898032,
                rank: 7,
              },
              {
                estimate: "87256",
                value: 19465.9185185092,
                rank: 8,
              },
              {
                estimate: "87256",
                value: 126662.3292951994,
                rank: 9,
              },
              {
                estimate: "87256",
                value: 16108.5858127977,
                rank: 10,
              },
              {
                estimate: "87256",
                value: 134342.5685026291,
                rank: 11,
              },
              {
                estimate: "87256",
                value: 770.3264989055,
                rank: 12,
              },
              {
                estimate: "87256",
                value: 133.9485660592,
                rank: 13,
              },
              {
                estimate: "87256",
                value: 69281.8352753459,
                rank: 14,
              },
              {
                estimate: "87256",
                value: 7097.265350373,
                rank: 15,
              },
              {
                estimate: "87256",
                value: 68712.941189343,
                rank: 16,
              },
              {
                estimate: "87256",
                value: 65950.6765504289,
                rank: 17,
              },
              {
                estimate: "87256",
                value: 163130.908121636,
                rank: 18,
              },
              {
                estimate: "87256",
                value: 70394.4932007893,
                rank: 19,
              },
              {
                estimate: "87256",
                value: 395425.6779110172,
                rank: 20,
              },
              {
                estimate: "87256",
                value: 15451.6096917548,
                rank: 21,
              },
              {
                estimate: "87256",
                value: 122705.6768035453,
                rank: 22,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "87256",
              jsonValue: "176151.7512080681",
            },
            {
              estimate: "87256",
              jsonValue: "23953.0349997152",
            },
            {
              estimate: "87256",
              jsonValue: "180278.0169248464",
            },
            {
              estimate: "87256",
              jsonValue: "22232.9388237925",
            },
            {
              estimate: "87256",
              jsonValue: "23941.8994064844",
            },
            {
              estimate: "87256",
              jsonValue: "23127.1200855936",
            },
            {
              estimate: "87256",
              jsonValue: "203261.6912969277",
            },
            {
              estimate: "87256",
              jsonValue: "-8598.3130898032",
            },
            {
              estimate: "87256",
              jsonValue: "19465.9185185092",
            },
            {
              estimate: "87256",
              jsonValue: "126662.3292951994",
            },
            {
              estimate: "87256",
              jsonValue: "16108.5858127977",
            },
            {
              estimate: "87256",
              jsonValue: "134342.5685026291",
            },
            {
              estimate: "87256",
              jsonValue: "770.3264989055",
            },
            {
              estimate: "87256",
              jsonValue: "133.9485660592",
            },
            {
              estimate: "87256",
              jsonValue: "69281.8352753459",
            },
            {
              estimate: "87256",
              jsonValue: "7097.265350373",
            },
            {
              estimate: "87256",
              jsonValue: "68712.941189343",
            },
            {
              estimate: "87256",
              jsonValue: "65950.6765504289",
            },
            {
              estimate: "87256",
              jsonValue: "163130.908121636",
            },
            {
              estimate: "87256",
              jsonValue: "70394.4932007893",
            },
            {
              estimate: "87256",
              jsonValue: "395425.6779110172",
            },
            {
              estimate: "87256",
              jsonValue: "15451.6096917548",
            },
            {
              estimate: "87256",
              jsonValue: "122705.6768035453",
            },
          ],
        },
        uniqueCount: {
          estimate: 2210874.1140302094,
          upper: 2247249.5834674374,
          lower: 2175723.8184270477,
        },
      },
      mths_since_last_record: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            NULL: "1884974",
            FRACTIONAL: "414999",
          },
        },
        numberSummary: {
          count: "414999",
          min: -364.30095855418926,
          max: 552.2259396934621,
          mean: 65.92013026177597,
          stddev: 74.38049603831708,
          histogram: {
            start: -364.30096435546875,
            end: 552.2260073710327,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "272",
              "512",
              "1794",
              "2820",
              "7936",
              "17920",
              "39424",
              "74240",
              "75776",
              "67073",
              "47872",
              "32000",
              "20736",
              "11776",
              "6912",
              "5120",
              "1792",
              "1024",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 552.2259521484375,
            min: -364.30096435546875,
            bins: [
              -364.30096435546875, -333.7500652979187, -303.19916624036864, -272.6482671828186, -242.09736812526856,
              -211.5464690677185, -180.99557001016848, -150.44467095261842, -119.89377189506837, -89.34287283751831,
              -58.79197377996826, -28.241074722418205, 2.3098243351317933, 32.86072339268185, 63.4116224502319,
              93.96252150778196, 124.51342056533201, 155.06431962288207, 185.61521868043212, 216.16611773798218,
              246.71701679553223, 277.2679158530823, 307.81881491063234, 338.3697139681824, 368.92061302573234,
              399.4715120832824, 430.02241114083245, 460.5733101983825, 491.12420925593256, 521.6751083134826,
              552.2260073710327,
            ],
            n: "414999",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 397009.0450328805,
            upper: 403277.717452207,
            lower: 390837.0519454708,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -364.30096435546875, -101.33891296386719, -42.29154586791992, 16.254236221313477, 58.139678955078125,
              108.0, 198.8673553466797, 271.8868408203125, 552.2259521484375,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "16601",
                value: 13.5329349879,
                rank: 0,
              },
              {
                estimate: "16601",
                value: 157.0760302247,
                rank: 1,
              },
              {
                estimate: "16601",
                value: 163.6967035607,
                rank: 2,
              },
              {
                estimate: "16601",
                value: -7.9328368366,
                rank: 3,
              },
              {
                estimate: "16601",
                value: -51.1585469679,
                rank: 4,
              },
              {
                estimate: "16601",
                value: 82.7542577184,
                rank: 5,
              },
              {
                estimate: "16601",
                value: -126.0553497831,
                rank: 6,
              },
              {
                estimate: "16601",
                value: 91.2571376644,
                rank: 7,
              },
              {
                estimate: "16601",
                value: 95.2292377142,
                rank: 8,
              },
              {
                estimate: "16601",
                value: 124.6183703533,
                rank: 9,
              },
              {
                estimate: "16601",
                value: 15.2482942237,
                rank: 10,
              },
              {
                estimate: "16601",
                value: -94.9995816555,
                rank: 11,
              },
              {
                estimate: "16601",
                value: 60.4170931556,
                rank: 12,
              },
              {
                estimate: "16601",
                value: 156.0410084173,
                rank: 13,
              },
              {
                estimate: "16601",
                value: 185.7991517525,
                rank: 14,
              },
              {
                estimate: "16601",
                value: 242.6195071,
                rank: 15,
              },
              {
                estimate: "16601",
                value: 96.7827490034,
                rank: 16,
              },
              {
                estimate: "16601",
                value: 6.5256898523,
                rank: 17,
              },
              {
                estimate: "16601",
                value: 37.3456085374,
                rank: 18,
              },
              {
                estimate: "16601",
                value: 3.885932599,
                rank: 19,
              },
              {
                estimate: "16601",
                value: 44.134390001,
                rank: 20,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "16601",
              jsonValue: "13.5329349879",
            },
            {
              estimate: "16601",
              jsonValue: "157.0760302247",
            },
            {
              estimate: "16601",
              jsonValue: "163.6967035607",
            },
            {
              estimate: "16601",
              jsonValue: "-7.9328368366",
            },
            {
              estimate: "16601",
              jsonValue: "-51.1585469679",
            },
            {
              estimate: "16601",
              jsonValue: "82.7542577184",
            },
            {
              estimate: "16601",
              jsonValue: "-126.0553497831",
            },
            {
              estimate: "16601",
              jsonValue: "91.2571376644",
            },
            {
              estimate: "16601",
              jsonValue: "95.2292377142",
            },
            {
              estimate: "16601",
              jsonValue: "124.6183703533",
            },
            {
              estimate: "16601",
              jsonValue: "15.2482942237",
            },
            {
              estimate: "16601",
              jsonValue: "-94.9995816555",
            },
            {
              estimate: "16601",
              jsonValue: "60.4170931556",
            },
            {
              estimate: "16601",
              jsonValue: "156.0410084173",
            },
            {
              estimate: "16601",
              jsonValue: "185.7991517525",
            },
            {
              estimate: "16601",
              jsonValue: "242.6195071",
            },
            {
              estimate: "16601",
              jsonValue: "96.7827490034",
            },
            {
              estimate: "16601",
              jsonValue: "6.5256898523",
            },
            {
              estimate: "16601",
              jsonValue: "37.3456085374",
            },
            {
              estimate: "16601",
              jsonValue: "3.885932599",
            },
            {
              estimate: "16601",
              jsonValue: "44.134390001",
            },
          ],
        },
        uniqueCount: {
          estimate: 392219.7916278943,
          upper: 398672.97634451697,
          lower: 385983.9586921152,
        },
      },
      initial_list_status: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2299973",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 2.0,
            upper: 2.0,
            lower: 2.0,
          },
          frequent: {
            items: [
              {
                value: "w",
                estimate: 1982253.0,
              },
              {
                value: "f",
                estimate: 317720.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "1982253",
              jsonValue: '"w"',
            },
            {
              estimate: "317720",
              jsonValue: '"f"',
            },
          ],
        },
        uniqueCount: {
          estimate: 2.000000004967054,
          upper: 2.000099863468538,
          lower: 2.0,
        },
      },
      total_acc: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -240.9138733482456,
          max: 449.95928045324325,
          mean: 25.026294511027054,
          stddev: 30.280933876912254,
          histogram: {
            start: -240.91387939453125,
            end: 449.9593345467102,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "64",
              "1024",
              "11264",
              "21504",
              "129027",
              "665602",
              "807936",
              "392192",
              "167936",
              "68608",
              "25600",
              "5120",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 449.95928955078125,
            min: -240.91387939453125,
            bins: [
              -240.91387939453125, -217.88477226315655, -194.85566513178182, -171.82655800040712, -148.7974508690324,
              -125.76834373765769, -102.73923660628299, -79.71012947490826, -56.681022343533556, -33.651915212158855,
              -10.622808080784125, 12.406299050590576, 35.43540618196528, 58.46451331333998, 81.49362044471474,
              104.52272757608944, 127.55183470746414, 150.58094183883884, 173.61004897021354, 196.6391561015883,
              219.668263232963, 242.6973703643377, 265.7264774957124, 288.75558462708716, 311.7846917584618,
              334.81379888983656, 357.8429060212112, 380.87201315258596, 403.9011202839607, 426.93022741533537,
              449.9593345467101,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2188472.4953091363,
            upper: 2223172.78371379,
            lower: 2154309.6720922105,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -240.91387939453125, -43.34657669067383, -15.706841468811035, 6.199501991271973, 20.33978843688965,
              39.12860870361328, 79.33048248291016, 116.0433578491211, 449.95928955078125,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "87687",
                value: 13.5649365983,
                rank: 0,
              },
              {
                estimate: "87687",
                value: 21.8278315479,
                rank: 1,
              },
              {
                estimate: "87687",
                value: 8.1547542943,
                rank: 2,
              },
              {
                estimate: "87687",
                value: 106.7425614758,
                rank: 3,
              },
              {
                estimate: "87687",
                value: 11.6305031958,
                rank: 4,
              },
              {
                estimate: "87687",
                value: 12.1557201628,
                rank: 5,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "87687",
              jsonValue: "13.5649365983",
            },
            {
              estimate: "87687",
              jsonValue: "21.8278315479",
            },
            {
              estimate: "87687",
              jsonValue: "8.1547542943",
            },
            {
              estimate: "87687",
              jsonValue: "106.7425614758",
            },
            {
              estimate: "87687",
              jsonValue: "11.6305031958",
            },
            {
              estimate: "87687",
              jsonValue: "12.1557201628",
            },
          ],
        },
        uniqueCount: {
          estimate: 2188479.614356367,
          upper: 2224486.6275195456,
          lower: 2153685.3649335033,
        },
      },
      num_tl_90g_dpd_24m: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -32.00493493021688,
          max: 64.89118908929143,
          mean: 0.09604315404047828,
          stddev: 0.7668876036040088,
          histogram: {
            start: -32.00493621826172,
            end: 64.89119606431427,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "64",
              "2188293",
              "94208",
              "12288",
              "4096",
              "1024",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 64.89118957519531,
            min: -32.00493621826172,
            bins: [
              -32.00493621826172, -28.77506514217585, -25.545194066089987, -22.31532299000412, -19.085451913918256,
              -15.855580837832388, -12.62570976174652, -9.395838685660657, -6.165967609574789, -2.9360965334889215,
              0.29377454259694247, 3.5236456186828065, 6.7535166947686776, 9.983387770854542, 13.213258846940406,
              16.443129923026277, 19.67300099911214, 22.902872075198005, 26.132743151283876, 29.36261422736974,
              32.592485303455604, 35.822356379541475, 39.05222745562733, 42.2820985317132, 45.511969607799074,
              48.74184068388493, 51.9717117599708, 55.20158283605667, 58.43145391214253, 61.6613249882284,
              64.89119606431427,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 135442.46316019335,
            upper: 137559.76186072003,
            lower: 133357.48585950502,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -32.00493621826172, -0.10320723801851273, 0.0, 0.0, 0.0, 0.0, 0.0, 2.6344292163848877, 64.89118957519531,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "2160337",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "10295",
                value: 1.0,
                rank: 1,
              },
              {
                estimate: "6367",
                value: 2.0,
                rank: 2,
              },
              {
                estimate: "5773",
                value: 3.0,
                rank: 3,
              },
              {
                estimate: "5685",
                value: 4.0,
                rank: 4,
              },
              {
                estimate: "5628",
                value: 7.0,
                rank: 5,
              },
              {
                estimate: "5596",
                value: 10.0,
                rank: 6,
              },
              {
                estimate: "5572",
                value: -0.5400003571,
                rank: 7,
              },
              {
                estimate: "5572",
                value: 2.0465316859,
                rank: 8,
              },
              {
                estimate: "5572",
                value: 0.0107054518,
                rank: 9,
              },
              {
                estimate: "5572",
                value: 1.2074414441,
                rank: 10,
              },
              {
                estimate: "5572",
                value: 1.6173482646,
                rank: 11,
              },
              {
                estimate: "5572",
                value: -0.6909593602,
                rank: 12,
              },
              {
                estimate: "5572",
                value: 0.5249749536,
                rank: 13,
              },
              {
                estimate: "5572",
                value: 2.1584166049,
                rank: 14,
              },
              {
                estimate: "5572",
                value: -0.5845798898,
                rank: 15,
              },
              {
                estimate: "5572",
                value: 3.0317094445,
                rank: 16,
              },
              {
                estimate: "5572",
                value: 1.1584779705,
                rank: 17,
              },
              {
                estimate: "5572",
                value: 0.331734954,
                rank: 18,
              },
              {
                estimate: "5572",
                value: -0.1935777177,
                rank: 19,
              },
              {
                estimate: "5572",
                value: -0.157063529,
                rank: 20,
              },
            ],
            longs: [],
          },
          isDiscrete: true,
        },
        frequentItems: {
          items: [
            {
              estimate: "2160337",
              jsonValue: "0.0",
            },
            {
              estimate: "10295",
              jsonValue: "1.0",
            },
            {
              estimate: "6367",
              jsonValue: "2.0",
            },
            {
              estimate: "5773",
              jsonValue: "3.0",
            },
            {
              estimate: "5685",
              jsonValue: "4.0",
            },
            {
              estimate: "5628",
              jsonValue: "7.0",
            },
            {
              estimate: "5596",
              jsonValue: "10.0",
            },
            {
              estimate: "5572",
              jsonValue: "-0.5400003571",
            },
            {
              estimate: "5572",
              jsonValue: "2.0465316859",
            },
            {
              estimate: "5572",
              jsonValue: "0.0107054518",
            },
            {
              estimate: "5572",
              jsonValue: "1.2074414441",
            },
            {
              estimate: "5572",
              jsonValue: "1.6173482646",
            },
            {
              estimate: "5572",
              jsonValue: "-0.6909593602",
            },
            {
              estimate: "5572",
              jsonValue: "0.5249749536",
            },
            {
              estimate: "5572",
              jsonValue: "2.1584166049",
            },
            {
              estimate: "5572",
              jsonValue: "-0.5845798898",
            },
            {
              estimate: "5572",
              jsonValue: "3.0317094445",
            },
            {
              estimate: "5572",
              jsonValue: "1.1584779705",
            },
            {
              estimate: "5572",
              jsonValue: "0.331734954",
            },
            {
              estimate: "5572",
              jsonValue: "-0.1935777177",
            },
            {
              estimate: "5572",
              jsonValue: "-0.157063529",
            },
          ],
        },
        uniqueCount: {
          estimate: 135537.02328307793,
          upper: 137767.01133023054,
          lower: 133382.14417741587,
        },
      },
      total_rec_late_fee: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -1314.22371383244,
          max: 2833.0804199246104,
          mean: 2.296035965719948,
          stddev: 22.40998446963098,
          histogram: {
            start: -1314.2237548828125,
            end: 2833.080605573657,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "64",
              "2278405",
              "17408",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 2833.080322265625,
            min: -1314.2237548828125,
            bins: [
              -1314.2237548828125, -1175.9802762009301, -1037.7367975190477, -899.4933188371656, -761.2498401552832,
              -623.0063614734008, -484.76288279151856, -346.5194041096363, -208.27592542775392, -70.03244674587154,
              68.21103193601084, 206.454510617893, 344.6979892997754, 482.94146798165775, 621.1849466635399,
              759.4284253454225, 897.6719040273047, 1035.9153827091868, 1174.1588613910694, 1312.4023400729516,
              1450.6458187548342, 1588.8892974367163, 1727.1327761185985, 1865.376254800481, 2003.6197334823632,
              2141.8632121642454, 2280.106690846128, 2418.35016952801, 2556.5936482098923, 2694.837126891775,
              2833.0806055736575,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 124506.5285934527,
            upper: 126450.24582651233,
            lower: 122592.44199364934,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [-1314.2237548828125, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 63.662017822265625, 2833.080322265625],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "2171713",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "6262",
                value: 15.0,
                rank: 1,
              },
              {
                estimate: "5309",
                value: 30.0,
                rank: 2,
              },
              {
                estimate: "5306",
                value: 87.8350852561,
                rank: 3,
              },
              {
                estimate: "5306",
                value: -10.3312498904,
                rank: 4,
              },
            ],
            longs: [],
          },
          isDiscrete: true,
        },
        frequentItems: {
          items: [
            {
              estimate: "2171713",
              jsonValue: "0.0",
            },
            {
              estimate: "6262",
              jsonValue: "15.0",
            },
            {
              estimate: "5309",
              jsonValue: "30.0",
            },
            {
              estimate: "5306",
              jsonValue: "87.8350852561",
            },
            {
              estimate: "5306",
              jsonValue: "-10.3312498904",
            },
          ],
        },
        uniqueCount: {
          estimate: 120448.52566822285,
          upper: 122430.26295321771,
          lower: 118533.53591130485,
        },
      },
      pct_tl_nvr_dlq: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -362.32330466418006,
          max: 596.8131772414865,
          mean: 94.03920578729385,
          stddev: 94.50181486970203,
          histogram: {
            start: -362.32330322265625,
            end: 596.8132310680359,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "64",
              "11264",
              "5120",
              "25600",
              "39936",
              "86018",
              "133120",
              "190464",
              "254977",
              "296960",
              "375808",
              "263169",
              "216064",
              "159744",
              "111616",
              "61441",
              "37888",
              "17408",
              "9216",
              "0",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 596.8131713867188,
            min: -362.32330322265625,
            bins: [
              -362.32330322265625, -330.3520854129665, -298.3808676032768, -266.40964979358705, -234.43843198389732,
              -202.46721417420756, -170.49599636451782, -138.5247785548281, -106.55356074513836, -74.5823429354486,
              -42.61112512575886, -10.639907316069127, 21.331310493620606, 53.30252830331034, 85.27374611300007,
              117.2449639226898, 149.21618173237954, 181.18739954206933, 213.15861735175906, 245.1298351614488,
              277.10105297113853, 309.07227078082826, 341.043488590518, 373.01470640020773, 404.98592420989746,
              436.9571420195872, 468.9283598292769, 500.89957763896666, 532.8707954486564, 564.8420132583461,
              596.8132310680359,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2200061.15890072,
            upper: 2234945.366093536,
            lower: 2165717.268262161,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -362.32330322265625, -130.7034149169922, -61.26404571533203, 32.46940612792969, 94.44773864746094,
              154.85362243652344, 253.3107452392578, 323.9638671875, 596.8131713867188,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "126109",
                value: 100.0,
                rank: 0,
              },
              {
                estimate: "85639",
                value: 195.984005615,
                rank: 1,
              },
              {
                estimate: "85639",
                value: 42.1449080522,
                rank: 2,
              },
              {
                estimate: "85639",
                value: 25.9195407293,
                rank: 3,
              },
              {
                estimate: "85639",
                value: 66.4045645131,
                rank: 4,
              },
              {
                estimate: "85639",
                value: 138.5631286978,
                rank: 5,
              },
              {
                estimate: "85639",
                value: 62.9994461125,
                rank: 6,
              },
              {
                estimate: "85639",
                value: 263.9554488955,
                rank: 7,
              },
              {
                estimate: "85639",
                value: 28.0050637555,
                rank: 8,
              },
              {
                estimate: "85639",
                value: 25.1144423007,
                rank: 9,
              },
              {
                estimate: "85639",
                value: 188.2149920555,
                rank: 10,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "126109",
              jsonValue: "100.0",
            },
            {
              estimate: "85639",
              jsonValue: "195.984005615",
            },
            {
              estimate: "85639",
              jsonValue: "42.1449080522",
            },
            {
              estimate: "85639",
              jsonValue: "25.9195407293",
            },
            {
              estimate: "85639",
              jsonValue: "66.4045645131",
            },
            {
              estimate: "85639",
              jsonValue: "138.5631286978",
            },
            {
              estimate: "85639",
              jsonValue: "62.9994461125",
            },
            {
              estimate: "85639",
              jsonValue: "263.9554488955",
            },
            {
              estimate: "85639",
              jsonValue: "28.0050637555",
            },
            {
              estimate: "85639",
              jsonValue: "25.1144423007",
            },
            {
              estimate: "85639",
              jsonValue: "188.2149920555",
            },
          ],
        },
        uniqueCount: {
          estimate: 2178606.7304752697,
          upper: 2214451.30526912,
          lower: 2143969.4482830027,
        },
      },
      delinq_2yrs: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -46.1181886500706,
          max: 65.57410379409801,
          mean: 0.34953636248345704,
          stddev: 1.3544883580949745,
          histogram: {
            start: -46.118186950683594,
            end: 65.57411086649246,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "4096",
              "12352",
              "2167813",
              "98304",
              "13312",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 65.57410430908203,
            min: -46.118186950683594,
            bins: [
              -46.118186950683594, -42.39511035677773, -38.672033762871855, -34.94895716896599, -31.22588057506012,
              -27.50280398115425, -23.779727387248386, -20.056650793342516, -16.333574199436647, -12.610497605530782,
              -8.887421011624909, -5.164344417719043, -1.4412678238131775, 2.2818087700926952, 6.004885363998561,
              9.727961957904434, 13.4510385518103, 17.174115145716165, 20.89719173962203, 24.620268333527903,
              28.343344927433776, 32.066421521339635, 35.78949811524551, 39.51257470915138, 43.23565130305724,
              46.95872789696311, 50.681804490868984, 54.40488108477486, 58.127957678680716, 61.85103427258659,
              65.57411086649246,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 455772.13271972846,
            upper: 462973.4196434576,
            lower: 448681.9836644575,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -46.118186950683594, -1.0240603685379028, -0.0, 0.0, 0.0, 0.0, 2.283752202987671, 5.5035080909729,
              65.57410430908203,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "1819252",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "31726",
                value: 1.0,
                rank: 1,
              },
              {
                estimate: "23674",
                value: 2.0,
                rank: 2,
              },
              {
                estimate: "20684",
                value: 3.0,
                rank: 3,
              },
              {
                estimate: "19973",
                value: 4.0,
                rank: 4,
              },
              {
                estimate: "19347",
                value: 5.0,
                rank: 5,
              },
              {
                estimate: "19323",
                value: 6.0,
                rank: 6,
              },
              {
                estimate: "19276",
                value: 7.0,
                rank: 7,
              },
              {
                estimate: "19266",
                value: 9.0,
                rank: 8,
              },
              {
                estimate: "19244",
                value: 11.0,
                rank: 9,
              },
              {
                estimate: "19242",
                value: 8.0,
                rank: 10,
              },
              {
                estimate: "19234",
                value: 10.0,
                rank: 11,
              },
              {
                estimate: "19211",
                value: 1.3036621231,
                rank: 12,
              },
              {
                estimate: "19211",
                value: 3.3564477518,
                rank: 13,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "1819252",
              jsonValue: "0.0",
            },
            {
              estimate: "31726",
              jsonValue: "1.0",
            },
            {
              estimate: "23674",
              jsonValue: "2.0",
            },
            {
              estimate: "20684",
              jsonValue: "3.0",
            },
            {
              estimate: "19973",
              jsonValue: "4.0",
            },
            {
              estimate: "19347",
              jsonValue: "5.0",
            },
            {
              estimate: "19323",
              jsonValue: "6.0",
            },
            {
              estimate: "19276",
              jsonValue: "7.0",
            },
            {
              estimate: "19266",
              jsonValue: "9.0",
            },
            {
              estimate: "19244",
              jsonValue: "11.0",
            },
            {
              estimate: "19242",
              jsonValue: "8.0",
            },
            {
              estimate: "19234",
              jsonValue: "10.0",
            },
            {
              estimate: "19211",
              jsonValue: "1.3036621231",
            },
            {
              estimate: "19211",
              jsonValue: "3.3564477518",
            },
          ],
        },
        uniqueCount: {
          estimate: 452829.55509651004,
          upper: 460279.95109018334,
          lower: 445630.09827602416,
        },
      },
      num_sats: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -128.35191993142763,
          max: 220.47375484885768,
          mean: 11.95755862040437,
          stddev: 14.468153940736423,
          histogram: {
            start: -128.35191345214844,
            end: 220.473776930188,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "64",
              "8192",
              "43010",
              "284673",
              "917505",
              "641025",
              "249856",
              "99328",
              "30720",
              "9216",
              "12288",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 220.4737548828125,
            min: -128.35191345214844,
            bins: [
              -128.35191345214844, -116.72439043940389, -105.09686742665934, -93.4693444139148, -81.84182140117025,
              -70.2142983884257, -58.58677537568114, -46.959252362936596, -35.33172935019205, -23.704206337447502,
              -12.076683324702955, -0.44916031195840844, 11.178362700786153, 22.8058857135307, 34.433408726275246,
              46.06093173901979, 57.68845475176434, 69.31597776450889, 80.94350077725343, 92.57102378999798,
              104.19854680274253, 115.82606981548707, 127.45359282823162, 139.08111584097617, 150.70863885372074,
              162.33616186646526, 173.96368487920984, 185.59120789195435, 197.21873090469893, 208.84625391744345,
              220.47377693018802,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2283052.4815605367,
            upper: 2319253.808980293,
            lower: 2247411.890537042,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -128.35191345214844, -17.779600143432617, -7.363513469696045, 3.1586673259735107, 9.858644485473633,
              18.56801986694336, 37.75645446777344, 58.176368713378906, 220.4737548828125,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "91881",
                value: 9.0,
                rank: 0,
              },
              {
                estimate: "91671",
                value: 7.0,
                rank: 1,
              },
              {
                estimate: "91335",
                value: 10.0,
                rank: 2,
              },
              {
                estimate: "91171",
                value: 8.0,
                rank: 3,
              },
              {
                estimate: "89649",
                value: 11.0,
                rank: 4,
              },
              {
                estimate: "89419",
                value: 12.0,
                rank: 5,
              },
              {
                estimate: "89410",
                value: 13.0,
                rank: 6,
              },
              {
                estimate: "89393",
                value: 6.0,
                rank: 7,
              },
              {
                estimate: "88729",
                value: 14.0,
                rank: 8,
              },
              {
                estimate: "88016",
                value: 15.0,
                rank: 9,
              },
              {
                estimate: "87608",
                value: 16.0,
                rank: 10,
              },
              {
                estimate: "87366",
                value: -3.3450612805,
                rank: 11,
              },
              {
                estimate: "87366",
                value: 16.390328725,
                rank: 12,
              },
              {
                estimate: "87366",
                value: 1.2136729083,
                rank: 13,
              },
              {
                estimate: "87366",
                value: 36.8792313412,
                rank: 14,
              },
              {
                estimate: "87366",
                value: 10.210518586,
                rank: 15,
              },
              {
                estimate: "87366",
                value: 4.3690692644,
                rank: 16,
              },
              {
                estimate: "87366",
                value: 25.7782045495,
                rank: 17,
              },
              {
                estimate: "87366",
                value: -1.2009805803,
                rank: 18,
              },
              {
                estimate: "87366",
                value: -0.9815363232000001,
                rank: 19,
              },
              {
                estimate: "87366",
                value: 44.9150637117,
                rank: 20,
              },
              {
                estimate: "87366",
                value: 15.7799601732,
                rank: 21,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "91881",
              jsonValue: "9.0",
            },
            {
              estimate: "91671",
              jsonValue: "7.0",
            },
            {
              estimate: "91335",
              jsonValue: "10.0",
            },
            {
              estimate: "91171",
              jsonValue: "8.0",
            },
            {
              estimate: "89649",
              jsonValue: "11.0",
            },
            {
              estimate: "89419",
              jsonValue: "12.0",
            },
            {
              estimate: "89410",
              jsonValue: "13.0",
            },
            {
              estimate: "89393",
              jsonValue: "6.0",
            },
            {
              estimate: "88729",
              jsonValue: "14.0",
            },
            {
              estimate: "88016",
              jsonValue: "15.0",
            },
            {
              estimate: "87608",
              jsonValue: "16.0",
            },
            {
              estimate: "87366",
              jsonValue: "-3.3450612805",
            },
            {
              estimate: "87366",
              jsonValue: "16.390328725",
            },
            {
              estimate: "87366",
              jsonValue: "1.2136729083",
            },
            {
              estimate: "87366",
              jsonValue: "36.8792313412",
            },
            {
              estimate: "87366",
              jsonValue: "10.210518586",
            },
            {
              estimate: "87366",
              jsonValue: "4.3690692644",
            },
            {
              estimate: "87366",
              jsonValue: "25.7782045495",
            },
            {
              estimate: "87366",
              jsonValue: "-1.2009805803",
            },
            {
              estimate: "87366",
              jsonValue: "-0.9815363232",
            },
            {
              estimate: "87366",
              jsonValue: "44.9150637117",
            },
            {
              estimate: "87366",
              jsonValue: "15.7799601732",
            },
          ],
        },
        uniqueCount: {
          estimate: 2236930.9077063943,
          upper: 2273735.089070722,
          lower: 2201366.33975983,
        },
      },
      last_pymnt_d: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            NULL: "33562",
            STRING: "2266411",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 37.0,
            upper: 37.0,
            lower: 37.0,
          },
          frequent: {
            items: [
              {
                value: "Mar-2019",
                estimate: 622124.0,
              },
              {
                value: "Feb-2019",
                estimate: 174793.0,
              },
              {
                value: "Mar-2017",
                estimate: 77206.0,
              },
              {
                value: "Mar-2018",
                estimate: 77043.0,
              },
              {
                value: "Jul-2017",
                estimate: 76894.0,
              },
              {
                value: "Aug-2017",
                estimate: 76894.0,
              },
              {
                value: "Nov-2016",
                estimate: 76891.0,
              },
              {
                value: "May-2017",
                estimate: 76889.0,
              },
              {
                value: "Jan-2017",
                estimate: 76889.0,
              },
              {
                value: "Sep-2017",
                estimate: 76889.0,
              },
              {
                value: "Nov-2017",
                estimate: 76889.0,
              },
              {
                value: "Jun-2018",
                estimate: 76889.0,
              },
              {
                value: "Feb-2018",
                estimate: 76889.0,
              },
              {
                value: "May-2018",
                estimate: 76888.0,
              },
              {
                value: "Apr-2017",
                estimate: 76888.0,
              },
              {
                value: "Jun-2017",
                estimate: 76888.0,
              },
              {
                value: "Aug-2016",
                estimate: 76888.0,
              },
              {
                value: "Jan-2018",
                estimate: 76887.0,
              },
              {
                value: "Dec-2017",
                estimate: 76887.0,
              },
              {
                value: "Oct-2016",
                estimate: 76887.0,
              },
              {
                value: "Oct-2018",
                estimate: 76887.0,
              },
              {
                value: "Jun-2016",
                estimate: 76887.0,
              },
              {
                value: "Nov-2018",
                estimate: 76887.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "622124",
              jsonValue: '"Mar-2019"',
            },
            {
              estimate: "174793",
              jsonValue: '"Feb-2019"',
            },
            {
              estimate: "77206",
              jsonValue: '"Mar-2017"',
            },
            {
              estimate: "77042",
              jsonValue: '"Mar-2018"',
            },
            {
              estimate: "76895",
              jsonValue: '"Jul-2017"',
            },
            {
              estimate: "76894",
              jsonValue: '"Aug-2017"',
            },
            {
              estimate: "76890",
              jsonValue: '"Sep-2017"',
            },
            {
              estimate: "76890",
              jsonValue: '"Nov-2017"',
            },
            {
              estimate: "76890",
              jsonValue: '"Jan-2017"',
            },
            {
              estimate: "76890",
              jsonValue: '"Jun-2018"',
            },
            {
              estimate: "76889",
              jsonValue: '"Jun-2017"',
            },
            {
              estimate: "76889",
              jsonValue: '"Feb-2018"',
            },
            {
              estimate: "76888",
              jsonValue: '"May-2018"',
            },
            {
              estimate: "76888",
              jsonValue: '"May-2017"',
            },
            {
              estimate: "76888",
              jsonValue: '"Nov-2016"',
            },
            {
              estimate: "76888",
              jsonValue: '"Nov-2018"',
            },
            {
              estimate: "76888",
              jsonValue: '"Jan-2018"',
            },
            {
              estimate: "76888",
              jsonValue: '"Oct-2018"',
            },
            {
              estimate: "76888",
              jsonValue: '"Apr-2017"',
            },
          ],
        },
        uniqueCount: {
          estimate: 37.00000330805813,
          upper: 37.00185069049617,
          lower: 37.0,
        },
      },
      grade: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2299973",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 7.0,
            upper: 7.0,
            lower: 7.0,
          },
          frequent: {
            items: [
              {
                value: "B",
                estimate: 655316.0,
              },
              {
                value: "C",
                estimate: 625777.0,
              },
              {
                value: "A",
                estimate: 486759.0,
              },
              {
                value: "D",
                estimate: 293670.0,
              },
              {
                value: "E",
                estimate: 161273.0,
              },
              {
                value: "F",
                estimate: 61072.0,
              },
              {
                value: "G",
                estimate: 16106.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "655316",
              jsonValue: '"B"',
            },
            {
              estimate: "625777",
              jsonValue: '"C"',
            },
            {
              estimate: "486759",
              jsonValue: '"A"',
            },
            {
              estimate: "293670",
              jsonValue: '"D"',
            },
            {
              estimate: "161273",
              jsonValue: '"E"',
            },
            {
              estimate: "61072",
              jsonValue: '"F"',
            },
            {
              estimate: "16106",
              jsonValue: '"G"',
            },
          ],
        },
        uniqueCount: {
          estimate: 7.000000104308129,
          upper: 7.000349609067664,
          lower: 7.0,
        },
      },
      sub_grade: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2299973",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 35.0,
            upper: 35.0,
            lower: 35.0,
          },
          frequent: {
            items: [
              {
                value: "A1",
                estimate: 180290.0,
              },
              {
                value: "C1",
                estimate: 153260.0,
              },
              {
                value: "B3",
                estimate: 142612.0,
              },
              {
                value: "B4",
                estimate: 142031.0,
              },
              {
                value: "B5",
                estimate: 139389.0,
              },
              {
                value: "C2",
                estimate: 132397.0,
              },
              {
                value: "C3",
                estimate: 124248.0,
              },
              {
                value: "B2",
                estimate: 123052.0,
              },
              {
                value: "C4",
                estimate: 118818.0,
              },
              {
                value: "A4",
                estimate: 116000.0,
              },
              {
                value: "B1",
                estimate: 114607.0,
              },
              {
                value: "C5",
                estimate: 112007.0,
              },
              {
                value: "A5",
                estimate: 110808.0,
              },
              {
                value: "D1",
                estimate: 110808.0,
              },
              {
                value: "D3",
                estimate: 110807.0,
              },
              {
                value: "D4",
                estimate: 110806.0,
              },
              {
                value: "A3",
                estimate: 110806.0,
              },
              {
                value: "E2",
                estimate: 110805.0,
              },
              {
                value: "E4",
                estimate: 110805.0,
              },
              {
                value: "E5",
                estimate: 110805.0,
              },
              {
                value: "D2",
                estimate: 110805.0,
              },
              {
                value: "A2",
                estimate: 110805.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "180290",
              jsonValue: '"A1"',
            },
            {
              estimate: "153260",
              jsonValue: '"C1"',
            },
            {
              estimate: "142612",
              jsonValue: '"B3"',
            },
            {
              estimate: "142031",
              jsonValue: '"B4"',
            },
            {
              estimate: "139389",
              jsonValue: '"B5"',
            },
            {
              estimate: "132397",
              jsonValue: '"C2"',
            },
            {
              estimate: "124248",
              jsonValue: '"C3"',
            },
            {
              estimate: "123052",
              jsonValue: '"B2"',
            },
            {
              estimate: "118818",
              jsonValue: '"C4"',
            },
            {
              estimate: "116000",
              jsonValue: '"A4"',
            },
            {
              estimate: "114607",
              jsonValue: '"B1"',
            },
            {
              estimate: "112005",
              jsonValue: '"C5"',
            },
            {
              estimate: "110808",
              jsonValue: '"A5"',
            },
            {
              estimate: "110808",
              jsonValue: '"D1"',
            },
            {
              estimate: "110807",
              jsonValue: '"D3"',
            },
            {
              estimate: "110806",
              jsonValue: '"E1"',
            },
            {
              estimate: "110806",
              jsonValue: '"D4"',
            },
            {
              estimate: "110806",
              jsonValue: '"A3"',
            },
            {
              estimate: "110805",
              jsonValue: '"E4"',
            },
            {
              estimate: "110805",
              jsonValue: '"E5"',
            },
            {
              estimate: "110805",
              jsonValue: '"D2"',
            },
            {
              estimate: "110805",
              jsonValue: '"A2"',
            },
            {
              estimate: "110805",
              jsonValue: '"E2"',
            },
          ],
        },
        uniqueCount: {
          estimate: 35.000002955397264,
          upper: 35.001750479316456,
          lower: 35.0,
        },
      },
      emp_title: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 0.9999265637592619,
          },
          typeCounts: {
            UNKNOWN: "97",
            INTEGRAL: "60",
            NULL: "162064",
            STRING: "2137752",
          },
        },
        numberSummary: {
          count: "60",
          min: 1.0,
          max: 1.0,
          mean: 1.0,
          histogram: {
            start: 1.0,
            end: 1.0000001,
            counts: ["60"],
            max: 1.0,
            min: 1.0,
            bins: [1.0, 1.0000001],
            n: "60",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 1.0,
            upper: 1.0,
            lower: 1.0,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
          },
          frequentNumbers: {
            longs: [
              {
                estimate: "60",
                value: "1",
                rank: 0,
              },
            ],
            doubles: [],
          },
          isDiscrete: true,
          stddev: 0.0,
        },
        stringSummary: {
          uniqueCount: {
            estimate: 20774.983682191625,
            upper: 21070.93465927992,
            lower: 20483.140226382777,
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "85519",
              jsonValue: '"Manager"',
            },
            {
              estimate: "85517",
              jsonValue: '"Solution Architect"',
            },
            {
              estimate: "85517",
              jsonValue: '"Vice President, Analytics"',
            },
            {
              estimate: "85517",
              jsonValue: '"Nanny"',
            },
            {
              estimate: "85517",
              jsonValue: '"Mail handling Clarke"',
            },
            {
              estimate: "85517",
              jsonValue: '"Supervisory Principal"',
            },
            {
              estimate: "85517",
              jsonValue: '"Project Manager"',
            },
          ],
        },
        uniqueCount: {
          estimate: 19028.0619699575,
          upper: 19341.130308966698,
          lower: 18725.53818592348,
        },
      },
      tot_hi_cred_lim: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -24471871.323798735,
          max: 34713800.07815558,
          mean: 180176.35307836824,
          stddev: 321604.48001359304,
          histogram: {
            start: -24471872.0,
            end: 34713803.47138,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "2112",
              "2258949",
              "34816",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 34713800.0,
            min: -24471872.0,
            bins: [
              -24471872.0, -22499016.150954, -20526160.301908, -18553304.452862002, -16580448.603815999,
              -14607592.75477, -12634736.905724, -10661881.056677999, -8689025.207632, -6716169.358585998,
              -4743313.509539999, -2770457.6604939997, -797601.8114480004, 1175254.037597999, 3148109.886644002,
              5120965.735690001, 7093821.584736001, 9066677.433782, 11039533.282828003, 13012389.131874003,
              14985244.980920002, 16958100.829966, 18930956.679012, 20903812.528058, 22876668.377104, 24849524.22615,
              26822380.075195998, 28795235.924242005, 30768091.773288004, 32740947.622334003, 34713803.47138,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2208517.642018579,
            upper: 2243536.0584848616,
            lower: 2174041.6228066026,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -24471872.0, -283279.4375, -87130.328125, 20570.935546875, 85825.6875, 251732.796875, 747162.75,
              1327132.125, 34713800.0,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "87256",
                value: 1489432.6571343068,
                rank: 0,
              },
              {
                estimate: "87256",
                value: 708128.3670523112,
                rank: 1,
              },
              {
                estimate: "87256",
                value: -105217.3535290142,
                rank: 2,
              },
              {
                estimate: "87256",
                value: 126994.5399169591,
                rank: 3,
              },
              {
                estimate: "87256",
                value: -34778.51206285,
                rank: 4,
              },
              {
                estimate: "87256",
                value: -1394.1068400571,
                rank: 5,
              },
              {
                estimate: "87256",
                value: 47478.0265034534,
                rank: 6,
              },
              {
                estimate: "87256",
                value: 72690.0346699328,
                rank: 7,
              },
              {
                estimate: "87256",
                value: 1152951.7529895732,
                rank: 8,
              },
              {
                estimate: "87256",
                value: 207679.5333556925,
                rank: 9,
              },
              {
                estimate: "87256",
                value: -86190.6652265346,
                rank: 10,
              },
              {
                estimate: "87256",
                value: 35464.0207224955,
                rank: 11,
              },
              {
                estimate: "87256",
                value: 805703.2686045804,
                rank: 12,
              },
              {
                estimate: "87256",
                value: 423764.1210270221,
                rank: 13,
              },
              {
                estimate: "87256",
                value: 216174.5455111687,
                rank: 14,
              },
              {
                estimate: "87256",
                value: 371109.4183790956,
                rank: 15,
              },
              {
                estimate: "87256",
                value: 112654.2277449534,
                rank: 16,
              },
              {
                estimate: "87256",
                value: 9027.8802573156,
                rank: 17,
              },
              {
                estimate: "87256",
                value: -1329.6834734888,
                rank: 18,
              },
              {
                estimate: "87256",
                value: 412877.1814822696,
                rank: 19,
              },
              {
                estimate: "87256",
                value: 273522.6302427909,
                rank: 20,
              },
              {
                estimate: "87256",
                value: 57241.4253413058,
                rank: 21,
              },
              {
                estimate: "87256",
                value: 322274.3160108923,
                rank: 22,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "87256",
              jsonValue: "1489432.6571343068",
            },
            {
              estimate: "87256",
              jsonValue: "708128.3670523112",
            },
            {
              estimate: "87256",
              jsonValue: "-105217.3535290142",
            },
            {
              estimate: "87256",
              jsonValue: "126994.5399169591",
            },
            {
              estimate: "87256",
              jsonValue: "-34778.51206285",
            },
            {
              estimate: "87256",
              jsonValue: "-1394.1068400571",
            },
            {
              estimate: "87256",
              jsonValue: "47478.0265034534",
            },
            {
              estimate: "87256",
              jsonValue: "72690.0346699328",
            },
            {
              estimate: "87256",
              jsonValue: "1152951.7529895732",
            },
            {
              estimate: "87256",
              jsonValue: "207679.5333556925",
            },
            {
              estimate: "87256",
              jsonValue: "-86190.6652265346",
            },
            {
              estimate: "87256",
              jsonValue: "35464.0207224955",
            },
            {
              estimate: "87256",
              jsonValue: "805703.2686045804",
            },
            {
              estimate: "87256",
              jsonValue: "423764.1210270221",
            },
            {
              estimate: "87256",
              jsonValue: "216174.5455111687",
            },
            {
              estimate: "87256",
              jsonValue: "371109.4183790956",
            },
            {
              estimate: "87256",
              jsonValue: "112654.2277449534",
            },
            {
              estimate: "87256",
              jsonValue: "9027.8802573156",
            },
            {
              estimate: "87256",
              jsonValue: "-1329.6834734888",
            },
            {
              estimate: "87256",
              jsonValue: "412877.1814822696",
            },
            {
              estimate: "87256",
              jsonValue: "273522.6302427909",
            },
            {
              estimate: "87256",
              jsonValue: "57241.4253413058",
            },
            {
              estimate: "87256",
              jsonValue: "322274.3160108923",
            },
          ],
        },
        uniqueCount: {
          estimate: 2250957.3410557527,
          upper: 2287992.299059227,
          lower: 2215169.7693319223,
        },
      },
      out_prncp_inv: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -53920.38420077515,
          max: 96161.638092744,
          mean: 1226.5568187585552,
          stddev: 5081.507257363204,
          histogram: {
            start: -53920.3828125,
            end: 96161.65024116407,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "64",
              "0",
              "2048",
              "2",
              "10240",
              "2030595",
              "84992",
              "56320",
              "41984",
              "30720",
              "17408",
              "4096",
              "12288",
              "0",
              "5120",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 96161.640625,
            min: -53920.3828125,
            bins: [
              -53920.3828125, -48917.64837737786, -43914.91394225573, -38912.179507133595, -33909.445072011455,
              -28906.71063688932, -23903.976201767182, -18901.24176664505, -13898.50733152291, -8895.77289640077,
              -3893.0384612786365, 1109.6959738434962, 6112.430408965636, 11115.164844087776, 16117.899279209902,
              21120.63371433204, 26123.36814945418, 31126.10258457632, 36128.83701969846, 41131.57145482059,
              46134.30588994273, 51137.04032506487, 56139.77476018699, 61142.50919530913, 66145.24363043127,
              71147.97806555341, 76150.71250067555, 81153.44693579769, 86156.1813709198, 91158.91580604194,
              96161.65024116408,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 441566.71287675644,
            upper: 448542.5493411252,
            lower: 434698.51906662073,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -53920.3828125, -2511.248779296875, 0.0, 0.0, 0.0, 0.0, 11227.4462890625, 28702.466796875, 96161.640625,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "1841031",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "19123",
                value: 5.4184595934,
                rank: 1,
              },
              {
                estimate: "19123",
                value: -87.9917987979,
                rank: 2,
              },
              {
                estimate: "19123",
                value: -175.8025608231,
                rank: 3,
              },
              {
                estimate: "19123",
                value: 47.2920365216,
                rank: 4,
              },
              {
                estimate: "19123",
                value: -0.0686081576,
                rank: 5,
              },
              {
                estimate: "19123",
                value: 15398.1444191117,
                rank: 6,
              },
              {
                estimate: "19123",
                value: 13223.247014678,
                rank: 7,
              },
              {
                estimate: "19123",
                value: 12001.0626757297,
                rank: 8,
              },
              {
                estimate: "19123",
                value: 17357.5824637006,
                rank: 9,
              },
              {
                estimate: "19123",
                value: 8.6739429763,
                rank: 10,
              },
              {
                estimate: "19123",
                value: -5030.3439153244,
                rank: 11,
              },
              {
                estimate: "19123",
                value: 11.0205504545,
                rank: 12,
              },
              {
                estimate: "19123",
                value: 22349.0106328968,
                rank: 13,
              },
              {
                estimate: "19123",
                value: 31195.7080305412,
                rank: 14,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "1841031",
              jsonValue: "0.0",
            },
            {
              estimate: "19123",
              jsonValue: "5.4184595934",
            },
            {
              estimate: "19123",
              jsonValue: "-87.9917987979",
            },
            {
              estimate: "19123",
              jsonValue: "-175.8025608231",
            },
            {
              estimate: "19123",
              jsonValue: "47.2920365216",
            },
            {
              estimate: "19123",
              jsonValue: "-0.0686081576",
            },
            {
              estimate: "19123",
              jsonValue: "15398.1444191117",
            },
            {
              estimate: "19123",
              jsonValue: "13223.247014678",
            },
            {
              estimate: "19123",
              jsonValue: "12001.0626757297",
            },
            {
              estimate: "19123",
              jsonValue: "17357.5824637006",
            },
            {
              estimate: "19123",
              jsonValue: "8.6739429763",
            },
            {
              estimate: "19123",
              jsonValue: "-5030.3439153244",
            },
            {
              estimate: "19123",
              jsonValue: "11.0205504545",
            },
            {
              estimate: "19123",
              jsonValue: "22349.0106328968",
            },
            {
              estimate: "19123",
              jsonValue: "31195.7080305412",
            },
          ],
        },
        uniqueCount: {
          estimate: 448326.8013791313,
          upper: 455703.1136521665,
          lower: 441198.9330417661,
        },
      },
      verification_status_joint: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            NULL: "2246258",
            STRING: "53715",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 1.0,
            upper: 1.0,
            lower: 1.0,
          },
          frequent: {
            items: [
              {
                value: "Not Verified",
                estimate: 53715.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "53715",
              jsonValue: '"Not Verified"',
            },
          ],
        },
        uniqueCount: {
          estimate: 1.0,
          upper: 1.000049929250618,
          lower: 1.0,
        },
      },
      home_ownership: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2299973",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 3.0,
            upper: 3.0,
            lower: 3.0,
          },
          frequent: {
            items: [
              {
                value: "MORTGAGE",
                estimate: 1133902.0,
              },
              {
                value: "RENT",
                estimate: 888500.0,
              },
              {
                value: "OWN",
                estimate: 277571.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "1133902",
              jsonValue: '"MORTGAGE"',
            },
            {
              estimate: "888500",
              jsonValue: '"RENT"',
            },
            {
              estimate: "277571",
              jsonValue: '"OWN"',
            },
          ],
        },
        uniqueCount: {
          estimate: 3.000000014901161,
          upper: 3.0001498026537594,
          lower: 3.0,
        },
      },
      mths_since_recent_bc: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2280288",
            NULL: "19685",
          },
        },
        numberSummary: {
          count: "2280288",
          min: -1120.4645108804973,
          max: 2319.0560777710925,
          mean: 23.839732513315273,
          stddev: 51.46975318449788,
          histogram: {
            start: -1120.4644775390625,
            end: 2319.0563842493652,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "64",
              "4896",
              "1667072",
              "535552",
              "55296",
              "9216",
              "8192",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 2319.05615234375,
            min: -1120.4644775390625,
            bins: [
              -1120.4644775390625, -1005.8137821461149, -891.1630867531674, -776.5123913602197, -661.8616959672721,
              -547.2110005743245, -432.56030518137686, -317.9096097884293, -203.25891439548172, -88.60821900253404,
              26.042476390413412, 140.6931717833611, 255.34386717630878, 369.99456256925623, 484.6452579622039,
              599.2959533551514, 713.946648748099, 828.5973441410467, 943.2480395339944, 1057.8987349269419,
              1172.5494303198893, 1287.2001257128372, 1401.8508211057847, 1516.5015164987321, 1631.15221189168,
              1745.8029072846275, 1860.453602677575, 1975.1042980705224, 2089.7549934634703, 2204.405688856418,
              2319.0563842493652,
            ],
            n: "2280288",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2196938.086151242,
            upper: 2231772.7283748155,
            lower: 2162642.992056382,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -1120.4644775390625, -42.87798309326172, -9.532422065734863, 2.011122941970825, 9.607108116149902,
              28.04412841796875, 108.18061828613281, 224.1678009033203, 2319.05615234375,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "93067",
                value: 5.0,
                rank: 0,
              },
              {
                estimate: "92639",
                value: 3.0,
                rank: 1,
              },
              {
                estimate: "92111",
                value: 4.0,
                rank: 2,
              },
              {
                estimate: "91904",
                value: 2.0,
                rank: 3,
              },
              {
                estimate: "91550",
                value: 6.0,
                rank: 4,
              },
              {
                estimate: "91496",
                value: 9.0,
                rank: 5,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "93067",
              jsonValue: "5.0",
            },
            {
              estimate: "92639",
              jsonValue: "3.0",
            },
            {
              estimate: "92111",
              jsonValue: "4.0",
            },
            {
              estimate: "91904",
              jsonValue: "2.0",
            },
            {
              estimate: "91550",
              jsonValue: "6.0",
            },
            {
              estimate: "91496",
              jsonValue: "9.0",
            },
          ],
        },
        uniqueCount: {
          estimate: 2120958.8638027795,
          upper: 2155854.9593508146,
          lower: 2087238.114823132,
        },
      },
      acc_now_delinq: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -3.648169028193079,
          max: 7.58168399320003,
          mean: 0.006996922970230361,
          stddev: 0.12136162821549165,
          histogram: {
            start: -3.6481690406799316,
            end: 7.58168487071724,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "1024",
              "0",
              "1088",
              "2281477",
              "8192",
              "0",
              "0",
              "4096",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 7.581684112548828,
            min: -3.6481690406799316,
            bins: [
              -3.6481690406799316, -3.2738405769666925, -2.8995121132534534, -2.5251836495402147, -2.1508551858269755,
              -1.7765267221137364, -1.4021982584004977, -1.0278697946872586, -0.6535413309740195, -0.2792128672607803,
              0.09511559645245882, 0.4694440601656975, 0.8437725238789362, 1.2181009875921758, 1.5924294513054145,
              1.966757915018654, 2.3410863787318927, 2.7154148424451314, 3.089743306158371, 3.4640717698716097,
              3.8384002335848493, 4.212728697298088, 4.587057161011327, 4.961385624724565, 5.335714088437804,
              5.7100425521510445, 6.084371015864283, 6.458699479577522, 6.833027943290761, 7.207356407003999,
              7.58168487071724,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 15266.381971626784,
            upper: 15474.169312917998,
            lower: 15061.346681431216,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [-3.6481690406799316, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 7.581684112548828],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "2284296",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "1291",
                value: 1.0,
                rank: 1,
              },
              {
                estimate: "626",
                value: 0.9121798711,
                rank: 2,
              },
              {
                estimate: "626",
                value: 2.4100203241,
                rank: 3,
              },
              {
                estimate: "626",
                value: 1.1381631816,
                rank: 4,
              },
              {
                estimate: "626",
                value: 0.7737641391000001,
                rank: 5,
              },
              {
                estimate: "626",
                value: 1.5582691283,
                rank: 6,
              },
              {
                estimate: "626",
                value: 0.7483993531,
                rank: 7,
              },
              {
                estimate: "626",
                value: -0.058711145000000006,
                rank: 8,
              },
              {
                estimate: "626",
                value: -0.5601260965,
                rank: 9,
              },
              {
                estimate: "626",
                value: 2.109004466,
                rank: 10,
              },
              {
                estimate: "626",
                value: 0.6861881146000001,
                rank: 11,
              },
              {
                estimate: "626",
                value: 0.5608922889,
                rank: 12,
              },
            ],
            longs: [],
          },
          isDiscrete: true,
        },
        frequentItems: {
          items: [
            {
              estimate: "2284296",
              jsonValue: "0.0",
            },
            {
              estimate: "1291",
              jsonValue: "1.0",
            },
            {
              estimate: "626",
              jsonValue: "0.9121798711",
            },
            {
              estimate: "626",
              jsonValue: "2.4100203241",
            },
            {
              estimate: "626",
              jsonValue: "1.1381631816",
            },
            {
              estimate: "626",
              jsonValue: "0.7737641391",
            },
            {
              estimate: "626",
              jsonValue: "1.5582691283",
            },
            {
              estimate: "626",
              jsonValue: "0.7483993531",
            },
            {
              estimate: "626",
              jsonValue: "-0.058711145",
            },
            {
              estimate: "626",
              jsonValue: "-0.5601260965",
            },
            {
              estimate: "626",
              jsonValue: "2.109004466",
            },
            {
              estimate: "626",
              jsonValue: "0.6861881146",
            },
            {
              estimate: "626",
              jsonValue: "0.5608922889",
            },
          ],
        },
        uniqueCount: {
          estimate: 15086.955853538988,
          upper: 15335.18124912735,
          lower: 14847.0910169848,
        },
      },
      purpose: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2299973",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 12.0,
            upper: 12.0,
            lower: 12.0,
          },
          frequent: {
            items: [
              {
                value: "debt_consolidation",
                estimate: 1313207.0,
              },
              {
                value: "credit_card",
                estimate: 517137.0,
              },
              {
                value: "home_improvement",
                estimate: 164382.0,
              },
              {
                value: "other",
                estimate: 134836.0,
              },
              {
                value: "major_purchase",
                estimate: 57583.0,
              },
              {
                value: "small_business",
                estimate: 26010.0,
              },
              {
                value: "medical",
                estimate: 24816.0,
              },
              {
                value: "car",
                estimate: 23250.0,
              },
              {
                value: "vacation",
                estimate: 14886.0,
              },
              {
                value: "moving",
                estimate: 13261.0,
              },
              {
                value: "house",
                estimate: 9193.0,
              },
              {
                value: "renewable_energy",
                estimate: 1412.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "1313207",
              jsonValue: '"debt_consolidation"',
            },
            {
              estimate: "517137",
              jsonValue: '"credit_card"',
            },
            {
              estimate: "164382",
              jsonValue: '"home_improvement"',
            },
            {
              estimate: "134836",
              jsonValue: '"other"',
            },
            {
              estimate: "57583",
              jsonValue: '"major_purchase"',
            },
            {
              estimate: "26010",
              jsonValue: '"small_business"',
            },
            {
              estimate: "24816",
              jsonValue: '"medical"',
            },
            {
              estimate: "23250",
              jsonValue: '"car"',
            },
            {
              estimate: "14886",
              jsonValue: '"vacation"',
            },
            {
              estimate: "13261",
              jsonValue: '"moving"',
            },
            {
              estimate: "9193",
              jsonValue: '"house"',
            },
            {
              estimate: "1412",
              jsonValue: '"renewable_energy"',
            },
          ],
        },
        uniqueCount: {
          estimate: 12.000000327825557,
          upper: 12.000599478849342,
          lower: 12.0,
        },
      },
      int_rate: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -78.6321155241939,
          max: 137.47348824578305,
          mean: 12.527061510519886,
          stddev: 14.371421383885181,
          histogram: {
            start: -78.63211822509766,
            end: 137.473509230748,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "6208",
              "5120",
              "34818",
              "87040",
              "254976",
              "513025",
              "550913",
              "374784",
              "219136",
              "116736",
              "64513",
              "33792",
              "21504",
              "8192",
              "9216",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 137.47349548339844,
            min: -78.63211822509766,
            bins: [
              -78.63211822509766, -71.4285973099028, -64.22507639470794, -57.02155547951309, -49.81803456431824,
              -42.61451364912338, -35.41099273392852, -28.207471818733673, -21.003950903538815, -13.800429988343964,
              -6.5969090731491065, 0.6066118420457514, 7.810132757240609, 15.013653672435467, 22.21717458763031,
              29.42069550282517, 36.624216418020026, 43.827737333214884, 51.03125824840973, 58.2347791636046,
              65.43830007879944, 72.64182099399432, 79.84534190918916, 87.048862824384, 94.25238373957887,
              101.45590465477372, 108.65942556996859, 115.86294648516343, 123.06646740035828, 130.26998831555315,
              137.473509230748,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2226316.1505703013,
            upper: 2261617.0396503448,
            lower: 2191562.0380305056,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -78.63211822509766, -18.04680061340332, -7.827950954437256, 3.6584231853485107, 10.789743423461914,
              19.832441329956055, 38.97895431518555, 57.127532958984375, 137.47349548339844,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "91024",
                value: 5.32,
                rank: 0,
              },
              {
                estimate: "89669",
                value: 12.99,
                rank: 1,
              },
              {
                estimate: "89351",
                value: 11.99,
                rank: 2,
              },
              {
                estimate: "89312",
                value: 10.75,
                rank: 3,
              },
              {
                estimate: "89221",
                value: 9.75,
                rank: 4,
              },
              {
                estimate: "89013",
                value: 11.47,
                rank: 5,
              },
              {
                estimate: "88489",
                value: 13.67,
                rank: 6,
              },
              {
                estimate: "88453",
                value: 14.46,
                rank: 7,
              },
              {
                estimate: "88270",
                value: 9.16,
                rank: 8,
              },
              {
                estimate: "88224",
                value: 8.39,
                rank: 9,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "91024",
              jsonValue: "5.32",
            },
            {
              estimate: "89669",
              jsonValue: "12.99",
            },
            {
              estimate: "89351",
              jsonValue: "11.99",
            },
            {
              estimate: "89312",
              jsonValue: "10.75",
            },
            {
              estimate: "89221",
              jsonValue: "9.75",
            },
            {
              estimate: "89013",
              jsonValue: "11.47",
            },
            {
              estimate: "88489",
              jsonValue: "13.67",
            },
            {
              estimate: "88453",
              jsonValue: "14.46",
            },
            {
              estimate: "88270",
              jsonValue: "9.16",
            },
            {
              estimate: "88224",
              jsonValue: "8.39",
            },
          ],
        },
        uniqueCount: {
          estimate: 2226134.0101835765,
          upper: 2262760.5503998324,
          lower: 2190741.100196692,
        },
      },
      addr_state: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2299973",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 50.0,
            upper: 50.0,
            lower: 50.0,
          },
          frequent: {
            items: [
              {
                value: "CA",
                estimate: 296870.0,
              },
              {
                value: "TX",
                estimate: 195392.0,
              },
              {
                value: "NY",
                estimate: 182510.0,
              },
              {
                value: "FL",
                estimate: 162663.0,
              },
              {
                value: "IL",
                estimate: 94972.0,
              },
              {
                value: "OH",
                estimate: 91827.0,
              },
              {
                value: "NJ",
                estimate: 89609.0,
              },
              {
                value: "GA",
                estimate: 88444.0,
              },
              {
                value: "PA",
                estimate: 88395.0,
              },
              {
                value: "VA",
                estimate: 88355.0,
              },
              {
                value: "IN",
                estimate: 88353.0,
              },
              {
                value: "NC",
                estimate: 88353.0,
              },
              {
                value: "AZ",
                estimate: 88353.0,
              },
              {
                value: "WA",
                estimate: 88352.0,
              },
              {
                value: "KY",
                estimate: 88351.0,
              },
              {
                value: "SC",
                estimate: 88351.0,
              },
              {
                value: "WV",
                estimate: 88351.0,
              },
              {
                value: "MA",
                estimate: 88351.0,
              },
              {
                value: "TN",
                estimate: 88351.0,
              },
              {
                value: "ME",
                estimate: 88351.0,
              },
              {
                value: "OR",
                estimate: 88351.0,
              },
              {
                value: "KS",
                estimate: 88351.0,
              },
              {
                value: "MN",
                estimate: 88351.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "296870",
              jsonValue: '"CA"',
            },
            {
              estimate: "195392",
              jsonValue: '"TX"',
            },
            {
              estimate: "182510",
              jsonValue: '"NY"',
            },
            {
              estimate: "162663",
              jsonValue: '"FL"',
            },
            {
              estimate: "94972",
              jsonValue: '"IL"',
            },
            {
              estimate: "91827",
              jsonValue: '"OH"',
            },
            {
              estimate: "89609",
              jsonValue: '"NJ"',
            },
            {
              estimate: "88444",
              jsonValue: '"GA"',
            },
            {
              estimate: "88395",
              jsonValue: '"PA"',
            },
            {
              estimate: "88355",
              jsonValue: '"VA"',
            },
            {
              estimate: "88355",
              jsonValue: '"NC"',
            },
            {
              estimate: "88354",
              jsonValue: '"IN"',
            },
          ],
        },
        uniqueCount: {
          estimate: 50.00000608464168,
          upper: 50.00250254747639,
          lower: 50.0,
        },
      },
      next_pymnt_d: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "501129",
            NULL: "1798844",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 3.0,
            upper: 3.0,
            lower: 3.0,
          },
          frequent: {
            items: [
              {
                value: "Apr-2019",
                estimate: 499664.0,
              },
              {
                value: "Mar-2019",
                estimate: 1374.0,
              },
              {
                value: "May-2019",
                estimate: 91.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "499664",
              jsonValue: '"Apr-2019"',
            },
            {
              estimate: "1374",
              jsonValue: '"Mar-2019"',
            },
            {
              estimate: "91",
              jsonValue: '"May-2019"',
            },
          ],
        },
        uniqueCount: {
          estimate: 3.000000014901161,
          upper: 3.0001498026537594,
          lower: 3.0,
        },
      },
      desc: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            NULL: "2299786",
            STRING: "187",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 5.0,
            upper: 5.0,
            lower: 5.0,
          },
          frequent: {
            items: [
              {
                value:
                  "I would like to pay off 3 different credit cards, at 12%, 17% and 22% (after initial 0% period is up).  It would be great to have everything under one loan, making it easier to pay off.  Also, once I've paid off or down the loan, I can start looking into buying a house.",
                estimate: 49.0,
              },
              {
                value:
                  "I have problems with one of my loans what I have it is with household-beneficial finance bank it is for $15,250.00 and the interest rate is 25.8% and the payment is 323 monthly and I have 3 months pass due, I page all time in time but the situacion make loss that way cause the gas bill was very higher and the last mont the second day my Mother die and was very expensive for us her funeral cause she die here but we bring her body to Nicaragua, and the rest is for a loan with American General Finance $2,500.00, Wells Fargo $480.00 , Fifth Third Bank Optimun $357.00, all make a total of $18,587.00, please I need cause I get some problems with the big loan with household-beneficial.",
                estimate: 48.0,
              },
              {
                value:
                  "Hello, I have a credit card consolidation loan at 23% interest. Please help me to get a better rate!",
                estimate: 48.0,
              },
              {
                value: " ",
                estimate: 46.0,
              },
              {
                value:
                  "I developed poor credit in college due to not having a job and not being able to pay my credit card bills all the time. I am a recent graduate and am employeed with a very large and prestigous contractor. I have a steady income and want to get out of debt. Please help me out. Thank you",
                estimate: 42.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "49",
              jsonValue:
                '"I would like to pay off 3 different credit cards, at 12%, 17% and 22% (after initial 0% period is up).  It would be great to have everything under one loan, making it easier to pay off.  Also, once I\'ve paid off or down the loan, I can start looking into buying a house."',
            },
            {
              estimate: "48",
              jsonValue:
                '"I have problems with one of my loans what I have it is with household-beneficial finance bank it is for $15,250.00 and the interest rate is 25.8% and the payment is 323 monthly and I have 3 months pass due, I page all time in time but the situacion make loss that way cause the gas bill was very higher and the last mont the second day my Mother die and was very expensive for us her funeral cause she die here but we bring her body to Nicaragua, and the rest is for a loan with American General Finance $2,500.00, Wells Fargo $480.00 , Fifth Third Bank Optimun $357.00, all make a total of $18,587.00, please I need cause I get some problems with the big loan with household-beneficial."',
            },
            {
              estimate: "48",
              jsonValue:
                '"Hello, I have a credit card consolidation loan at 23% interest. Please help me to get a better rate!"',
            },
            {
              estimate: "42",
              jsonValue:
                '"I developed poor credit in college due to not having a job and not being able to pay my credit card bills all the time. I am a recent graduate and am employeed with a very large and prestigous contractor. I have a steady income and want to get out of debt. Please help me out. Thank you"',
            },
          ],
        },
        uniqueCount: {
          estimate: 4.000000029802323,
          upper: 4.000199746806284,
          lower: 4.0,
        },
      },
      mo_sin_old_il_acct: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2236345",
            NULL: "63628",
          },
        },
        numberSummary: {
          count: "2236345",
          min: -959.4747157670128,
          max: 2067.0752093721567,
          mean: 126.70055123554481,
          stddev: 146.23316859827827,
          histogram: {
            start: -959.4747314453125,
            end: 2067.0754020200197,
            counts: [
              "0",
              "0",
              "0",
              "256",
              "128",
              "544",
              "8192",
              "26628",
              "124928",
              "543751",
              "670728",
              "464900",
              "241666",
              "90112",
              "43008",
              "13312",
              "0",
              "8192",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 2067.0751953125,
            min: -959.4747314453125,
            bins: [
              -959.4747314453125, -858.5897269964681, -757.7047225476236, -656.8197180987793, -555.9347136499348,
              -455.0497092010904, -354.16470475224605, -253.27970030340157, -152.3946958545572, -51.50969140571283,
              49.375313043131655, 150.26031749197614, 251.1453219408204, 352.0303263896649, 452.91533083850936,
              553.8003352873536, 654.6853397361981, 755.5703441850426, 856.4553486338868, 957.3403530827313,
              1058.2253575315758, 1159.1103619804203, 1259.9953664292648, 1360.8803708781088, 1461.7653753269533,
              1562.6503797757978, 1663.5353842246423, 1764.4203886734867, 1865.3053931223312, 1966.1903975711753,
              2067.0754020200197,
            ],
            n: "2236345",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2162738.348771859,
            upper: 2197030.2213964486,
            lower: 2128977.609501864,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -959.4747314453125, -199.89736938476562, -76.4536361694336, 29.126859664916992, 111.44511413574219,
              208.8317108154297, 387.2887878417969, 549.317626953125, 2067.0751953125,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "54073",
                value: 21.0248279257,
                rank: 0,
              },
              {
                estimate: "54073",
                value: 41.0438252835,
                rank: 1,
              },
              {
                estimate: "54073",
                value: 137.1303096599,
                rank: 2,
              },
              {
                estimate: "54073",
                value: 60.9437494684,
                rank: 3,
              },
              {
                estimate: "54073",
                value: 472.5461656692,
                rank: 4,
              },
              {
                estimate: "54073",
                value: 112.2226785858,
                rank: 5,
              },
              {
                estimate: "54073",
                value: 394.8715691009,
                rank: 6,
              },
              {
                estimate: "54073",
                value: 252.6049030874,
                rank: 7,
              },
              {
                estimate: "54073",
                value: 67.8178023398,
                rank: 8,
              },
              {
                estimate: "54073",
                value: 176.6396683699,
                rank: 9,
              },
              {
                estimate: "54073",
                value: 204.9778794159,
                rank: 10,
              },
              {
                estimate: "54073",
                value: 23.4870097934,
                rank: 11,
              },
              {
                estimate: "54073",
                value: 72.9324152472,
                rank: 12,
              },
              {
                estimate: "54073",
                value: 2.0478456224,
                rank: 13,
              },
              {
                estimate: "54073",
                value: 9.355204468,
                rank: 14,
              },
              {
                estimate: "54073",
                value: 88.1416884724,
                rank: 15,
              },
              {
                estimate: "54073",
                value: 213.2710555531,
                rank: 16,
              },
              {
                estimate: "54073",
                value: -7.4412010479,
                rank: 17,
              },
              {
                estimate: "54073",
                value: 291.3510707833,
                rank: 18,
              },
              {
                estimate: "54073",
                value: 51.5662755349,
                rank: 19,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "54073",
              jsonValue: "21.0248279257",
            },
            {
              estimate: "54073",
              jsonValue: "41.0438252835",
            },
            {
              estimate: "54073",
              jsonValue: "137.1303096599",
            },
            {
              estimate: "54073",
              jsonValue: "60.9437494684",
            },
            {
              estimate: "54073",
              jsonValue: "472.5461656692",
            },
            {
              estimate: "54073",
              jsonValue: "112.2226785858",
            },
            {
              estimate: "54073",
              jsonValue: "394.8715691009",
            },
            {
              estimate: "54073",
              jsonValue: "252.6049030874",
            },
            {
              estimate: "54073",
              jsonValue: "67.8178023398",
            },
            {
              estimate: "54073",
              jsonValue: "176.6396683699",
            },
            {
              estimate: "54073",
              jsonValue: "204.9778794159",
            },
            {
              estimate: "54073",
              jsonValue: "23.4870097934",
            },
            {
              estimate: "54073",
              jsonValue: "72.9324152472",
            },
            {
              estimate: "54073",
              jsonValue: "2.0478456224",
            },
            {
              estimate: "54073",
              jsonValue: "9.355204468",
            },
            {
              estimate: "54073",
              jsonValue: "88.1416884724",
            },
            {
              estimate: "54073",
              jsonValue: "213.2710555531",
            },
            {
              estimate: "54073",
              jsonValue: "-7.4412010479",
            },
            {
              estimate: "54073",
              jsonValue: "291.3510707833",
            },
            {
              estimate: "54073",
              jsonValue: "51.5662755349",
            },
          ],
        },
        uniqueCount: {
          estimate: 2181147.443024018,
          upper: 2217033.820112752,
          lower: 2146469.7664933465,
        },
      },
      mths_since_last_major_derog: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "635709",
            NULL: "1664264",
          },
        },
        numberSummary: {
          count: "635709",
          min: -283.0216270793266,
          max: 580.5712863300928,
          mean: 43.93971303944628,
          stddev: 53.7298667620909,
          histogram: {
            start: -283.0216369628906,
            end: 580.5713471196289,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "280",
              "3106",
              "6656",
              "24577",
              "99329",
              "179713",
              "129024",
              "85504",
              "48128",
              "29696",
              "14848",
              "9216",
              "3584",
              "0",
              "0",
              "1024",
              "1024",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 580.5712890625,
            min: -283.0216369628906,
            bins: [
              -283.0216369628906, -254.23520416013997, -225.44877135738932, -196.66233855463867, -167.875905751888,
              -139.0894729491374, -110.30304014638673, -81.51660734363608, -52.73017454088543, -23.943741738134804,
              4.842691064615849, 33.6291238673665, 62.415556670117155, 91.20198947286781, 119.98842227561846,
              148.77485507836911, 177.56128788111977, 206.34772068387042, 235.13415348662102, 263.9205862893717,
              292.7070190921223, 321.49345189487303, 350.27988469762363, 379.06631750037434, 407.85275030312494,
              436.63918310587565, 465.42561590862624, 494.21204871137695, 522.9984815141275, 551.7849143168783,
              580.5713471196289,
            ],
            n: "635709",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 619032.7675404275,
            upper: 628825.1130820162,
            lower: 609391.7298084996,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -283.0216369628906, -70.68348693847656, -26.225337982177734, 8.33553695678711, 34.45321273803711,
              72.17816925048828, 146.94993591308594, 205.43638610839844, 580.5712890625,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "22406",
                value: 182.8875848228,
                rank: 0,
              },
              {
                estimate: "22406",
                value: 9.4028708485,
                rank: 1,
              },
              {
                estimate: "22406",
                value: 90.8113971015,
                rank: 2,
              },
              {
                estimate: "22406",
                value: 109.542279007,
                rank: 3,
              },
              {
                estimate: "22406",
                value: 5.2266188513,
                rank: 4,
              },
              {
                estimate: "22406",
                value: 29.5518365286,
                rank: 5,
              },
              {
                estimate: "22406",
                value: 74.5936483652,
                rank: 6,
              },
              {
                estimate: "22406",
                value: -26.0500151083,
                rank: 7,
              },
              {
                estimate: "22406",
                value: 63.8220157692,
                rank: 8,
              },
              {
                estimate: "22406",
                value: 4.5941252185,
                rank: 9,
              },
              {
                estimate: "22406",
                value: 80.4698472614,
                rank: 10,
              },
              {
                estimate: "22406",
                value: 273.9924168886,
                rank: 11,
              },
              {
                estimate: "22406",
                value: 60.8175485382,
                rank: 12,
              },
              {
                estimate: "22406",
                value: 25.4173683396,
                rank: 13,
              },
              {
                estimate: "22406",
                value: 21.5364929593,
                rank: 14,
              },
              {
                estimate: "22406",
                value: 10.0621993603,
                rank: 15,
              },
              {
                estimate: "22406",
                value: 68.3708992475,
                rank: 16,
              },
              {
                estimate: "22406",
                value: -20.8735846817,
                rank: 17,
              },
              {
                estimate: "22406",
                value: 80.100106893,
                rank: 18,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "22406",
              jsonValue: "182.8875848228",
            },
            {
              estimate: "22406",
              jsonValue: "9.4028708485",
            },
            {
              estimate: "22406",
              jsonValue: "90.8113971015",
            },
            {
              estimate: "22406",
              jsonValue: "109.542279007",
            },
            {
              estimate: "22406",
              jsonValue: "5.2266188513",
            },
            {
              estimate: "22406",
              jsonValue: "29.5518365286",
            },
            {
              estimate: "22406",
              jsonValue: "74.5936483652",
            },
            {
              estimate: "22406",
              jsonValue: "-26.0500151083",
            },
            {
              estimate: "22406",
              jsonValue: "63.8220157692",
            },
            {
              estimate: "22406",
              jsonValue: "4.5941252185",
            },
            {
              estimate: "22406",
              jsonValue: "80.4698472614",
            },
            {
              estimate: "22406",
              jsonValue: "273.9924168886",
            },
            {
              estimate: "22406",
              jsonValue: "60.8175485382",
            },
            {
              estimate: "22406",
              jsonValue: "25.4173683396",
            },
            {
              estimate: "22406",
              jsonValue: "21.5364929593",
            },
            {
              estimate: "22406",
              jsonValue: "10.0621993603",
            },
            {
              estimate: "22406",
              jsonValue: "68.3708992475",
            },
            {
              estimate: "22406",
              jsonValue: "-20.8735846817",
            },
            {
              estimate: "22406",
              jsonValue: "80.100106893",
            },
          ],
        },
        uniqueCount: {
          estimate: 602781.7216336086,
          upper: 612699.2777501951,
          lower: 593198.2019003297,
        },
      },
      mths_since_recent_inq: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2065372",
            NULL: "234601",
          },
        },
        numberSummary: {
          count: "2065372",
          min: -76.88768925428842,
          max: 152.2078410712209,
          mean: 6.818304055828532,
          stddev: 10.781283832273445,
          histogram: {
            start: -76.88768768310547,
            end: 152.20785518660432,
            counts: [
              "0",
              "64",
              "0",
              "0",
              "0",
              "2176",
              "1048",
              "11008",
              "29700",
              "192512",
              "1126400",
              "359424",
              "169984",
              "88064",
              "41984",
              "23552",
              "12288",
              "2048",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "1024",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 152.2078399658203,
            min: -76.88768768310547,
            bins: [
              -76.88768768310547, -69.25116958744847, -61.614651491791484, -53.97813339613449, -46.3416153004775,
              -38.705097204820504, -31.068579109163508, -23.43206101350652, -15.795542917849524, -8.159024822192535,
              -0.5225067265355392, 7.114011369121457, 14.750529464778452, 22.387047560435448, 30.02356565609243,
              37.660083751749426, 45.29660184740642, 52.93311994306342, 60.5696380387204, 68.20615613437741,
              75.84267423003439, 83.4791923256914, 91.11571042134838, 98.75222851700536, 106.38874661266237,
              114.02526470831936, 121.66178280397637, 129.29830089963335, 136.93481899529033, 144.57133709094734,
              152.20785518660432,
            ],
            n: "2065372",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 1824108.4266559712,
            upper: 1853026.0462164255,
            lower: 1795638.6212633285,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -76.88768768310547, -13.211658477783203, -3.6065773963928223, 0.20559249818325043, 3.4713263511657715,
              10.24567699432373, 28.016807556152344, 44.107547760009766, 152.2078399658203,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "192690",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "83712",
                value: 1.0,
                rank: 1,
              },
              {
                estimate: "83086",
                value: 3.0,
                rank: 2,
              },
              {
                estimate: "82202",
                value: 4.0,
                rank: 3,
              },
              {
                estimate: "81568",
                value: 2.0,
                rank: 4,
              },
              {
                estimate: "80603",
                value: 5.0,
                rank: 5,
              },
              {
                estimate: "79511",
                value: 8.0,
                rank: 6,
              },
              {
                estimate: "79346",
                value: 7.0,
                rank: 7,
              },
              {
                estimate: "78584",
                value: 10.0,
                rank: 8,
              },
              {
                estimate: "78369",
                value: 6.0,
                rank: 9,
              },
              {
                estimate: "78062",
                value: 9.0,
                rank: 10,
              },
              {
                estimate: "77646",
                value: 29.9022147128,
                rank: 11,
              },
              {
                estimate: "77646",
                value: 0.6967132361,
                rank: 12,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "192690",
              jsonValue: "0.0",
            },
            {
              estimate: "83712",
              jsonValue: "1.0",
            },
            {
              estimate: "83086",
              jsonValue: "3.0",
            },
            {
              estimate: "82202",
              jsonValue: "4.0",
            },
            {
              estimate: "81568",
              jsonValue: "2.0",
            },
            {
              estimate: "80603",
              jsonValue: "5.0",
            },
            {
              estimate: "79511",
              jsonValue: "8.0",
            },
            {
              estimate: "79346",
              jsonValue: "7.0",
            },
            {
              estimate: "78584",
              jsonValue: "10.0",
            },
            {
              estimate: "78369",
              jsonValue: "6.0",
            },
            {
              estimate: "78062",
              jsonValue: "9.0",
            },
            {
              estimate: "77646",
              jsonValue: "29.9022147128",
            },
            {
              estimate: "77646",
              jsonValue: "0.6967132361",
            },
          ],
        },
        uniqueCount: {
          estimate: 1802583.305420352,
          upper: 1832241.174006478,
          lower: 1773924.3530029536,
        },
      },
      emp_length: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2138066",
            NULL: "161907",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 11.0,
            upper: 11.0,
            lower: 11.0,
          },
          frequent: {
            items: [
              {
                value: "10+ years",
                estimate: 778402.0,
              },
              {
                value: "2 years",
                estimate: 207492.0,
              },
              {
                value: "3 years",
                estimate: 189962.0,
              },
              {
                value: "< 1 year",
                estimate: 167727.0,
              },
              {
                value: "1 year",
                estimate: 154143.0,
              },
              {
                value: "5 years",
                estimate: 137929.0,
              },
              {
                value: "4 years",
                estimate: 134496.0,
              },
              {
                value: "8 years",
                estimate: 108221.0,
              },
              {
                value: "6 years",
                estimate: 95279.0,
              },
              {
                value: "9 years",
                estimate: 88042.0,
              },
              {
                value: "7 years",
                estimate: 76373.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "778402",
              jsonValue: '"10+ years"',
            },
            {
              estimate: "207492",
              jsonValue: '"2 years"',
            },
            {
              estimate: "189962",
              jsonValue: '"3 years"',
            },
            {
              estimate: "167727",
              jsonValue: '"< 1 year"',
            },
            {
              estimate: "154143",
              jsonValue: '"1 year"',
            },
            {
              estimate: "137929",
              jsonValue: '"5 years"',
            },
            {
              estimate: "134496",
              jsonValue: '"4 years"',
            },
            {
              estimate: "108221",
              jsonValue: '"8 years"',
            },
            {
              estimate: "95279",
              jsonValue: '"6 years"',
            },
            {
              estimate: "88042",
              jsonValue: '"9 years"',
            },
            {
              estimate: "76373",
              jsonValue: '"7 years"',
            },
          ],
        },
        uniqueCount: {
          estimate: 11.000000273187965,
          upper: 11.000549494958403,
          lower: 11.0,
        },
      },
      application_type: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2299973",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 2.0,
            upper: 2.0,
            lower: 2.0,
          },
          frequent: {
            items: [
              {
                value: "Individual",
                estimate: 2246258.0,
              },
              {
                value: "Joint App",
                estimate: 53715.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "2246258",
              jsonValue: '"Individual"',
            },
            {
              estimate: "53715",
              jsonValue: '"Joint App"',
            },
          ],
        },
        uniqueCount: {
          estimate: 2.000000004967054,
          upper: 2.000099863468538,
          lower: 2.0,
        },
      },
      mo_sin_rcnt_tl: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -459.6938461702925,
          max: 964.1107707218908,
          mean: 7.832470436112699,
          stddev: 14.885161418917313,
          histogram: {
            start: -459.69384765625,
            end: 964.1108752196716,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "1088",
              "1921028",
              "356353",
              "13312",
              "4096",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 964.1107788085938,
            min: -459.69384765625,
            bins: [
              -459.69384765625, -412.23369022705265, -364.77353279785524, -317.31337536865783, -269.8532179394605,
              -222.3930605102631, -174.93290308106572, -127.47274565186831, -80.01258822267096, -32.552430793473604,
              14.907726635723805, 62.36788406492121, 109.82804149411857, 157.28819892331592, 204.74835635251338,
              252.20851378171074, 299.6686712109081, 347.12882864010544, 394.5889860693028, 442.04914349850026,
              489.5093009276976, 536.969458356895, 584.4296157860924, 631.8897732152898, 679.3499306444871,
              726.8100880736845, 774.2702455028818, 821.7304029320792, 869.1905603612768, 916.6507177904741,
              964.1108752196715,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2198175.231060657,
            upper: 2233029.5074888505,
            lower: 2163860.807158105,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -459.69384765625, -13.024307250976562, -3.6944615840911865, 1.0525503158569336, 4.246200084686279,
              10.364317893981934, 31.138330459594727, 62.12807083129883, 964.1107788085938,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "95919",
                value: 3.0,
                rank: 0,
              },
              {
                estimate: "93861",
                value: 4.0,
                rank: 1,
              },
              {
                estimate: "93376",
                value: 2.0,
                rank: 2,
              },
              {
                estimate: "91819",
                value: 5.0,
                rank: 3,
              },
              {
                estimate: "90106",
                value: 1.0,
                rank: 4,
              },
              {
                estimate: "90055",
                value: 7.0,
                rank: 5,
              },
              {
                estimate: "89555",
                value: 6.0,
                rank: 6,
              },
              {
                estimate: "89132",
                value: 8.0,
                rank: 7,
              },
              {
                estimate: "87495",
                value: 10.0,
                rank: 8,
              },
              {
                estimate: "87493",
                value: 9.0,
                rank: 9,
              },
              {
                estimate: "85989",
                value: 11.0,
                rank: 10,
              },
              {
                estimate: "85960",
                value: 4.5201700986999995,
                rank: 11,
              },
              {
                estimate: "85960",
                value: 0.1607916165,
                rank: 12,
              },
              {
                estimate: "85960",
                value: 17.00502477,
                rank: 13,
              },
              {
                estimate: "85960",
                value: 19.306807808,
                rank: 14,
              },
              {
                estimate: "85960",
                value: -7.6279299412,
                rank: 15,
              },
              {
                estimate: "85960",
                value: -0.8208727504000001,
                rank: 16,
              },
              {
                estimate: "85960",
                value: 2.8134458299,
                rank: 17,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "95919",
              jsonValue: "3.0",
            },
            {
              estimate: "93861",
              jsonValue: "4.0",
            },
            {
              estimate: "93376",
              jsonValue: "2.0",
            },
            {
              estimate: "91819",
              jsonValue: "5.0",
            },
            {
              estimate: "90106",
              jsonValue: "1.0",
            },
            {
              estimate: "90055",
              jsonValue: "7.0",
            },
            {
              estimate: "89555",
              jsonValue: "6.0",
            },
            {
              estimate: "89132",
              jsonValue: "8.0",
            },
            {
              estimate: "87495",
              jsonValue: "10.0",
            },
            {
              estimate: "87493",
              jsonValue: "9.0",
            },
            {
              estimate: "85989",
              jsonValue: "11.0",
            },
            {
              estimate: "85960",
              jsonValue: "4.5201700987",
            },
            {
              estimate: "85960",
              jsonValue: "0.1607916165",
            },
            {
              estimate: "85960",
              jsonValue: "17.00502477",
            },
            {
              estimate: "85960",
              jsonValue: "19.306807808",
            },
            {
              estimate: "85960",
              jsonValue: "-7.6279299412",
            },
            {
              estimate: "85960",
              jsonValue: "-0.8208727504",
            },
            {
              estimate: "85960",
              jsonValue: "2.8134458299",
            },
          ],
        },
        uniqueCount: {
          estimate: 2185183.155866645,
          upper: 2221135.932461453,
          lower: 2150441.3162529254,
        },
      },
      chargeoff_within_12_mths: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -8.13348879679001,
          max: 17.193181523234795,
          mean: 0.00988298593286254,
          stddev: 0.16254314042542745,
          histogram: {
            start: -8.133488655090332,
            end: 17.193183710895347,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "1088",
              "0",
              "2277381",
              "13312",
              "0",
              "0",
              "0",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "4096",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 17.19318199157715,
            min: -8.133488655090332,
            bins: [
              -8.133488655090332, -7.289266242890809, -6.4450438306912865, -5.600821418491764, -4.756599006292241,
              -3.912376594092719, -3.0681541818931963, -2.2239317696936736, -1.3797093574941508, -0.535486945294628,
              0.30873546690489384, 1.1529578791044166, 1.9971802913039394, 2.841402703503462, 3.685625115702985,
              4.529847527902508, 5.37406994010203, 6.218292352301553, 7.062514764501076, 7.906737176700599,
              8.75095958890012, 9.595182001099644, 10.439404413299165, 11.28362682549869, 12.12784923769821,
              12.972071649897735, 13.816294062097256, 14.66051647429678, 15.504738886496302, 16.348961298695826,
              17.193183710895347,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 19973.370794019753,
            upper: 20256.52072776298,
            lower: 19694.131136732707,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [-8.133488655090332, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 17.19318199157715],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "2279569",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "1797",
                value: 1.0,
                rank: 1,
              },
            ],
            longs: [],
          },
          isDiscrete: true,
        },
        frequentItems: {
          items: [
            {
              estimate: "2279569",
              jsonValue: "0.0",
            },
            {
              estimate: "1797",
              jsonValue: "1.0",
            },
          ],
        },
        uniqueCount: {
          estimate: 19374.837166862286,
          upper: 19693.610991541856,
          lower: 19066.800065447736,
        },
      },
      last_fico_range_high: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -2968.259712562144,
          max: 4733.85957123104,
          mean: 678.6764928189132,
          stddev: 686.7258275143556,
          histogram: {
            start: -2968.259765625,
            end: 4733.859848385938,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "4096",
              "64",
              "0",
              "19456",
              "26624",
              "73731",
              "133121",
              "203776",
              "267264",
              "344064",
              "385024",
              "289792",
              "220160",
              "146432",
              "95232",
              "47105",
              "26624",
              "8192",
              "9216",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 4733.859375,
            min: -2968.259765625,
            bins: [
              -2968.259765625, -2711.522445157969, -2454.7851246909377, -2198.047804223906, -1941.3104837568749,
              -1684.5731632898437, -1427.8358428228123, -1171.098522355781, -914.3612018887497, -657.6238814217186,
              -400.8865609546874, -144.1492404876558, 112.58807997937538, 369.32540044640655, 626.0627209134382,
              882.8000413804693, 1139.5373618475005, 1396.2746823145317, 1653.0120027815628, 1909.749323248594,
              2166.486643715625, 2423.2239641826573, 2679.9612846496884, 2936.6986051167196, 3193.4359255837508,
              3450.173246050782, 3706.910566517813, 3963.6478869848443, 4220.385207451876, 4477.1225279189075,
              4733.859848385939,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2168590.4699046398,
            upper: 2202975.219074547,
            lower: 2134738.293991539,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -2968.259765625, -962.062255859375, -432.2259826660156, 231.04266357421875, 678.00927734375,
              1119.9190673828125, 1832.4364013671875, 2330.02392578125, 4733.859375,
            ],
          },
          isDiscrete: false,
        },
        uniqueCount: {
          estimate: 2237408.369337746,
          upper: 2274220.406369146,
          lower: 2201836.210313347,
        },
      },
      num_tl_120dpd_2m: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2180283",
            NULL: "119690",
          },
        },
        numberSummary: {
          count: "2180283",
          min: -2.42552806190773,
          max: 7.079678690602373,
          mean: 0.0011548661657314902,
          stddev: 0.049861793893676114,
          histogram: {
            start: -2.425528049468994,
            end: 7.079679243429279,
            counts: [
              "0",
              "0",
              "0",
              "0",
              "128",
              "0",
              "0",
              "2180155",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 7.079678535461426,
            min: -2.425528049468994,
            bins: [
              -2.425528049468994, -2.108687806372385, -1.791847563275776, -1.4750073201791667, -1.1581670770825576,
              -0.8413268339859485, -0.5244865908893392, -0.20764634779273017, 0.10919389530387891, 0.426034138400488,
              0.7428743814970971, 1.0597146245937061, 1.3765548676903157, 1.6933951107869243, 2.010235353883534,
              2.3270755969801424, 2.643915840076752, 2.9607560831733615, 3.27759632626997, 3.5944365693665796,
              3.9112768124631883, 4.228117055559798, 4.544957298656406, 4.861797541753016, 5.1786377848496254,
              5.495478027946234, 5.812318271042843, 6.129158514139453, 6.445998757236062, 6.76283900033267,
              7.079679243429279,
            ],
            n: "2180283",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2488.0,
            upper: 2488.0,
            lower: 2488.0,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [-2.425528049468994, 0.0, -0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 7.079678535461426],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "2177752",
                value: 0.0,
                rank: 0,
              },
              {
                estimate: "142",
                value: 1.0,
                rank: 1,
              },
              {
                estimate: "104",
                value: 1.7777172113000002,
                rank: 2,
              },
              {
                estimate: "104",
                value: 3.9180802726,
                rank: 3,
              },
              {
                estimate: "104",
                value: -0.068082371,
                rank: 4,
              },
              {
                estimate: "104",
                value: 3.2335764635,
                rank: 5,
              },
              {
                estimate: "104",
                value: 2.7480014899,
                rank: 6,
              },
              {
                estimate: "104",
                value: 1.3741775221,
                rank: 7,
              },
              {
                estimate: "104",
                value: 1.1186224053,
                rank: 8,
              },
              {
                estimate: "104",
                value: 0.4037785471,
                rank: 9,
              },
              {
                estimate: "104",
                value: 1.4350246369000002,
                rank: 10,
              },
              {
                estimate: "104",
                value: 1.2655742857,
                rank: 11,
              },
              {
                estimate: "104",
                value: 4.9318362541,
                rank: 12,
              },
              {
                estimate: "104",
                value: 0.2070561621,
                rank: 13,
              },
              {
                estimate: "104",
                value: -0.4534457475,
                rank: 14,
              },
              {
                estimate: "104",
                value: -1.4829319839,
                rank: 15,
              },
              {
                estimate: "104",
                value: 1.9583626995,
                rank: 16,
              },
              {
                estimate: "104",
                value: -0.6402745335000001,
                rank: 17,
              },
              {
                estimate: "104",
                value: 2.6141385339000003,
                rank: 18,
              },
              {
                estimate: "104",
                value: 1.7052246381,
                rank: 19,
              },
              {
                estimate: "104",
                value: 0.6259352398,
                rank: 20,
              },
              {
                estimate: "104",
                value: -2.4255280619,
                rank: 21,
              },
            ],
            longs: [],
          },
          isDiscrete: true,
        },
        frequentItems: {
          items: [
            {
              estimate: "2177752",
              jsonValue: "0.0",
            },
            {
              estimate: "142",
              jsonValue: "1.0",
            },
            {
              estimate: "104",
              jsonValue: "1.7777172113",
            },
            {
              estimate: "104",
              jsonValue: "3.9180802726",
            },
            {
              estimate: "104",
              jsonValue: "-0.068082371",
            },
            {
              estimate: "104",
              jsonValue: "3.2335764635",
            },
            {
              estimate: "104",
              jsonValue: "2.7480014899",
            },
            {
              estimate: "104",
              jsonValue: "1.3741775221",
            },
            {
              estimate: "104",
              jsonValue: "1.1186224053",
            },
            {
              estimate: "104",
              jsonValue: "0.4037785471",
            },
            {
              estimate: "104",
              jsonValue: "1.4350246369",
            },
            {
              estimate: "104",
              jsonValue: "1.2655742857",
            },
            {
              estimate: "104",
              jsonValue: "4.9318362541",
            },
            {
              estimate: "104",
              jsonValue: "0.2070561621",
            },
            {
              estimate: "104",
              jsonValue: "-0.4534457475",
            },
            {
              estimate: "104",
              jsonValue: "-1.4829319839",
            },
            {
              estimate: "104",
              jsonValue: "1.9583626995",
            },
            {
              estimate: "104",
              jsonValue: "-0.6402745335",
            },
            {
              estimate: "104",
              jsonValue: "2.6141385339",
            },
            {
              estimate: "104",
              jsonValue: "1.7052246381",
            },
            {
              estimate: "104",
              jsonValue: "0.6259352398",
            },
            {
              estimate: "104",
              jsonValue: "-2.4255280619",
            },
          ],
        },
        uniqueCount: {
          estimate: 2497.470961126505,
          upper: 2530.161487559386,
          lower: 2465.575650718175,
        },
      },
      policy_code: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "FRACTIONAL",
            ratio: 1.0,
          },
          typeCounts: {
            FRACTIONAL: "2299973",
          },
        },
        numberSummary: {
          count: "2299973",
          min: -4.136859760299243,
          max: 6.276281935787333,
          mean: 1.0003861768663325,
          stddev: 0.9963633824279016,
          histogram: {
            start: -4.136859893798828,
            end: 6.276282461276865,
            counts: [
              "0",
              "0",
              "64",
              "0",
              "0",
              "2048",
              "6144",
              "9217",
              "38912",
              "55298",
              "99328",
              "164864",
              "215040",
              "268288",
              "392192",
              "297984",
              "250881",
              "193536",
              "138240",
              "82944",
              "43008",
              "21505",
              "12288",
              "8192",
              "0",
              "0",
              "0",
              "0",
              "0",
              "0",
            ],
            max: 6.276281833648682,
            min: -4.136859893798828,
            bins: [
              -4.136859893798828, -3.7897551486296384, -3.4426504034604486, -3.0955456582912593, -2.748440913122069,
              -2.4013361679528797, -2.05423142278369, -1.7071266776145002, -1.3600219324453104, -1.0129171872761207,
              -0.6658124421069309, -0.31870769693774115, 0.028397048231448174, 0.3755017934006384, 0.7226065385698277,
              1.069711283739018, 1.4168160289082072, 1.7639207740773966, 2.1110255192465868, 2.458130264415776,
              2.8052350095849663, 3.1523397547541556, 3.499444499923346, 3.846549245092535, 4.1936539902617245,
              4.540758735430915, 4.887863480600105, 5.234968225769293, 5.5820729709384835, 5.929177716107674,
              6.276282461276864,
            ],
            n: "2299973",
            width: 0.0,
          },
          uniqueCount: {
            estimate: 2188481.0846174406,
            upper: 2223181.509339381,
            lower: 2154318.1271966095,
          },
          quantiles: {
            quantiles: [0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0],
            quantileValues: [
              -4.136859893798828, -1.2554171085357666, -0.6517077088356018, 0.3537181615829468, 1.0, 1.6546212434768677,
              2.6689982414245605, 3.366626262664795, 6.276281833648682,
            ],
          },
          frequentNumbers: {
            doubles: [
              {
                estimate: "175416",
                value: 1.0,
                rank: 0,
              },
              {
                estimate: "83583",
                value: 0.6984750503,
                rank: 1,
              },
              {
                estimate: "83583",
                value: 1.7954325737999999,
                rank: 2,
              },
              {
                estimate: "83583",
                value: 3.1953765911,
                rank: 3,
              },
              {
                estimate: "83583",
                value: 1.2372710907,
                rank: 4,
              },
              {
                estimate: "83583",
                value: 0.5682383181,
                rank: 5,
              },
              {
                estimate: "83583",
                value: 1.3856661733,
                rank: 6,
              },
              {
                estimate: "83583",
                value: 0.9372081853,
                rank: 7,
              },
              {
                estimate: "83583",
                value: 0.1646953403,
                rank: 8,
              },
              {
                estimate: "83583",
                value: 0.5684056718,
                rank: 9,
              },
              {
                estimate: "83583",
                value: 1.7174235511,
                rank: 10,
              },
              {
                estimate: "83583",
                value: 2.5782259054,
                rank: 11,
              },
              {
                estimate: "83583",
                value: 0.9059142125,
                rank: 12,
              },
              {
                estimate: "83583",
                value: 1.2932711475,
                rank: 13,
              },
              {
                estimate: "83583",
                value: -1.426457169,
                rank: 14,
              },
            ],
            longs: [],
          },
          isDiscrete: false,
        },
        frequentItems: {
          items: [
            {
              estimate: "175416",
              jsonValue: "1.0",
            },
            {
              estimate: "83583",
              jsonValue: "0.6984750503",
            },
            {
              estimate: "83583",
              jsonValue: "1.7954325738",
            },
            {
              estimate: "83583",
              jsonValue: "3.1953765911",
            },
            {
              estimate: "83583",
              jsonValue: "1.2372710907",
            },
            {
              estimate: "83583",
              jsonValue: "0.5682383181",
            },
            {
              estimate: "83583",
              jsonValue: "1.3856661733",
            },
            {
              estimate: "83583",
              jsonValue: "0.9372081853",
            },
            {
              estimate: "83583",
              jsonValue: "0.1646953403",
            },
            {
              estimate: "83583",
              jsonValue: "0.5684056718",
            },
            {
              estimate: "83583",
              jsonValue: "1.7174235511",
            },
            {
              estimate: "83583",
              jsonValue: "2.5782259054",
            },
            {
              estimate: "83583",
              jsonValue: "0.9059142125",
            },
            {
              estimate: "83583",
              jsonValue: "1.2932711475",
            },
            {
              estimate: "83583",
              jsonValue: "-1.426457169",
            },
          ],
        },
        uniqueCount: {
          estimate: 2170761.045039168,
          upper: 2206476.5349208633,
          lower: 2136248.5000087945,
        },
      },
      debt_settlement_flag: {
        counters: {
          count: "2299973",
        },
        schema: {
          inferredType: {
            type: "STRING",
            ratio: 1.0,
          },
          typeCounts: {
            STRING: "2299973",
          },
        },
        stringSummary: {
          uniqueCount: {
            estimate: 2.0,
            upper: 2.0,
            lower: 2.0,
          },
          frequent: {
            items: [
              {
                value: "N",
                estimate: 2232303.0,
              },
              {
                value: "Y",
                estimate: 67670.0,
              },
            ],
          },
        },
        frequentItems: {
          items: [
            {
              estimate: "2232303",
              jsonValue: '"N"',
            },
            {
              estimate: "67670",
              jsonValue: '"Y"',
            },
          ],
        },
        uniqueCount: {
          estimate: 2.000000004967054,
          upper: 2.000099863468538,
          lower: 2.0,
        },
      },
    },
  };
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
