import math

import matplotlib.dates as _dates
import matplotlib.pyplot as _plt
import matplotlib.ticker as _ticker
import numpy as np
import pandas as pd
from mpl_toolkits.axes_grid1 import ImageGrid

from whylogs.viz import BaseProfileVisualizer


class MatplotlibProfileVisualizer(BaseProfileVisualizer):
    def __init__(self):
        self.summary_data = None
        self.hist_data = None
        self.dist_data = None
        self.theme = {
            "colors": ["#005566", "#2683C9", "#44C0E7", "#F07028", "#FFDE1E"],
            "fill_colors": ["#D2F9FF", "#7AC0CB"],
            "font_color": "#4F595B",
        }
        super().__init__(framework="matplotlib", visualizer=self)

        self._init_theming()

    def available_plots(self):
        """Returns available plots for matplotlib framework."""
        print(
            """
Available plots for whylogs visualizations using matplotlib:
plot_data_types()
plot_distribution()
plot_missing_values()
plot_uniqueness()
plot_string()
plot_string_length()
"""
        )

    def _init_data_preprocessing(self, profiles):
        filtered_data = []
        for prof in profiles:
            df_flat = prof.flat_summary()["summary"]
            if len(df_flat) <= 0:
                continue
            df_flat.loc[:, "date"] = prof.dataset_timestamp

            filtered_data.append(df_flat)
        self.profiles = profiles
        self.summary_data = pd.concat(filtered_data).sort_values(by=["date"])

    def _init_theming(self):
        _plt.style.use("seaborn-whitegrid")
        _plt.rcParams["font.family"] = "sans-serif"
        _plt.rcParams["font.sans-serif"] = ["Asap", "Verdana"]
        _plt.rcParams["font.size"] = 10
        _plt.rcParams["figure.dpi"] = 200
        _plt.rcParams["savefig.dpi"] = 200
        _plt.rcParams["text.color"] = self.theme["font_color"]
        _plt.rcParams["axes.labelcolor"] = self.theme["font_color"]
        _plt.rcParams["xtick.color"] = self.theme["font_color"]
        _plt.rcParams["ytick.color"] = self.theme["font_color"]
        _plt.rcParams["axes.prop_cycle"] = _plt.cycler(color=self.theme["colors"])

    @staticmethod
    def _chart_theming():
        """Applies theming needed for each chart."""
        _plt.ioff()
        fig = _plt.figure(figsize=(10, 2))
        ax = _plt.axes()
        fig.text(
            1.0,
            1.06,
            "Made with whylogs",
            horizontalalignment="right",
            verticalalignment="center",
            transform=ax.transAxes,
            fontsize=10,
        )
        ax.xaxis.set_label_text("")

        return fig, ax

    def _prof_data(self, variable):
        filtered_data = []
        for prof in self.profiles:

            df = pd.DataFrame.from_records([{"date": prof.dataset_timestamp, "profile": prof}])
            if len(df) <= 0:
                continue

            filtered_data.append(df)

        prof_data = pd.concat(filtered_data).sort_values(by=["date"])
        return prof_data

    def _summary_data_preprocessing(self, variable):
        """Applies general data preprocessing for each chart."""
        proc_data = self.summary_data[self.summary_data["column"] == variable]
        proc_data.dropna(axis=0, subset=["date"])

        return proc_data

    def _confirm_profile_data(self):
        """Checks for that profiles and profile data already set."""
        if self.summary_data is not None and len(self.summary_data) > 0:
            return True

        print("Profiles have not been set for visualizer. " "Try ProfileVisualizer.set_profiles(...).")
        return False

    def plot_token_length(self, variable, ts_format="%d-%b-%y", **kwargs):

        fig, ax = MatplotlibProfileVisualizer._chart_theming()
        chart_data = self._prof_data(variable)
        chart_data["token_length_quantile_0.05"] = chart_data["profile"].apply(
            lambda x: x.columns[variable].string_tracker.token_length.histogram.get_quantiles([0.05])[0]
        )
        chart_data["token_length_quantile_0.25"] = chart_data["profile"].apply(
            lambda x: x.columns[variable].string_tracker.token_length.histogram.get_quantiles([0.25])[0]
        )
        chart_data["token_length_quantile_0.5"] = chart_data["profile"].apply(
            lambda x: x.columns[variable].string_tracker.token_length.histogram.get_quantiles([0.5])[0]
        )
        chart_data["token_length_quantile_0.75"] = chart_data["profile"].apply(
            lambda x: x.columns[variable].string_tracker.token_length.histogram.get_quantiles([0.75])[0]
        )
        chart_data["token_length_quantile_0.95"] = chart_data["profile"].apply(
            lambda x: x.columns[variable].string_tracker.token_length.histogram.get_quantiles([0.95])[0]
        )
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, "token_length_quantile_0.5"],
            color=self.theme["colors"][0],
            linewidth=1.5,
            label="50%",
        )

        # Lines bordering the fill area
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, "token_length_quantile_0.05"],
            color=self.theme["fill_colors"][0],
            linewidth=0.5,
        )
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, "token_length_quantile_0.95"],
            color=self.theme["fill_colors"][0],
            linewidth=0.5,
        )
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, "token_length_quantile_0.25"],
            color=self.theme["fill_colors"][1],
            linewidth=0.5,
        )
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, "token_length_quantile_0.75"],
            color=self.theme["fill_colors"][1],
            linewidth=0.5,
        )

        # Fill areas
        ax.fill_between(
            pd.to_datetime(chart_data.loc[:, "date"]),
            y1=chart_data.loc[:, "token_length_quantile_0.05"],
            y2=chart_data.loc[:, "token_length_quantile_0.95"],
            alpha=0.5,
            color=self.theme["fill_colors"][0],
            label="5-95%",
        )
        ax.fill_between(
            pd.to_datetime(chart_data.loc[:, "date"]),
            y1=chart_data.loc[:, "token_length_quantile_0.25"],
            y2=chart_data.loc[:, "token_length_quantile_0.75"],
            alpha=0.5,
            color=self.theme["fill_colors"][1],
            label="25-75%",
        )

        ax.yaxis.set_label_text(variable + " Token Length", fontweight="bold")
        ax.set_title(
            f"Token Length Distribution - Estimated Quantiles ({variable})",
            loc="left",
            fontweight="bold",
        )
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(
            handles,
            labels,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.1),
            frameon=False,
            ncol=3,
        )
        ax.xaxis.set_major_formatter(_dates.DateFormatter(ts_format))
        ax.yaxis.set_major_formatter(_ticker.ScalarFormatter(useOffset=False, useMathText=False, useLocale=None))

        return fig

    def plot_char_pos(self, variable, character_list=None, ts_format="%d-%b-%y", **kwargs):

        chart_data = self._prof_data(variable)

        _plt.ioff()
        fig = _plt.figure(figsize=(10.0, 4.0))

        max_length = max(chart_data["profile"].apply(lambda x: x.columns[variable].string_tracker.length.histogram.get_max_value()).tolist())

        matrixes = []
        set_character_list = set()
        if character_list is None:

            character_list = list(chart_data["profile"].loc[0].columns[variable].string_tracker.char_pos_tracker.character_list)

        bins = list(range(1, int(max_length + 1)))
        for prof in self.profiles:
            mycounts = prof.columns[variable].string_tracker.char_pos_tracker.char_pos_map

            max_length = max([val.histogram.get_max_value() for key, val in mycounts.items()])
            char_histos = {key: np.array(val.histogram.get_pmf(bins[:-1])) for key, val in mycounts.items()}

            _, matrx = array_creation(char_histos, bins, character_list)
            set_character_list = set.union(set_character_list, set(character_list))
            matrixes.append(matrx)

        # set_character_list = list(set_character_list)

        grid = ImageGrid(
            fig,
            111,  # similar to subplot(111)
            nrows_ncols=(math.ceil(len(self.profiles) / 7), 7),  # creates nx7 grid of axes
            axes_pad=0.3,  # pad between axes in inch.
        )
        fig.text(
            1.0,
            0.96,
            "Made with whylogs",
            horizontalalignment="right",
            verticalalignment="center",
            fontsize=10,
        )
        for idx, (ax, mat) in enumerate(zip(grid, matrixes)):
            # Iterating over the grid returns the Axes.

            ax.imshow(mat, cmap="Blues", vmin=0, vmax=1, **kwargs)

            ax.set_xlabel("Position in String")
            ax.set_ylabel("Character")
            ax.set_xticks(range(len(bins)), bins)
            ax.set_yticks(range(len(character_list)))
            ax.set_yticklabels(character_list)

            ax.grid(False)
            if self.profiles[idx]:
                ax.set_title(f"{self.profiles[idx].dataset_timestamp.strftime(ts_format)}")
        fig.suptitle(f"Character Position Distribution ({variable})", fontweight="bold")
        return fig

    def plot_string_length(self, variable, ts_format="%d-%b-%y", **kwargs):
        fig, ax = MatplotlibProfileVisualizer._chart_theming()

        chart_data = self._prof_data(variable)

        chart_data["length_quantile_0.05"] = chart_data["profile"].apply(lambda x: x.columns[variable].string_tracker.length.histogram.get_quantiles([0.05])[0])
        chart_data["length_quantile_0.25"] = chart_data["profile"].apply(lambda x: x.columns[variable].string_tracker.length.histogram.get_quantiles([0.25])[0])
        chart_data["length_quantile_0.5"] = chart_data["profile"].apply(lambda x: x.columns[variable].string_tracker.length.histogram.get_quantiles([0.5])[0])
        chart_data["length_quantile_0.75"] = chart_data["profile"].apply(lambda x: x.columns[variable].string_tracker.length.histogram.get_quantiles([0.75])[0])
        chart_data["length_quantile_0.95"] = chart_data["profile"].apply(lambda x: x.columns[variable].string_tracker.length.histogram.get_quantiles([0.95])[0])
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, "length_quantile_0.5"],
            color=self.theme["colors"][0],
            linewidth=1.5,
            label="50%",
        )

        # Lines bordering the fill area
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, "length_quantile_0.05"],
            color=self.theme["fill_colors"][0],
            linewidth=0.5,
        )
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, "length_quantile_0.95"],
            color=self.theme["fill_colors"][0],
            linewidth=0.5,
        )
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, "length_quantile_0.25"],
            color=self.theme["fill_colors"][1],
            linewidth=0.5,
        )
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, "length_quantile_0.75"],
            color=self.theme["fill_colors"][1],
            linewidth=0.5,
        )

        # Fill areas
        ax.fill_between(
            pd.to_datetime(chart_data.loc[:, "date"]),
            y1=chart_data.loc[:, "length_quantile_0.05"],
            y2=chart_data.loc[:, "length_quantile_0.95"],
            alpha=0.5,
            color=self.theme["fill_colors"][0],
            label="5-95%",
        )
        ax.fill_between(
            pd.to_datetime(chart_data.loc[:, "date"]),
            y1=chart_data.loc[:, "length_quantile_0.25"],
            y2=chart_data.loc[:, "length_quantile_0.75"],
            alpha=0.5,
            color=self.theme["fill_colors"][1],
            label="25-75%",
        )

        ax.yaxis.set_label_text(variable + " string Length", fontweight="bold")
        ax.set_title(
            f"String Length Distribution - Estimated Quantiles ({variable})",
            loc="left",
            fontweight="bold",
        )
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(
            handles,
            labels,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.1),
            frameon=False,
            ncol=3,
        )
        ax.xaxis.set_major_formatter(_dates.DateFormatter(ts_format))
        ax.yaxis.set_major_formatter(_ticker.ScalarFormatter(useOffset=False, useMathText=False, useLocale=None))
        return fig

    def plot_string(self, variable, character_list, ts_format="%d-%b-%y", **kwargs):

        token_length_fig = self.plot_token_length(variable, ts_format, **kwargs)
        length_fig = self.plot_string_length(variable, ts_format=ts_format, **kwargs)
        char_pos_plots_fig = self.plot_char_pos(variable, character_list=character_list, ts_format=ts_format, **kwargs)
        return length_fig, token_length_fig, char_pos_plots_fig

    def plot_distribution(self, variable, ts_format="%d-%b-%y", **kwargs):
        """Plots a distribution chart."""

        if not self._confirm_profile_data:
            return

        chart_data = self._summary_data_preprocessing(variable)

        fig, ax = MatplotlibProfileVisualizer._chart_theming()

        # Primary line
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, "quantile_0.5000"],
            color=self.theme["colors"][0],
            linewidth=1.5,
            label="50%",
        )

        # Lines bordering the fill area
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, "quantile_0.0500"],
            color=self.theme["fill_colors"][0],
            linewidth=0.5,
        )
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, "quantile_0.9500"],
            color=self.theme["fill_colors"][0],
            linewidth=0.5,
        )
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, "quantile_0.2500"],
            color=self.theme["fill_colors"][1],
            linewidth=0.5,
        )
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, "quantile_0.7500"],
            color=self.theme["fill_colors"][1],
            linewidth=0.5,
        )

        # Fill areas
        ax.fill_between(
            pd.to_datetime(chart_data.loc[:, "date"]),
            y1=chart_data.loc[:, "quantile_0.0500"],
            y2=chart_data.loc[:, "quantile_0.9500"],
            alpha=0.5,
            color=self.theme["fill_colors"][0],
            label="5-95%",
        )
        ax.fill_between(
            pd.to_datetime(chart_data.loc[:, "date"]),
            y1=chart_data.loc[:, "quantile_0.2500"],
            y2=chart_data.loc[:, "quantile_0.7500"],
            alpha=0.5,
            color=self.theme["fill_colors"][1],
            label="25-75%",
        )

        ax.yaxis.set_label_text(variable + " Range", fontweight="bold")
        ax.set_title(
            f"Distribution - Estimated Quantiles ({variable})",
            loc="left",
            fontweight="bold",
        )
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(
            handles,
            labels,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.1),
            frameon=False,
            ncol=3,
        )
        ax.xaxis.set_major_formatter(_dates.DateFormatter(ts_format))
        ax.yaxis.set_major_formatter(_ticker.ScalarFormatter(useOffset=False, useMathText=False, useLocale=None))

        return fig

    def plot_missing_values(self, variable, ts_format="%d-%b-%y", **kwargs):
        """Plots a Missing Value to Total Count ratio chart."""
        if not self._confirm_profile_data:
            return

        chart_data = self._summary_data_preprocessing(variable)
        chart_data.loc[:, "mv_ratio"] = chart_data.loc[:, "type_null_count"] / chart_data.loc[:, "count"]

        fig, ax = MatplotlibProfileVisualizer._chart_theming()

        # Primary line
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, "mv_ratio"],
            color=self.theme["colors"][0],
            linewidth=1.5,
            label="Missing Value Ratio",
        )

        ax.yaxis.set_label_text("Missing Value to\nTotal Count Ratio", fontweight="bold")
        ax.set_title(f"Missing Values ({variable})", loc="left", fontweight="bold")
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(
            handles,
            labels,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.1),
            frameon=False,
            ncol=3,
        )
        ax.xaxis.set_major_formatter(_dates.DateFormatter(ts_format))
        ax.yaxis.set_major_formatter(_ticker.ScalarFormatter(useOffset=False, useMathText=False, useLocale=None))

        return fig

    def plot_uniqueness(self, variable, ts_format="%d-%b-%y", **kwargs):
        """Plots a Estimated Unique Values chart."""
        if not self._confirm_profile_data:
            return

        chart_data = self._summary_data_preprocessing(variable)

        if chart_data.loc[:, "nunique_numbers"].sum() == 0 and chart_data.loc[:, "nunique_str"].sum() == 0:
            print("No data appropriate for uniqueness plot.")
            return
        elif chart_data.loc[:, "nunique_numbers"].sum() > chart_data.loc[:, "nunique_str"].sum():
            metrics = [
                "nunique_numbers",
                "nunique_numbers_lower",
                "nunique_numbers_upper",
            ]
        else:
            metrics = ["nunique_str", "nunique_str_lower", "nunique_str_upper"]

        fig, ax = MatplotlibProfileVisualizer._chart_theming()

        # Primary line
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, metrics[0]],
            color=self.theme["colors"][0],
            linewidth=1.5,
            label="Estimated unique count",
        )

        # Lines bordering the fill area
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, metrics[1]],
            color=self.theme["fill_colors"][1],
            linewidth=0.5,
        )
        ax.plot(
            chart_data.loc[:, "date"],
            chart_data.loc[:, metrics[2]],
            color=self.theme["fill_colors"][1],
            linewidth=0.5,
        )

        # Fill areas
        ax.fill_between(
            pd.to_datetime(chart_data.loc[:, "date"]),
            y1=chart_data.loc[:, metrics[1]].astype(float),
            y2=chart_data.loc[:, metrics[2]].astype(float),
            alpha=0.5,
            color=self.theme["fill_colors"][1],
            label="5-95%",
        )

        ax.yaxis.set_label_text("Unique Value Count", fontweight="bold")
        ax.set_title(f"Estimated Unique Values ({variable})", loc="left", fontweight="bold")
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(
            handles,
            labels,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.1),
            frameon=False,
            ncol=3,
        )
        ax.xaxis.set_major_formatter(_dates.DateFormatter(ts_format))
        ax.yaxis.set_major_formatter(_ticker.ScalarFormatter(useOffset=False, useMathText=False, useLocale=None))

        return fig

    def plot_data_types(self, variable, ts_format="%d-%b-%y", **kwargs):
        """Plots a Inferred Data Types chart."""
        if not self._confirm_profile_data:
            return

        chart_data = self._summary_data_preprocessing(variable)

        fig, ax = MatplotlibProfileVisualizer._chart_theming()

        type_metrics = {
            "Unknown": "type_unknown_count",
            "Null": "type_null_count",
            "Fractional": "type_fractional_count",
            "Integer": "type_integral_count",
            "Boolean": "type_boolean_count",
            "Text": "type_string_count",
        }

        for metric in type_metrics.keys():
            if chart_data.loc[:, type_metrics[metric]].sum() > 0:
                ax.plot(
                    chart_data.loc[:, "date"],
                    chart_data.loc[:, type_metrics[metric]],
                    linewidth=1.5,
                    label=metric,
                )

        ax.yaxis.set_label_text("Data Type Count", fontweight="bold")
        ax.set_title(f"Inferred Data Type ({variable})", loc="left", fontweight="bold")
        handles, labels = ax.get_legend_handles_labels()
        ax.legend(
            handles,
            labels,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.1),
            frameon=False,
            ncol=3,
        )
        ax.xaxis.set_major_formatter(_dates.DateFormatter(ts_format))
        ax.yaxis.set_major_formatter(_ticker.ScalarFormatter(useOffset=False, useMathText=False, useLocale=None))

        return fig


def array_creation(char_histos, bins, char_list):

    matrix = []
    for char in char_list:
        histo = char_histos.get(char, None)
        if histo is not None:
            matrix.append(histo)
        else:
            matrix.append(list(np.zeros_like(bins)))

    return np.array(char_list), np.array(matrix)
