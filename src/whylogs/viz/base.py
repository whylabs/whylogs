class BaseProfileVisualizer:
    def __init__(self, framework=None, visualizer=None):
        self.framework = framework
        self.visualizer = visualizer
        self.profiles = []

    def set_profiles(self, profiles):
        """"""
        if len(profiles) <= 1:
            self.profiles = [profiles]
        else:
            self.profiles = profiles

        self.visualizer._init_data_preprocessing(profiles)

    def plot_distribution(self, variable, **kwargs):
        """Plots a distribution chart."""
        return self.visualizer.plot_distribution(variable, **kwargs)

    def plot_missing_values(self, variable, **kwargs):
        """Plots a Missing Value to Total Count ratio chart."""
        return self.visualizer.plot_missing_values(variable, **kwargs)

    def plot_uniqueness(self, variable, **kwargs):
        """Plots a Estimated Unique Values chart."""
        return self.visualizer.plot_uniqueness(variable, **kwargs)

    def plot_data_types(self, variable, **kwargs):
        """Plots a Inferred Data Types chart."""
        return self.visualizer.plot_data_types(variable, **kwargs)

    def available_plots(self):
        """Returns available plots for selected framework."""
        return self.visualizer.available_plots()
