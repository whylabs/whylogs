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

    def plot_string_length(self, variable, **kwargs):
        """Plots string length data ."""
        return self.visualizer.plot_string_length(variable, **kwargs)

    def plot_token_length(self, variable, character_list, **kwargs):
        """Plots token length data ."""
        return self.visualizer.plot_token_length(variable, **kwargs)

    def plot_char_pos(self, variable, character_list, **kwargs):
        """Plots character position data ."""
        return self.visualizer.plot_char_pos(variable, character_list=character_list, **kwargs)

    def plot_string(self, variable, character_list, **kwargs):
        """Plots string related data ."""
        return self.visualizer.plot_string(variable, character_list=character_list, **kwargs)

    def available_plots(self):
        """Returns available plots for selected framework."""
        return self.visualizer.available_plots()
