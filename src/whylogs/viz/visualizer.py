from warnings import warn

from .base import BaseProfileVisualizer


class ProfileVisualizer(BaseProfileVisualizer):
    """"""

    def __init__(self, framework="matplotlib"):
        """Initializes the ProfileVisualizer class."""
        self.available_frameworks = ["matplotlib"]
        self.framework = framework
        self.visualizer = self.__subclass_framework(framework)
        super().__init__(framework=self.framework, visualizer=self.visualizer)

    def __subclass_framework(self, framework="matplotlib"):
        if framework == "matplotlib":
            from .matplotlib.visualizer import MatplotlibProfileVisualizer

            return MatplotlibProfileVisualizer()

        warn(
            f"""Unable to use chosen visualization framework. Please choose
                one of the following frameworks: {self.available_frameworks}
                using visualizer.enable_framework().""",
            RuntimeWarning,
        )
        return
