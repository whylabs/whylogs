
import numpy as np


class ComposeTransforms:
    """
    """

    def __init__(self, transforms):
        self.transforms = transforms

    def __call__(self, x):
        for t in self.transforms:
            x = t(x)
        return x

    def __repr__(self):
        format_string = '('
        for t in self.transforms:
            format_string += '{0}'.format(t)
            format_string += "->"
        format_string = format_string[:-2]
        format_string += ')'
        return format_string


class Brightness:

    """
    """

    def __call__(self, img):
        """
        Args:
            pic (PIL Image or numpy.ndarray): Image to be converted to tensor.

        Returns:
            Tensor: Converted image.
        """
        _, _, bright = img.convert("HSV").split()
        return np.array(bright).reshape((-1, 1))

    def __repr__(self):
        return self.__class__.__name__


class Saturation:
    """

    """

    def __call__(self, img):
        """
        """
        _, sat, _ = img.convert("HSV").split()
        return np.array(sat).reshape((-1, 1))

    def __repr__(self):
        return self.__class__.__name__


class Hue:

    def __call__(self, img):
        """
        """
        h, _, _ = img.convert("HSV").split()
        return np.array(h).reshape((-1, 1))

    def __repr__(self):
        return self.__class__.__name__
