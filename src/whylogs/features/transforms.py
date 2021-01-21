import logging
import numpy as np

from typing import Union, List

logger = logging.getLogger(__name__)

try:
    from PIL import Image
    from PIL import ImageFilter
    from PIL.Image import Image as ImageType
except ImportError as e:
    ImageType = None
    logger.debug(str(e))
    logger.debug("Unable to load PIL; install pillow for image support")


class ComposeTransforms:
    """
     Outputs the composition of each transformation passed in transforms
    """

    def __init__(self, transforms: List):
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
     Outputs the Brightness of each pixel in the image
    """

    def __call__(self, img: Union[ImageType, np.ndarray]) -> np.ndarray:
        """
        Args:
            img (Union[Image, np.ndarray]): Either a PIL image or numpy array with int8 values
        Returns:
            np.ndarray: Converted image.

        Deleted Parameters:
            pic (PIL Image or numpy.ndarray): Image to be converted to tensor.
        """
        if isinstance(img, np.ndarray):
            img = Image.fromarray(img)

        _, _, bright = img.convert("HSV").split()
        return np.array(bright).reshape((-1, 1))

    def __repr__(self):
        return self.__class__.__name__


class Saturation:
    """Summary
    Outputs the saturation of each pixel in the image
    """

    def __call__(self, img: Union[ImageType, np.ndarray]) -> np.ndarray:
        """
        Args:
            img (Union[Image, np.ndarray]): Either a PIL image or numpy array with int8 values

        Returns:
            np.ndarray:  (1,number_pixels) array for saturation values for the image
        """
        if isinstance(img, np.ndarray):
            img = Image.fromarray(img)
        _, sat, _ = img.convert("HSV").split()
        return np.array(sat).reshape((-1, 1))

    def __repr__(self):
        return self.__class__.__name__


class Hue:

    def __call__(self, img: Union[ImageType, np.ndarray]) -> np.ndarray:
        """
        Args:
            img (Union[Image, np.ndarray]): Either a PIL image or numpy array with int8 values
        Returns:
            np.ndarray: (1,number_pixels) array for hue values for the image
        """
        if isinstance(img, np.ndarray):
            img = Image.fromarray(img)
        h, _, _ = img.convert("HSV").split()
        return np.array(h).reshape((-1, 1))

    def __repr__(self):
        return self.__class__.__name__


class SimpleBlur:
    """Simple Blur Ammount computation based on variance of laplacian
    Overall metric of how blurry is the image. No overall scale.

    """

    def __call__(self, img: Union[ImageType, np.ndarray]) -> float:
        """
        Args:
            img (Union[Image, np.ndarray]): Either a PIL image or numpy array with int8 values

        Returns:
            float: variance of laplacian of image.
        """
        if isinstance(img, np.ndarray):
            img = Image.fromarray(img)
        # compute laplacian
        img = img.convert("RGB")
        img = img.filter(ImageFilter.Kernel((3, 3), (-1, -1, -1, -1, 8,
                                                     -1, -1, -1, -1), 1, 0))
        value = np.var(np.array(img).flatten())
        return value

    def __repr__(self):
        return self.__class__.__name__
