
import numpy as np
import numpy.typing as npt


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

    def __call__(self, img: Union[Image, npt.ArrayLike])->np.ndarray:
        """
        Args:
            pic (PIL Image or numpy.ndarray): Image to be converted to tensor.

        Returns:
            Tensor: Converted image.
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

    def __call__(self, img: Union[Image, npt.ArrayLike])->np.ndarray:
        """
        Args:
            img (Union[Image, npt.ArrayLike]): Description

        Returns:
            np.ndarray: Description
        """
        if isinstance(img, np.ndarray):
            img = Image.fromarray(img)
        _, sat, _ = img.convert("HSV").split()
        return np.array(sat).reshape((-1, 1))

    def __repr__(self):
        return self.__class__.__name__


class Hue:

    def __call__(self, img: Union[Image, npt.ArrayLike])->np.ndarray:
        """
        Args:
            img (Union[Image, npt.ArrayLike]): Description

        Returns:
            np.ndarray: Description
        """
        if isinstance(img, np.ndarray):
            img = Image.fromarray(img)
        h, _, _ = img.convert("HSV").split()
        return np.array(h).reshape((-1, 1))

    def __repr__(self):
        return self.__class__.__name__


class SimpleBlur:

    """Simple Blur Ammount computation based on variance of laplacian
    Overall metric of how blurry is the image. 

    """

    def __call__(self, img: Union[Image, npt.ArrayLike])->float:
        """
        """
        if isinstance(img, np.ndarray):
            img = Image.fromarray(img)
        # compute laplacian
        img = img.filter(ImageFilter.Kernel((3, 3), (-1, -1, -1, -1, 8,
                                                     -1, -1, -1, -1), 1, 0))
        value = np.variance(np.array(img).flatten()).reshape((-1, 1))
        return value

    def __repr__(self):
        return self.__class__.__name__
