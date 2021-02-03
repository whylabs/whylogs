
from whylogs.features.transforms import Hue, Brightness, Saturation, ComposeTransforms, SimpleBlur
from whylogs.core.image_profiling import image_loader
import os
import numpy as np
from PIL import Image
import pytest


TEST_DATA_PATH = os.path.abspath(os.path.join(os.path.realpath(
    os.path.dirname(__file__)), os.pardir, os.pardir, os.pardir, "testdata"))


def test_hue():

    img = image_loader(os.path.join(TEST_DATA_PATH, "images", "flower2.jpg"))

    transform = Hue()
    res_img = transform(img)
    assert np.array(res_img)[100][0] == 39


def test_saturation():

    img = image_loader(os.path.join(TEST_DATA_PATH, "images", "flower2.jpg"))
    transform = Saturation()
    res_img = transform(img)
    # compute average brightness
    res = np.mean(res_img)
    assert pytest.approx(res, 0.1) == 133.1
    res_img = transform(np.array(img))
    # compute average brightness
    res = np.mean(res_img)
    assert pytest.approx(res, 0.1) == 133.1


# Compute brightness of a black image
def test_zero_brightness():

    zero_images = np.zeros((3, 3, 3))
    transform = Brightness()

    im = Image.fromarray(zero_images.astype('uint8')).convert('RGBA')
    res_img = transform(im)

    for each_va in res_img:
        assert each_va[0] == 0
    res_img = transform(zero_images.astype('uint8'))
    for each_va in res_img:
        assert each_va[0] == 0

# Compute brightness of a white image


def test_one_brightness():

    ones_images = np.ones((3, 3, 3))*255
    transform = Brightness()

    im = Image.fromarray(ones_images.astype('uint8')).convert('RGBA')
    res_img = transform(im)

    for each_va in res_img:
        assert each_va[0] == 255
    res_img = transform(ones_images.astype('uint8'))
    for each_va in res_img:
        assert each_va[0] == 255


def test_Brightness():

    img = image_loader(os.path.join(TEST_DATA_PATH, "images", "flower2.jpg"))
    transform = Brightness()
    res_img = transform(img)
    # compute average brightness
    res = np.mean(res_img)
    assert pytest.approx(res, 0.1) == 117.1
    res_img = transform(np.array(img))
    # compute average brightness
    res = np.mean(res_img)
    assert pytest.approx(res, 0.1) == 117.1


def test_simple_blur(image_files):
    expected_results = [3754.4, 1392.5, 13544.2, ]
    transform = SimpleBlur()
    for idx, eachimg in enumerate(image_files):
        img = image_loader(eachimg)
        res = transform(img)
        # compute average brightness

        assert pytest.approx(res, 0.1) == expected_results[idx]


def test_compose():
    img = image_loader(os.path.join(TEST_DATA_PATH, "images", "flower2.jpg"))
    transforms = ComposeTransforms([Saturation(), np.mean])
    res_compose = transforms(img)
    transform_sat_only = Saturation()
    res_img = transform_sat_only(img)
    # compute average brightness
    res = np.mean(res_img)
    assert res == res_compose
