import unittest
from PIL.Image import Image as ImageType
from whylogs.io.local_dataset import LocalDataset, file_loader
import os


def test_file_loader(test_data_path):

    img, imgfmt = file_loader(os.path.join(
        test_data_path, "images", "flower2.jpg"))
    assert isinstance(img, ImageType)
    assert isinstance(imgfmt, str)
    assert imgfmt == "JPEG"


def test_imagefolder(test_data_path):

    folder_dataset = os.path.join(test_data_path, 'fake_dataset')

    folder_segmented_features = sorted(['A_target', 'B_target'])

    segment_A_target_files = [
        os.path.join(folder_dataset, 'A_target', file) for file in ('flower2.jpg', 'grace_hopper_517x606.jpg',)
    ]
    segment_B_target_files = [
        os.path.join(folder_dataset, 'B_target', file) for file in ('16bit.cropped.tif', 'lending_club_1000.csv')
    ]
    dataset = LocalDataset(
        folder_dataset, loader=lambda x: x)

    # test if all classes are present
    assert folder_segmented_features == sorted(
        dataset.folder_segmented_feature)

    # test if combination of classesp and class_to_index functions correctly
    for seg in folder_segmented_features:
        assert seg == dataset.folder_segmented_feature[dataset.folder_feature_dict[seg]]

    # test if all images were detected correctly
    segment_A_idx = dataset.folder_feature_dict['A_target']
    segment_B_idx = dataset.folder_feature_dict['B_target']
    files_A_target = [(file, segment_A_idx)
                      for file in segment_A_target_files]
    files_B_target = [(file, segment_B_idx)
                      for file in segment_B_target_files]
    files = sorted(files_A_target + files_B_target)
    assert files == dataset.items
    output_A_target = [(file, 'A_target')
                       for file in segment_A_target_files]
    output_B_target = [(file, 'B_target')
                       for file in segment_B_target_files]
    outputs_expected = sorted(output_A_target + output_B_target)
    # test if the datasets outputs all images correctly
    outputs = sorted([dataset[i] for i in range(len(dataset))])
    assert outputs_expected == outputs


def test_empty_imagefolder(tmpdir):
    dataset = LocalDataset(
        tmpdir, loader=lambda x: x)


def test_imagefolder(test_data_path):

    folder_dataset = os.path.join(test_data_path, 'fake_dataset')

    folder_segmented_features = sorted(['A_target', 'B_target'])

    segment_A_target_files = [
        os.path.join(folder_dataset, 'A_target', file) for file in ('flower2.jpg', 'grace_hopper_517x606.jpg',)
    ]
    segment_B_target_files = [
        os.path.join(folder_dataset, 'B_target', file) for file in ('16bit.cropped.tif', 'lending_club_1000.csv')
    ]
    dataset = LocalDataset(
        folder_dataset, loader=lambda x: x)

    # test if all classes are present
    assert folder_segmented_features == sorted(
        dataset.folder_segmented_feature)

    # test if combination of classesp and class_to_index functions correctly
    for seg in folder_segmented_features:
        assert seg == dataset.folder_segmented_feature[dataset.folder_feature_dict[seg]]
