import os
from typing import Any, Callable, List, Optional, Tuple
import abc

EXTENSIONS = ('.csv', '.xls', '.jpg', '.jpeg', '.png', '.ppm', '.bmp',
              '.pgm', '.tif', '.tiff', '.webp')


def valid_file(fname):
    extension = os.path.splitext(fname)[1]
    return extension in EXTENSIONS


def file_loader(path: str) -> Any:
    from PIL import Image

    try:
        with open(path, 'rb') as f:
            img = Image.open(f)
            return img, img.format
    except Exception as e:
        raise e


class Dataset(abc.ABC):

    def __init__(self,
                 root_folder: str = "",
                 feature_transforms: Optional[List[Callable]] = None,
                 ) -> None:
        self.root_folder = os.path.expanduser(root_folder)
        self.feature_transforms = feature_transforms

    @abc.abstractmethod
    def __getitem__(self, index: int) -> Any:
        raise NotImplementedError

    def __len__(self) -> int:
        raise NotImplementedError

    def __repr__(self) -> str:

        head = "Dataset " + self.__class__.__name__
        body = ["Number of files: {}".format(self.__len__())]
        if self.root is not None:
            body.append("Folder location: {}".format(self.root))
        body += self.extra_repr().splitlines()
        if hasattr(self, "transforms") and self.transforms is not None:
            body += [repr(self.transforms)]
        lines = [head] + [" " * self._repr_indent + line for line in body]
        return '\n'.join(lines)


class LocalDataset(Dataset):

    def __init__(
            self,
            root_folder,
            loader: Callable[[str], Any] = file_loader,
            extensions: List[str] = EXTENSIONS,
            feature_transforms: Optional[List[Callable]] = None,
            is_valid_file: Optional[Callable[[str], bool]] = None,
    ) -> None:
        super().__init__(root_folder, feature_transforms=feature_transforms)
        self.folder_segmented_feature = []
        self._find_folder_feature()
        self._init_dataset()
        self.loader = loader

    def _find_folder_feature(self, ) -> None:
        self.folder_segmented_feature = [
            d.name for d in os.scandir(self.root_folder) if d.is_dir()]
        self.folder_segmented_feature.sort()
        self.folder_feature_dict = {
            seg_value: i for i, seg_value in enumerate(self.folder_segmented_feature)}

    def _init_dataset(self, ) -> List[Tuple[str, int]]:

        self.samples = []
        # is_valid_file = cast(Callable[[str], bool], is_valid_file)
        for folder_feature_value in sorted(self.folder_feature_dict.keys()):
            print(folder_feature_value)
            folder_index = self.folder_feature_dict[folder_feature_value]
            folder_feature_value = os.path.join(
                self.root_folder, folder_feature_value)
            if not os.path.isdir(folder_feature_value):
                continue
            for root, _, fnames in sorted(os.walk(folder_feature_value, followlinks=True)):
                for fname in sorted(fnames):
                    file_path = os.path.join(root, fname)
                    if valid_file(file_path):
                        item = file_path, folder_index
                        self.samples.append(item)

    def __getitem__(self, index: int) -> Tuple[Any, Any]:

        path, folder_feature_index = self.samples[index]
        sample, file_format = self.loader(path)

        return (sample, file_format), self.folder_segmented_feature[folder_feature_index],

    def __len__(self) -> int:
        return len(self.samples)
