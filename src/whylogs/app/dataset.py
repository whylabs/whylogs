import os
from typing import Any, Callable, List, Optional, Tuple
import ABC

from PIL import
from PIL.TiffTags import TAGS

EXTENSIONS = ('.csv','.xls','.jpg', '.jpeg', '.png', '.ppm', '.bmp', 
              '.pgm', '.tif', '.tiff','.webp')



class Dataset(ABC):
"""
"""
    def __init__(self,
        root_folder: str = "",
        transforms : Optional[Callable]=None,
        )->None:
    def __getitem__(self, index: int) -> Any:
        raise NotImplementedError
    def __len__(self)-> int :
        raise NotImplementedError

    def __repr__(self)-> str:
        head = "Dataset "+ self.__class__.__name__
        body = ["Number of files: {}".format(self.__len__())]
        if self.root is not None:
            body.append("Folder location: {}".format(self.root))
        body += self.extra_repr().splitlines()
        if hasattr(self, "transforms") and self.transforms is not None:
            body += [repr(self.transforms)]
        lines = [head] + [" " * self._repr_indent + line for line in body]
        return '\n'.join(lines)


class WhylogsDataset(Dataset):

    def __init__(
    self,
    root_folder,
    loader: Callable[[str],Any]= file_loader,
    extensions: List[str] = EXTENSIONS,
    transform:Optional[Callable] = None,
    
    is_valid_file: Optional[Callable[[str],bool]]= None,
    ) -> None:
    super(WhylogsDataset, obj).__init__(root_folder, transform= transform )


    def _find_segments(self, dir: str)-> Tuple[List[str], Dict[str, int]]:
        main_segments = [d.name for d in os.scandir(dir) if d.is_dir()]
        main_segments.sort()
        segment_dict= { seg_value: i for i, seg_value in enumerate(classes)}

    def __getitem__(self, index: int) -> Tuple[Any,Any]:
         path, target = self.samples[index]
        sample = self.loader(path)
        if self.transform is not None:
            sample = self.transform(sample)
        if self.target_transform is not None:
            target = self.target_transform(target)
    def __len__(self)->int:
        return len(self.data)


def file_loader(path: str) -> Any:
    # open path as file to avoid ResourceWarning (https://github.com/python-pillow/Pillow/issues/835)
    check_extensions=
    if image_extension:

    try:
        with open(path, 'rb') as f:
            img = Image.open(f)
            meta_dict= {TAGS[key] : img.tag[key] for key in img.tag.iterkeys()}
            print(meta_dict)
            return img.convert('RGB')
    except:
        with open(path,'r') as f:
            json= json.load(f)

