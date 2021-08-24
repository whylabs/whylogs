# import smart_open as open
import json
import os
from typing import Any, Callable, Dict, Union

import pandas as pd
import puremagic

EXTENSIONS = (
    ".csv",
    ".jpg",
    ".jpeg",
    ".png",
    ".ppm",
    ".bmp",
    ".jsonl",
    ".json",
    ".pgm",
    ".tif",
    ".tiff",
    ".webp",
    ".xls",
    ".xlsx",
    ".xlsm",
    ".xlsb",
    ".odf",
    ".ods",
    ".odt",
)

IMAGE_EXTENSIONS = (
    ".jpg",
    ".jpeg",
    ".png",
    ".ppm",
    ".bmp",
    ".pgm",
    ".tif",
    ".tiff",
    ".webp",
    ".gif",
)

PD_EXCEL_FORMATS = (".xls", ".xlsx", ".xlsm", ".xlsb", ".odf", ".ods", ".odt")


def valid_file(fname: str):
    """
    simple check if extension is part of the implemented ones

    Args:
        fname (str): file path

    Returns:
        bool
    """
    if os.path.isdir(fname):
        return False
    extension = os.path.splitext(fname)[1]
    return extension in EXTENSIONS


def extension_file(path: str):
    """
    Check the enconding format based on the magic number
    if file has no magic number we simply use extension.
    More advance analytics of file content is needed, potentially
    extendind to a lib like libmagic

    Args:
        path (str): File path

    Returns:
        file_extension_given: str: extension of encoding data
        magic_data : dic : any magic data information available including
                            magic number : byte
                            mime_type: str
                            name : str

    """
    file_extension_given = os.path.splitext(path)[1]

    format_file = puremagic.magic_file(path)
    if len(format_file) > 0:
        if format_file[0].confidence < 0.5:
            print("poor confidence")
            magicdata = {
                "byte_match": None,
                "mime_type": None,
                "name": None,
            }
            return file_extension_given, magicdata
        else:
            magicdata = {
                "byte_match": "{}".format(format_file[0].byte_match),
                "mime_type": format_file[0].mime_type,
                "name": format_file[0].name,
            }
            return format_file[0].extension, magicdata
    else:
        magicdata = {
            "byte_match": None,
            "mime_type": None,
            "name": None,
        }
        return file_extension_given, magicdata


def image_loader(path: str):
    """
    tries to load  image using the PIL lib

    Args:
        path (str): path to image files

    Returns:
        PIL.Image.Image : image data and image encoding format
    """
    try:
        from PIL import Image

        img = Image.open(open(path, "rb"))
        return img, img.format
    except Exception as e:
        raise (e)


def json_loader(path: str = None) -> Union[Dict, list]:
    """
    Loads json or jsonl data

    Args:
        path (str, optional): path to file

    Returns:
        objs : Union[Dict, list]: Returns a list or dict of json data
        json_format : format of file (json or jsonl)
    """
    check_extension = os.path.splitext(path)[1]
    objs = []
    json_format = None
    with open(path, "r") as file_p:
        if check_extension == ".jsonl":
            lines = file_p.readlines()
            for line in lines:
                objs.append(json.loads(line))
            json_format = "jsonl"
        elif check_extension == ".json":
            objs = json.loads(file_p)
            json_format = "json"
    return objs, json_format


def file_loader(path: str, valid_file: Callable[[str], bool] = valid_file) -> Any:
    """
    Factory for file data

    Args:
        path (str): path to file
        valid_file (Callable[[str], bool], optional): Optional valid file check,

    Returns:
        data : Tuple( [] Dataframe or Image data (PIL format), or Dict],
        magic_data: Dict of magic number data)

    Raises:
        NotImplementedError: Description
    """
    if not valid_file(path):
        return None, None

    ext, magic_data = extension_file(path)
    if ext in IMAGE_EXTENSIONS:
        data, file_format = image_loader(path)
        return (data, magic_data), file_format
    elif (ext == ".json") or (ext == ".jsonl"):
        data, file_format = json_loader(path)
        return (data, magic_data), file_format
    elif ext == ".csv":
        data = pd.read_csv(path)
        return (data, magic_data), "csv"
    elif ext in PD_EXCEL_FORMATS:
        # default to 1st sheet
        try:
            data = pd.read_excel(path)
            return (data, magic_data), "excel"
        except Exception as e:
            raise NotImplementedError("File not supported: {}".format(e))
    else:
        IOError("File format, {} not supported yet : {} ".format(ext, magic_data))
