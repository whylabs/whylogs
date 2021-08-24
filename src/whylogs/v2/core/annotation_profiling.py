from typing import Callable, Dict, List, Optional

import numpy as np
import pandas as pd

from whylogs.io.file_loader import file_loader


class Rectangle:

    """
    Helper class to compute minimal bouding box intersections and/or iou
    minimal stats properties of boudning box

    Attributes:
        area (float): Description
        aspect_ratio (TYPE): Description
        boundingBox (TYPE): Description
        centroid (TYPE): Description
        confidence (TYPE): Description
        height (TYPE): Description
        labels (TYPE): Description
        width (TYPE): Description
    """

    # replace with shapley functions and methods
    # or move to cpp/cpython
    def __init__(self, boundingBox, confidence=None, labels=None):
        self.boundingBox = boundingBox
        self._x1 = boundingBox[0][0]
        self._x2 = boundingBox[1][0]
        self._y1 = boundingBox[0][1]
        self._y2 = boundingBox[1][0]
        self.confidence = confidence
        self.labels = labels
        self.area = abs(self.x2 - self.x1) * abs(self.y2 - self.y1)
        self.width = abs(self.x2 - self.x1)
        self.height = abs(self.y2 - self.y1)
        self.aspect_ratio = self.width / self.height if self.height > 0 else 0.0
        self.centroid = [self.x1 + self.width / 2, self.y1 + self.height / 2]

    @property
    def x1(self):
        return self._x1

    @property
    def x2(self):
        return self._x2

    @property
    def y1(self):
        return self._y1

    @property
    def y2(self):
        return self._y2

    def intersection(self, Rectangle_2):
        x_left = max(self.x1, Rectangle_2.x1)
        y_top = max(self.y1, Rectangle_2.y1)
        x_right = min(self.x2, Rectangle_2.x2)
        y_bottom = min(self.y2, Rectangle_2.y2)
        if x_right < x_left or y_bottom < y_top:
            return 0.0
        intersection_area = (x_right - x_left) * (y_bottom - y_top)

        return intersection_area

    def iou(self, Rectangle_2):
        intersection_area = self.intersection(Rectangle_2)
        if Rectangle_2.area <= 0 or self.area <= 0:
            return 0.0
        return intersection_area / (self.area + Rectangle_2.area - intersection_area)


BB_ATTRIBUTES = (
    "annotation_count",
    "annotation_density",
    "area_coverage",
    "bb_width",
    "bb_height",
    "bb_area",
    "bb_aspect_ratio",
    "confidence",
    "dist_to_center",
)


class TrackBB:
    def __init__(
        self,
        filepath: str = None,
        obj: Dict = None,
        feature_transforms: Optional[List[Callable]] = None,
        feature_names: str = "",
    ):

        if filepath is None and obj is None:
            raise ValueError("Need  filepath or object data")
        if filepath is not None:
            (self.obj, magic_data), self.fmt = file_loader(filepath)

        else:
            self.obj = obj
        self.per_image_stats = []
        self.all_bboxes = []
        self.calculate_metrics()

    def calculate_metrics(
        self,
    ):

        for obj in self.obj:

            annotation_metrics = {}
            annotations = obj.get("annotation", None)
            if annotations is None:
                continue
            img_height_pixel = annotations["size"]["height"]
            img_width_pixel = annotations["size"]["width"]
            img_rect = Rectangle([[0, 0], [img_width_pixel, img_height_pixel]])
            annotation_metrics["annotation_count"] = len(annotations["object"])
            annotation_metrics["annotation_density"] = annotation_metrics["annotation_count"] / img_rect.area

            # Get individual bbox metrics
            annotation_metrics["area_coverage"] = 0

            for bb_obj in filter(lambda x: "bndbox" in x, annotations["object"]):
                bounding_box_metric = {}

                rect1 = Rectangle(
                    [
                        [bb_obj["bndbox"]["xmin"], bb_obj["bndbox"]["ymin"]],
                        [bb_obj["bndbox"]["xmax"], bb_obj["bndbox"]["ymax"]],
                    ],
                    confidence=bb_obj["confidence"],
                )

                bounding_box_metric["confidence"] = rect1.confidence

                bounding_box_metric["bb_width"] = rect1.width
                bounding_box_metric["bb_height"] = rect1.height
                bounding_box_metric["bb_area"] = rect1.area

                bounding_box_metric["bb_aspect_ratio"] = rect1.aspect_ratio

                bounding_box_metric["dist_to_center"] = np.linalg.norm(
                    [
                        rect1.centroid[0] - (img_width_pixel / 2.0),
                        rect1.centroid[1] - (img_height_pixel / 2.0),
                    ],
                    ord=2,
                )

                annotation_metrics["area_coverage"] += rect1.intersection(img_rect) / (img_rect.area * annotation_metrics["annotation_count"])

                self.all_bboxes.append(bounding_box_metric)

            # Send object to metrics
            self.per_image_stats.append(annotation_metrics)

    def __call__(self, profiles):

        if not isinstance(profiles, list):
            profiles = [profiles]

        per_image_dataframe = pd.DataFrame(self.per_image_stats)
        bounding_boxes = pd.DataFrame(self.all_bboxes)
        for each_profile in profiles:

            each_profile.track_dataframe(per_image_dataframe)
            each_profile.track_dataframe(bounding_boxes)
