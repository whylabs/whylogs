import torch
import torchvision.models as models
from torch import nn
from torch.nn import functional


class Image_Embeddings(nn.Module):
    def __init__(self, model_config=None):
        super(Image_Embeddings, self).__init__()
        self._config = model_config

        if self._config is None:
            self._config = {
                "backbone": "resnet",
                "outlayer": "C5",
                "model_path": None,
            }
        if self._config["backbone"] == "resnet":

            if self._config.get("model_path", None) is not None:
                self.resnet101 = models.resnet101()
                self.resnet101.load_state_dict(torch.load(self._config["model_path"]), strict=False)
            else:
                self.resnet101 = models.resnet101(pretrained=True)

            if self._config["outlayer"] == "C5":
                self.output_layer = nn.Sequential(*(list(self.resnet101.children())[:-1]))

            elif self._config["outlayer"] == "C4":
                self.output_layer = nn.Sequential(*(list(self.resnet101.children())[:-3]))
                self.glbAvgPool = nn.AdaptiveAvgPool2d(1)
        else:
            raise ValueError("Undefined Image Backbone")

    def freeze_all_layer(self):
        for p in self.resnet101.parameters():
            p.requires_grad = False

    def freeze_nth_layer(self, n):
        for i, p in enumerate(self.resnet101.parameters()):
            if i < n:
                p.requires_grad = False

    def forward(self, image_variable):
        x = self.output_layer(image_variable)
        if self._config["outlayer"] == "C4":
            x = self.glbAvgPool(x)

        x = functional.normalize(x, p=2, dim=1)
        x.squeeze_(3)
        x.squeeze_(2)
        return x
