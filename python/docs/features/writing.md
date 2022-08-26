# Writers

Writers are responsible for uploading the generated profiles to specific locations. This could be to your local filesystem, cloud object storages or monitoring platforms that are capable of ingesting the profiles.

Currently, the supported writer are:

- **Local Writer** - Writes profiles to the local filesystem
- **S3 Writer** - Uploads profiles to AWS S3
- **WhyLabs Writer** - Uploads profiles to WhyLabs Platform

For more information on how to write and upload profiles, check our example on [Writing Profiles](https://github.com/whylabs/whylogs/blob/mainline/python/examples/integrations/writers/Writing_Profiles.ipynb)!
