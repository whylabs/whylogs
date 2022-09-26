Ecommerce Dataset
=================

The Ecommerce dataset contains transaction information of several products for a popular grocery supermarket in India. It contains features such as the product's description, category, market price and user rating.

The original data was sourced from Kaggle's `BigBasket Entire Product List <https://www.kaggle.com/datasets/surajjha101/bigbasket-entire-product-list-28k-datapoints>`_ . From the source data additional transformations were made, such as: oversampling and feature creation/engineering.

License:
CC BY-NC-SA 4.0

Usage
-----

You can follow this guide to see how to use the ecommerce dataset:

.. toctree::
    :maxdepth: 1

    ../examples/datasets/ecommerce


Versions and Data Partitions
----------------------------

Currently the dataset contains one version: **base**. The task for the base version is to classify wether an incoming product should be provided a discount, given product features such as history of items sold, user rating, category and market price.
The **base** version contains two partitions: **Baseline** and **Inference**

base
~~~~

This version contains 5

* Baseline
   * Number of instances: 34743
   * Number of features: 19
      * Input Features: 5
      * Target Features: 1
      * Prediction Features: 2
      * Extra Features: 11
   * Period: from 2022-08-09 to 2022-08-16
* Inference
   * Number of instances: 86899
   * Number of features: 19
      * Input Features: 5
      * Target Features: 1
      * Prediction Features: 2
      * Extra Features: 11
   * Period: from 2022-08-19 to 2022-09-08

There are 11 possible categories for a given product. In order to get the desired size for the dataset, original data was oversampled for each category with the `Random Oversampling Examples (ROSE) <https://link.springer.com/article/10.1007/s10618-012-0295-5>`_.

The original data didn't contain date and time information. Data was artificially partitioned into separate days in a preprocessing stage for the fabrication of this dataset.

Features Description
--------------------

Target Features
~~~~~~~~~~~~~~~

These are features that are typically targeted for prediction/classification.

.. list-table:: Target Features
    :widths: 20 50 10 20
    :header-rows: 1

    *   - Feature
        - Description
        - Type
        - Present in Versions
    *   - output_discount
        - if the purchased product had a discount, bool
        - Target
        - all


Prediction Features
~~~~~~~~~~~~~~~~~~~

These features are outputs from a given ML model. Can be directly the prediction/predicted class or also scores such as uncertainty, probability and confidence scores.

.. list-table:: Prediction Features
    :widths: 20 50 10 20
    :header-rows: 1

    *   - Feature
        - Description
        - Type
        - Present in Versions
    *   - output_prediction
        - Random Forest model's prediction for target variable `output_discount`
        - Prediction
        - all
    *   - output_score
        - Class probability for the predicted class
        - Prediction
        - all

`output_prediction` and `output_score` was obtaind by training a Random Forest model with the SKLearn library. Data used to train the model was previously separated and is not present in this dataset.

The remaining partitions (baseline and inference) were each oversampled and split into separate days.

Extra Features
~~~~~~~~~~~~~~

These are extra features that are not of any of the previous categories, but still contain relevant information about the data.

.. list-table:: Miscellaneous Features
    :widths: 20 50 10 20
    :header-rows: 1

    *   - Feature
        - Description
        - Type
        - Present in Versions
    *   - category.Baby Care
        - Binarized category feature for `Baby Care` class
        - Extra
        - all
    *   - category.Bakery, Cakes and Dairy
        - Binarized category feature for `Bakery, Cakes and Dairy` class
        - Extra
        - all
    *   - category.Beauty and Hygiene
        - Binarized category feature for `Beauty and Hygiene` class
        - Extra
        - all
    *   - category.Beverages
        - Binarized category feature for `Beverages` class
        - Extra
        - all
    *   - category.Cleaning and Household
        - Binarized category feature for `Cleaning and Household` class
        - Extra
        - all
    *   - category.Eggs, Meat and Fish
        - Binarized category feature for `Eggs, Meat and Fish` class
        - Extra
        - all
    *   - category.Foodgrains, Oil and Masala
        - Binarized category feature for `Foodgrains, Oil and Masala` class
        - Extra
        - all
    *   - category.Fruits and Vegetables
        - Binarized category feature for `Fruits and Vegetables` class
        - Extra
        - all
    *   - category.Gourmet and World Food
        - Binarized category feature for `Gourmet and World Food` class
        - Extra
        - all
    *   - category.Kitchen, Garden and Pets
        - Binarized category feature for `Kitchen, Garden and Pets` class
        - Extra
        - all
    *   - category.Snacks and Branded Foods
        - Binarized category feature for `Snacks and Branded Foods` class
        - Extra
        - all


Input Features
~~~~~~~~~~~~~~

These are input features that were used to train and predict the prediction features.

.. list-table:: Input Features
    :widths: 20 50 10 20
    :header-rows: 1

    *   - Feature
        - Description
        - Type
        - Present in Versions
    *   - product
        - text description of the product, str
        - Input
        - all
    *   - sales_last_week
        - number of items sold in the last week, int
        - Input
        - all
    *   - market_price
        - the product's market price, float
        - Input
        - all
    *   - rating
        - the user's rating for the product at time of purchase, float
        - Input
        - all
    *   - category
        - the product's category, str
        - Input
        - all

The `sales_last_week` feature was created based on the product's total count for the complete dataset, and was created for demonstrational purposes.

License
-------

CC BY NC SA 4.0


References
----------

- Giovanna Menardi and Nicola Torelli. Training and assessing classification rules with imbalanced data. Data Mining and Knowledge Discovery, 28:92–122, 2014.
