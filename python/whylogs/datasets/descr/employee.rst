Employee Dataset
================

The employee dataset contains annual salary information for employees of an American county. It contains features related to each employee, such as employee's department, gender, salary, and hiring date.

The original data was sourced from the `employee_salaries` OpenML dataset, and can be found here: https://www.openml.org/d/42125. From the source data additional transformations were made, such as: data cleaning, feature creation and feature engineering.

License:
CC0: Public Domain

Usage
-----

You can follow this guide to see how to use the ecommerce dataset:

.. toctree::
    :maxdepth: 1

    ../examples/datasets/employee

Versions and Data Partitions
----------------------------

Currently the dataset contains one version: **base**. This dataset has no particular tasks defined, as it is aimed to explore data quality issues that are not necessarily related to ML.
The **base** version contains two partitions: **Baseline** and **Production**

base
~~~~

* Baseline
   * Number of instances: 836
   * Number of features: 12
      * Input Features: 11
      * Target Features: 0
      * Prediction Features: 0
      * Extra Features: 1
   * Period: 2023-01-16 (single day)
* Inference
   * Number of instances: 26573
   * Number of features: 12
      * Input Features: 11
      * Target Features: 0
      * Prediction Features: 0
      * Extra Features: 1
   * Period: from 2023-01-17 to 2023-02-16

The original data didn't contain date and time information. Data was randomly sampled from a source dataframe to compose separate days in a preprocessing stage for the fabrication of this dataset.

Features Description
--------------------

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
    *   - assignment_category
        - The assignment category of the employee. Can be Full-time or Part-time and essentially carries the same information as the full_time/part_time features.
        - Extra
        - all

Regular Features
~~~~~~~~~~~~~~~~

.. list-table:: Regular Features
    :widths: 20 50 30
    :header-rows: 1

    *   - Feature
        - Description
        - Present in Versions
    *   - employee_id
        - The employee's ID, int
        - all
    *   - gender
        - The employee's gender, str
        - all
    *   - overtime_pay
        - The employee's overtime pay, float
        - all
    *   - department
        - The employee's department, str
        - all
    *   - position_title
        - The employee's position title, str
        - all
    *   - date_first_hired
        - The employee's date of first hire, str
        - all
    *   - date_first_hired
        - The employee's date of first hire, str
        - all
    *   - year_first_hired
        - The employee's year of first hire, int
        - all
    *   - salary
        - The employee's salary, float
        - all
    *   - full_time
        - The employee's full time status, bool
        - all
    *   - part_time
        - The employee's part time status, bool
        - all
    *   - sector
        - The employee's sector, str
        - all

License
-------

CC0: Public Domain


References
----------

.. [1] https://www.openml.org/d/42125 - Employee Salaries Dataset
