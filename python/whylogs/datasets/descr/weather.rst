Weather Forecast Dataset
========================

The Weather Forecast Dataset contains meteorological features at a particular place (defined by latitude and longitude features) and time. This dataset can present data distribution shifts over both time and space.

The original data was sourced from the `Weather Prediction Dataset <https://github.com/Shifts-Project/shifts>`_. From the source data additional transformations were made, such as: feature renaming, feature selection and subsampling.
The original dataset is described in `Shifts: A Dataset of Real Distributional Shift Across Multiple Large-Scale Tasks <https://arxiv.org/pdf/2107.07455.pdf>`_, by **Malinin, Andrey, et al.**

Usage
-----

You can follow this guide to see how to use the weather dataset:

.. toctree::
    :maxdepth: 1

    ../examples/datasets/weather


Versions and Data Partitions
----------------------------

Currently the dataset contains two versions: **in_domain** and **out_domain**. The task is the same for the both versions: predicting the temperature at a given location and time, base on input meteorological features.
Each version contains two partitions: **Baseline**  and **Inference**.

in_domain
~~~~~~~~~~

This version contains data from the same domain between **Baseline** and **Inference**. This means that both subsets contains meteorological records for the same major climate types.

Both subsets contain records from the following regions: **Tropical**, **Dry** and **Mild Temperate**.
Note that the time periods are different between subsets, which means that distribution shifts may occur over time.

* Baseline
   * Number of instances: 10000
   * Number of features: 55
      * Input Features: 48
      * Target Features: 1
      * Prediction Features: 2
      * Miscellaneous Features: 4
   * Period: from 2018-09-01 to 2019-01-31
* Inference
   * Number of instances: 13066
   * Number of features: 55
      * Input Features: 48
      * Target Features: 1
      * Prediction Features: 2
      * Miscellaneous Features: 4
   * Period: from 2019-02-01 to 2019-03-29

out_domain
~~~~~~~~~~

This version contains data from different domains between **Baseline** and **Inference**. This means that subsets contains meteorological records for different major climate types.

Baseline contains record from the following climate types:  **Tropical**, **Dry** and **Mild Temperate**, while **Inference** contains records from the following climate types: **Snow** and **Polar**.
Note that the time periods are different between subsets, which means that distribution shifts may also occur over time.


* Baseline - Equal to **in_domain**'s baseline.
* Inference
   * Number of instances: 11000
   * Number of features: 55
      * Input Features: 48
      * Target Features: 1
      * Prediction Features: 2
      * Miscellaneous Features: 4
   * Period: from 2019-05-14 to 2019-07-08


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
    *   - temperature
        - air temperature 2m above the ground, C
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
    *   - prediction_temperature
        - regression model's prediction for target variable `temperature`
        - Prediction
        - all
    *   - uncertainty
        - uncertainty measure of the given `prediction_temperature` prediction
        - Prediction
        - all

The ``prediction_temperature`` feature was obtained by training an ensemble of three GBDT models with the CatBoost library.
The model was trained with data from the same period given by the baseline dataset (2019-02-01 to 2019-03-29). However, all of the data partitions were taken from separate partitions of the `original dataset <https://github.com/Shifts-Project/shifts>`_:

.. list-table:: Partition Relationships
    :widths: 20 20 20 20 20
    :header-rows: 1

    *   -
        - Training
        - Baseline
        - Inference (in_domain)
        - Inference (out_domain)
    *   - **Original Data Source**
        - Training
        - Evaluation (in_domain)
        - Development (in_domain)
        - Evaluation (out_domain)


The ``uncertainty`` feature was calculated by using the ``total variance`` measure, which is the sum of the variance of the predicted mean and the mean of the predicted value.

Miscellaneous Features
~~~~~~~~~~~~~~~~~~~~~~~

These are metadata features that are not of any of the previous categories, but still contain relevant information about the data.

.. list-table:: Miscellaneous Features
    :widths: 20 50 10 20
    :header-rows: 1

    *   - Feature
        - Description
        - Type
        - Present in Versions
    *   - longitude
        - geographical longitude, degrees
        - Misc.
        - all
    *   - latitude
        - geographical latitude, degrees
        - Misc.
        - all
    *   - climate
        - major climate type
        - Misc.
        - all
    *   - date
        - date of the measurement
        - Misc.
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
    *   - height_sea_level
        - height above or below sea level, m
        - Input
        - all
    *   - sun_elevation
        - sun height proxy above horizon (without corrections for precision and diffraction)
        - Input
        - all
    *   - pressure
        - climate pressure, mmHg
        - Input
        - all
    *   - cmc_temperature_grad
        - difference between temperatures on adjacent horizons at 2m, K
        - Input
        - all
    *   - cmc_temperature
        - temperature at 2m, K
        - Input
        - all
    *   - dew_point_temperature
        - dew point temp at 2m, K
        - Input
        - all
    *   - absolute_humidity
        - absolute humidity from 0 to 1
        - Input
        - all
    *   - snow_depth
        - snow depth, m
        - Input
        - all
    *   - rain_accumulated
        - rain accumulated from cmc gentime, mm
        - Input
        - all
    *   - snow_accumulated
        - snow accumulated from cmc gentime, mm
        - Input
        - all
    *   - ice_rain
        - ice rain accumulated from cmc gentime, mm
        - Input
        - all
    *   - iced_graupel
        - iced graupel accumulated from cmc gentime, mm
        - Input
        - all
    *   - instant_precipitation
        - instant precipitation intensity, mm/h
        - Input
        - all
    *   - cmc_wind_u
        - wind U component at 10m, m/s
        - Input
        - all
    *   - cmc_wind_v
        - wind V component at 10m, m/s
        - Input
        - all
    *   - surface_pressure
        - surface pressure, Pa
        - Input
        - all
    *   - sea_level_pressure
        - sea level pressure, Pa
        - Input
        - all
    *   - geopotential_height_1000
        - geopotential height at 1000 hPa isobaric level, gpm (geopotential meter)
        - Input
        - all
    *   - cloudiness
        - cloudiness, % from 0 to 100
        - Input
        - all
    *   - precipitation_rate
        - avg precipitations rate between adjacent horizons, mm/h
        - Input
        - all
    *   - vorticity
        - absolute vorticity at height 1000 hPa, s-1
        - Input
        - all
    *   - cloud_mixing_ratio
        - Cloud mixing ratio at level 1000 hPa, kg/kg 0.0
        - Input
        - all
    *   - relative_humidity
        - relative humidity at 2m, %
        - Input
        - all
    *   - precipitable_water
        - total precipitable water, kg m-2
        - Input
        - all
    *   - vertical_velocity
        - vertical Velocity at 1000 hPa, Pa/s
        - Input
        - all
    *   - soil_temperature
        - soil temperature at 0.0-0.1 m, C
        - Input
        - all
    *   - gfs_soil_temperature_available
        - is there gfs soil temp data
        - Input
        - all
    *   - gfs_temperature
        - temperature at 2m, C
        - Input
        - all
    *   - gfs_temperature_grad
        - temperature difference adjacent horizons at 2m
        - Input
        - all
    *   - cloud_coverage_high
        - cloud coverage (between horizons, divisible by 6) at high level, %
        - Input
        - all
    *   - cloud_coverage_low
        - cloud coverage (between horizons, divisible by 6) at low level, %
        - Input
        - all
    *   - cloud_coverage_middle
        - cloud coverage (between horizons, divisible by 6) at middle level, %
        - Input
        - all
    *   - gfs_wind_u
        - 10 meter U wind component, m/s
        - Input
        - all
    *   - gfs_wind_v
        - 10 meter V wind component, m/s
        - Input
        - all
    *   - gfs_wind_speed
        - wind velocity, sqrt(gfs_u_wind2 + gfs_v_wind2), m/s
        - Input
        - all
    *   - wrf_temperature
        - temperature at 2m, K
        - Input
        - all
    *   - wrf_wind_u
        - wind U component, m/s
        - Input
        - all
    *   - wrf_wind_v
        - wind V component, m/s
        - Input
        - all
    *   - wrf_wind_v
        - wind V component, m/s
        - Input
        - all
    *   - rain_rate
        - avg rain rate between two horizons, mm/h
        - Input
        - all
    *   - snow_rate
        - avg snow rate between two horizons, mm/h
        - Input
        - all
    *   - hail_velocity
        - hail velocity on two horizons, mm/h
        - Input
        - all
    *   - wrf_temperature_grad
        - difference between temperatures at 2m on adjacent horizons, K
        - Input
        - all
    *   - rain_accumulated_grad
        - rain accumulated from cmc gentime difference between adjacent horizons, mm
        - Input
        - all
    *   - snow_accumulated_grad
        - snow accumulated from cmc gentime difference between adjacent horizons, mm
        - Input
        - all
    *   - ice_rain_grad
        - ice rain accumulated from cmc gentime difference between adjacent horizons, mm
        - Input
        - all
    *   - iced_graupel_grad
        - iced graupel accumulated from cmc gentime difference between adjacent horizons, mm
        - Input
        - all
    *   - cloud_coverage_grad
        - difference between low level cloud coverage on adjacent horizons, %
        - Input
        - all

License
-------

CC BY NC SA 4.0

References
----------

- Malinin, Andrey, et al. "Shifts: A dataset of real distributional shift across multiple large-scale tasks." arXiv preprint arXiv:2107.07455 (2021).
