# regional-transit-screening-platform
``Python`` &amp; ``SQL`` code to (re)create the data products featured on https://www.dvrpc.org/webmaps/RTSP/

## Prior Work

The code in this repository builds upon the methodology and earlier coding efforts found across a hanful of repositories:
- https://github.com/dvrpc/RTPS_GITHUB
- https://github.com/dvrpc/CityTranstiPlan_FY20
- https://github.com/addisonlarson/transit_reliability_measure


## Documentation
- [Methodology](documentation/methodology.md)
- [Set up your development environment](documentation/development_environment.md)
- [Execute the analysis](documentation/analysis_execution.md)


## Folder structure:

```bash
.
├── LICENSE
├── README.md
├── documentation
│   ├── analysis_execution.md
│   ├── data_profiles
│   │   └── ridership_septa_2020_07.html
│   ├── development_environment.md
│   ├── methodology.md
│   └── notebooks
│       ├── data_profiles.ipynb
│       ├── documentation.ipynb
│       └── profile.ipynb
├── env.yml
├── regional_transit_screening_platform
│   ├── __init__.py
│   ├── cli.py
│   ├── database.py
│   ├── step_00_helpers
│   │   ├── __init__.py
│   │   ├── interpolation.py
│   │   └── readme.md
│   ├── step_01_import_data
│   │   ├── __init__.py
│   │   ├── cmd.py
│   │   ├── main.py
│   │   └── readme.md
│   ├── step_02_average_speed
│   │   ├── __init__.py
│   │   ├── cmd.py
│   │   ├── main.py
│   │   └── readme.md
│   ├── step_03_on_time_performance
│   │   ├── __init__.py
│   │   └── readme.md
│   ├── step_04_travel_time_index
│   │   ├── __init__.py
│   │   └── readme.md
│   └── step_05_ridership
│       ├── __init__.py
│       ├── main.py
│       └── readme.md
└── setup.py

10 directories, 32 files
```