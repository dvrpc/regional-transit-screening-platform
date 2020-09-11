# step_02_average_speed

This module uses the average speed data source
to create a 0 - 100 scale that combines all overlapping
segments, normalized by the number of trips along each segment.

## QAQC

- Remove empty and zero values
    - Addison's work indicated that 63,895 features should remain after filtering.
    - I have 65,342 after filtering
    - I'm using ``WHERE avgspeed IS NOT NULL AND avgspeed > 0``
