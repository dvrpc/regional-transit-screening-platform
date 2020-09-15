## Development Environment Setup

## Install ``conda``

Download and install the latest version of ``miniconda`` that matches your computer.

## Install this package & dependencies

Use ``conda`` to create an environment named ``RTSP`` for this code, and install all dependencies, as defined in the ``env.yml`` file.
Your terminal's working directory should be the root of this repository.

```bash
(base) $ conda env create -f env.yml
```

## Activate the environment before executing code

```bash
(base) $ conda activate RTSP
(RTSP) $ python my_script.py
```
