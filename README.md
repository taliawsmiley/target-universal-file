# target-universal-file

`target-universal-file` is a Singer target for UniversalFile.

Build with the [Meltano Target SDK](https://sdk.meltano.com).

## Installation

<!-- Install from PyPi:

```bash
pipx install target-universal-file
``` -->

Install from GitHub:

```bash
pipx install git+https://github.com/sebastianswms/target-universal-file.git@main
```

## Configuration

### Accepted Config Options


| Setting | Required | Default | Description |
|:--------|:--------:|:-------:|:------------|
| filesystem | True     | None    |             |
| filesystem.protocol | True     | None    |             |
| filesystem.path | True     | None    |             |
| format | True     | None    |             |
| format.type | True     | None    |             |
| add_record_metadata | False    | None    | Add metadata to records. |
| load_method | False    | append-only | The method to use when loading data into the destination. `append-only` will always write all input records whether that records already exists or not. `upsert` will update existing records and insert new records. `overwrite` will delete all existing records and insert all input records. |
| batch_size_rows | False    | None    | Maximum number of rows in each batch. |
| validate_records | False    |       1 | Whether to validate the schema of the incoming streams. |
| stream_maps | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config | False    | None    | User-defined config values to be used within map expressions. |
| faker_config | False    | None    | Config for the [`Faker`](https://faker.readthedocs.io/en/master/) instance variable `fake` used within map expressions. Only applicable if the plugin specifies `faker` as an addtional dependency (through the `singer-sdk` `faker` extra or directly). |
| faker_config.seed | False    | None    | Value to seed the Faker generator for deterministic output: https://faker.readthedocs.io/en/master/#seeding-the-generator |
| faker_config.locale | False    | None    | One or more LCID locale strings to produce localized output for: https://faker.readthedocs.io/en/master/#localization |
| flattening_enabled | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth | False    | None    | The max depth to flatten schemas. |


A full list of supported settings and capabilities for this
target is available by running:

```bash
target-universal-file --about
```

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Authentication and Authorization

Varies by file system.

## Usage

You can easily run `target-universal-file` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-universal-file --version
target-universal-file --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-universal-file --config /path/to/target-universal-file-config.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-universal-file` CLI interface directly using `poetry run`:

```bash
poetry run target-universal-file --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._


Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd target-universal-file
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke target-universal-file --version
# OR run a test `elt` pipeline with the Carbon Intensity sample tap:
meltano run tap-carbon-intensity target-universal-file
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the Meltano Singer SDK to
develop your own Singer taps and targets.
