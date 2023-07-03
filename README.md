This repository contains shared library modules which will be used for the
[translation imbalances](https://meta.wikimedia.org/wiki/Research:Content_Translation_language_imbalances)
research project.

Module structure
===
Each module is responsible for generating one or more data tables.  The library
is designed for fast access of static data, and not for continually-updated data.

Data memoization
===
Some getter accessors will memoize results as CSV files, using the
`data_store.cached` decorator.  The criteria to decide when a function should be
memoized are roughly,
* Is this output expensive to calculate, taking more than 1 second of network
or processing?
* Will an external tool be reading this output, which can more conveniently read
from a file rather than calling a python function directly?

Configuration
===
The default data source should be explicitly configured before using the
library.  A simple configuration can read from and write to a separate data
repository:

```python
import data_store
data_store.configure_global_store(
    data_store.DataStore(output_path="/home/USER/translation-stats-data")
)
```

Data structure
===
Data returned by public functions can be a list or dict.  Each row can be a
simple value or a dict, but not a tuple.  This way the data has clear semantics,
and changing the structure will result in no change or simple changes to
consuming code.  If the data will be memoized, it must be a list of dicts.

Example output structures:

```python
# Returns simple values
>>> get_valid_languages()
["aa", "af", ...]

# Returns structured data
>>> get_users_with_language_proficiencies()
[
	{"user": "Aardvark", "proficiencies": ["aa-1", "af-N"]}
]
```

Complex cell values such as lists should be serialized as JSON.

Coding conventions
===
Follow [PEP 8](https://peps.python.org/pep-0008/), especially prefixing any
private methods with an underscore so that they don't accidentally become part
of the public interface.  We'll include a linter to standardize formatting.

Include unit tests for any non-trivial functions.

Installing
===
For a runtime environment, install the package like

```
pip install -e .
```

In order to get the test and lint packages, install like

```
pip install -e .[lint,test]
```

Running tests
===
```
pytest
```

Running the code formatter
===
```
black .
```
