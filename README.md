This repository contains shared modules which will be used for the [translation imbalances](https://meta.wikimedia.org/wiki/Research:Content_Translation_language_imbalances)
research project.

Module structure
===
Each data source is managed by a dedicated python module.  For example, the
[content translation stats](https://en.wikipedia.org/w/api.php?action=help&modules=query%2Bcontenttranslations)
API corresponds to the `content_translation_stats.py` module.

Functions which take more than a few seconds to execute should memoize their
results to the filesystem using the `cached` decorator.

Modules may provide a single, top-level accessor function for retrieving the
information, or multiple functions for retrieving various views of the data.

Data structure
===
Data is persisted as CSV or newline-delimited JSON, via the `cached` module.

Data returned by public functions can be a list or dict.  Each row can be a
simple value or a dict, but not a tuple.  This way the data has clear semantics,
and changing the structure will result in no change or simple changes to
consuming code.  For example:

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

Coding conventions
===
Follow [PEP 8](https://peps.python.org/pep-0008/), especially prefixing any
private methods with an underscore so that they don't become part of the public
interface.  We'll include a linter to standardize formatting.

Include unit tests for any non-trivial functions.

Installing dependencies
===
```
pip install -r requirements.txt
```

Running tests
===
```
pytest
```
