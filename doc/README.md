This is a Sphinx project. HTML can be built using:

```
make clean html
```

The html files will appear under `build/html/`

To run make, ensure that spinx is installed:

```
pip install -U sphinx
```

Convenient for editing is a "live view", which automatically rebuilds
the docs and refreshes the browser page after edits are saved.

```
pip install -U sphinx-autobuild

make clean livehtml
```

Then open a browser at http://localhost:8000/

