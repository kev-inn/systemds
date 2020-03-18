# Build instructions

## Basic steps
The following steps have to be done for both cases
- Build SystemDS with maven first `mvn package -DskipTests`, with the working directory being `SYSTEMDS_ROOT` (Root directory of SystemDS)
- `cd` to this folder (basically `SYSTEMDS_ROOT/src/main/python`

### Building package
If we want to build the package for uploading to the repository via `python3 -m twine upload --repository-url [URL] dist/*` (will be automated in the future)
- Run `create_python_dist.py`
```bash
python3 create_python_dist.py
```
- now in the `./dist` directory there will exist the source distribution `systemds-VERSION.tar.gz` and the wheel distribution `systemds-VERSION-py3-none-any.whl`, with `VERSION` being the current version number
- Finished. We can now upload it with `python3 -m twine upload --repository-url [URL] dist/*`

### Building for development
If we want to build the package just locally for development, the following steps will suffice
- Run `pre_setup.py` (this will copy `lib` and `systemds-VERSION-SNAPSHOT.jar`)
```bash
python3 create_python_dist.py
```
- Finished. Test by running a test case of your choice:
```bash
python3 tests/test_matrix_binary_op.py
```
