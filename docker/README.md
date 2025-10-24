This docker file container 4 available stages to build:

- gdal-base: builds a gdal container based on the default base image, in this case an ironbank python3.10 rhel
- cert-base: a container that is utilized during the CI process to add certs allowing access to the internal NGA pypi mirror
- runner: the ultimate container that will be fielded to production
- unit-test: the stage to build and test the applications unit test suite.

Locally: find instructions in the root of the readme
In CI field: you'll see significant rebuild as you deal with laoding the custom certs, backlog item to streamline build times for the container.

Available stage chaining:
Advised local path: gdal-base -> runner -> unit-test
Path implemented in CI: [cert-base || gdal-base] -> runner -> unit-test
