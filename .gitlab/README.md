# GitLab Pipeline README

This folder contains all of the CI/CD pipeline files that are used by GitLab to execute jobs across multiple environments.

## Folder Structure

The folder structure is organized as follows:

`.gitlab/<environment>`

The first nested folder represented the GitLab CI environment. For example, `.gitlab/aws` would contain all of the files that would execute in the GitLab pipeline for AWS.

There is one exception to this - there is a **common** folder that exists at the same level. This folder represents jobs that could be executed across multiple GitLab environments.

`.gitlab/<environment>/<scenario>`

The next nested folder represents the scenario that a GitLab job would be executed for. For example, we may have jobs that would happen only on **merge_request** or only against a specific branch like **main**. This directory structure helps to quickly find a specific job depending on where it is supposed to be running. In addition, the main logic to control this (a GitLab rule) can be used in one place instead of on every underlying job.

## Entrypoints

At every level of the GitLab folder structure there will be an entrypoint file called `.gitlab-ci.yml`. This file represents the starting point for either a specific environment or a specific scenario. You can refer back to these files to determine what exactly is running at each level.

## Hidden Files

At certain places in the folder structure you might see a `.hidden.gitlab-ci.yml` file. This file will contain jobs that are *hidden* and are not executed automatically. Jobs in GitLab are hidden by prefixing them with a `.`. For example, a hidden job might look like:

```yaml
.build:
  before_script:
    - echo foo
```

These jobs can be extended upon, resulting in a reusable job that can be used across the pipeline. For example, to extend the above job might look something like:

```yaml
build_foo_bar:
  extends: .build
  script:
    - echo bar
```

This would result in a job that run both scripts:

```
foo
bar
```

## Override Files

Similar to hidden files, you might see a `.overrides.gitlab-ci.yml` in the folder structure. These jobs will use the same name as a job that is already defined, but override specific properties to change their behavior. Given the above two jobs referenced in the previous section were defined, see below example:

```yaml
build_foo_bar:
  script:
    - echo baz
```

This would result in the `script` section being replaced and would result in the following output:

```
foo
baz
```
