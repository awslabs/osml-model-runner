#!/bin/sh

FILES=$(git diff --name-only --cached -- '***.py')
if test -z "$FILES"
then
  echo "No files to format"
else

  echo "Running flake8"
  echo "$FILES" | xargs flake8

  echo "Running isort"
  echo "$FILES" | xargs isort

  echo "Running black"
  echo "$FILES" | xargs black

  echo "Adding files back to commit"
  echo "$FILES" | xargs git add

fi