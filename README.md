Before you start.

Consider using 
https://github.com/pyenv/pyenv#choosing-the-python-version
https://github.com/pyenv/pyenv-virtualenv

Libraries required python 3.8 (3.9 is not working for now, there is problem with numpy dependencies).

Remember to updated you .Xrc file like this 
.zprofile ->
```
export PYENV_ROOT="$HOME/.pyenv/shims"
export PATH="$PYENV_ROOT:$PATH"
export PIPENV_PYTHON="$PYENV_ROOT/python"
eval "$(pyenv init --path)"
```

.zsh ->
```
export PYENV_ROOT="$HOME/.pyenv/shims"
export PATH="$PYENV_ROOT:$PATH"
export PIPENV_PYTHON="$PYENV_ROOT/python"
eval "$(pyenv init -)"
```


```shell
pyenv install 3.8.12
pyenv global 3.8.12
pyenv virtualenv 3.8.12 testvenv
pyenv activate testvenv
brew install postgresql #when using OSX otherwise might not be needed
brew link postgresql
./install.sh
``` 



Code formatter can be found on: https://pypi.org/project/black/



Setting up local airflow cluster
https://dev.to/mucio/quickly-setup-airflow-for-development-with-breeze-d8h
https://github.com/apache/airflow/blob/main/BREEZE.rst
