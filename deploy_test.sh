cd hdijupyterutils
python setup.py sdist upload -r pypitest
cd..
cd autovizwidget
python setup.py sdist upload -r pypitest
cd ..
cd sparkmagic
python setup.py sdist upload -r pypitest
cd ..